/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <algorithm>
#include "common/time/WallClock.h"
#include "codec/RowWriterV2.h"
#include "utils/IndexKeyUtils.h"
#include "utils/NebulaKeyUtils.h"
#include "utils/OperationKeyUtils.h"
#include "storage/StorageFlags.h"
#include "storage/mutate/AddVerticesProcessor.h"

DECLARE_bool(enable_vertex_cache);

namespace nebula {
namespace storage {

void AddVerticesProcessor::process(const cpp2::AddVerticesRequest& req) {
    auto version = FLAGS_enable_multi_versions ?
        std::numeric_limits<int64_t>::max() - time::WallClock::fastNowInMicroSec() : 0L;
    // Switch version to big-endian, make sure the key is in ordered.
    version = folly::Endian::big(version);

    spaceId_ = req.get_space_id();
    const auto& partVertices = req.get_parts();
    const auto& propNamesMap = req.get_prop_names();

    CHECK_NOTNULL(env_->schemaMan_);
    auto ret = env_->schemaMan_->getSpaceVidLen(spaceId_);
    if (!ret.ok()) {
        LOG(ERROR) << ret.status();
        for (auto& part : partVertices) {
            pushResultCode(cpp2::ErrorCode::E_INVALID_SPACEVIDLEN, part.first);
        }
        onFinished();
        return;
    }
    spaceVidLen_ = ret.value();
    callingNum_ = partVertices.size();

    CHECK_NOTNULL(env_->indexMan_);
    handleTagIndexes(spaceId_);

    CHECK_NOTNULL(env_->kvstore_);
    for (auto& part : partVertices) {
        auto partId = part.first;
        const auto& vertices = part.second;

        std::vector<kvstore::KV> data;
        data.reserve(32);
        for (auto& vertex : vertices) {
            auto vid = vertex.get_id();
            const auto& newTags = vertex.get_tags();

            if (!NebulaKeyUtils::isValidVidLen(spaceVidLen_, vid.getStr())) {
                LOG(ERROR) << "Space " << spaceId_ << ", vertex length invalid, "
                           << " space vid len: " << spaceVidLen_ << ",  vid is " << vid;
                pushResultCode(cpp2::ErrorCode::E_INVALID_VID, partId);
                onFinished();
                return;
            }

            for (auto& newTag : newTags) {
                auto tagId = newTag.get_tag_id();
                VLOG(3) << "PartitionID: " << partId << ", VertexID: " << vid
                        << ", TagID: " << tagId << ", TagVersion: " << version;

                auto key = NebulaKeyUtils::vertexKey(spaceVidLen_, partId, vid.getStr(),
                                                     tagId, version);
                auto schema = env_->schemaMan_->getTagSchema(spaceId_, tagId);
                if (!schema) {
                    LOG(ERROR) << "Space " << spaceId_ << ", Tag " << tagId << " invalid";
                    pushResultCode(cpp2::ErrorCode::E_TAG_NOT_FOUND, partId);
                    onFinished();
                    return;
                }

                auto props = newTag.get_props();
                auto iter = propNamesMap.find(tagId);
                std::vector<std::string> propNames;
                if (iter != propNamesMap.end()) {
                    propNames = iter->second;
                }

                WriteResult wRet;
                auto retEnc = encodeRowVal(schema.get(), propNames, props, wRet);
                if (!retEnc.ok()) {
                    LOG(ERROR) << retEnc.status();
                    pushResultCode(writeResultTo(wRet, false), partId);
                    onFinished();
                    return;
                }
                data.emplace_back(std::move(key), std::move(retEnc.value()));

                if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
                    vertexCache_->evict(std::make_pair(vid.getStr(), tagId), partId);
                    VLOG(3) << "Evict cache for vId " << vid
                            << ", tagId " << tagId;
                }
            }
        }
        if (indexes_.empty() && vertexIndexes_.empty() && allVertexStatIndex_ == nullptr) {
            doPut(spaceId_, partId, std::move(data));
        } else {
            auto atomic = [partId, vertices = std::move(data), this]()
                          -> folly::Optional<std::string> {
                return addVertices(partId, vertices);
            };

            auto callback = [partId, this](kvstore::ResultCode code) {
                handleAsync(spaceId_, partId, code);
            };
            env_->kvstore_->asyncAtomicOp(spaceId_, partId, atomic, callback);
        }
    }
}

folly::Optional<std::string>
AddVerticesProcessor::addVertices(PartitionID partId,
                                  const std::vector<kvstore::KV>& vertices) {
    env_->onFlyingRequest_.fetch_add(1);
    std::unique_ptr<kvstore::BatchHolder> batchHolder = std::make_unique<kvstore::BatchHolder>();

    // Statistics of the vertexId added for each part
    std::unordered_set<VertexID> vertexIds;
    /*
     * Define the map newVertices to avoid inserting duplicate vertex.
     * This map means :
     * map<vertex_unique_key, prop_value> ,
     * -- vertex_unique_key is only used as the unique key , for example:
     * insert below vertices in the same request:
     *     kv(part1_vid1_tag1 , v1)
     *     kv(part1_vid1_tag1 , v2)
     *     kv(part1_vid1_tag1 , v3)
     *     kv(part1_vid1_tag1 , v4)
     *
     * Ultimately, kv(part1_vid1_tag1 , v4) . It's just what I need.
     */
    std::unordered_map<std::string, std::string> newVertices;
    std::for_each(vertices.begin(), vertices.end(),
                 [&newVertices](const std::map<std::string, std::string>::value_type& v)
                 { newVertices[v.first] = v.second; });

    for (auto& v : newVertices) {
        std::string val;
        RowReaderWrapper oReader;
        RowReaderWrapper nReader;
        auto tagId = NebulaKeyUtils::getTagId(spaceVidLen_, v.first);
        auto vId = NebulaKeyUtils::getVertexId(spaceVidLen_, v.first).str();

        // Normal index
        for (auto& index : indexes_) {
            if (tagId == index->get_schema_id().get_tag_id()) {
                auto indexId = index->get_index_id();
                /*
                 * step 1 , Delete old version normal index if exists.
                 */
                if (val.empty()) {
                    auto oldVal = findOldValue(partId, vId, tagId);
                    if (oldVal == folly::none) {
                        return folly::none;
                    }

                    val = std::move(oldVal).value();
                    if (!val.empty()) {
                        oReader = RowReaderWrapper::getTagPropReader(env_->schemaMan_,
                                                                     spaceId_,
                                                                     tagId,
                                                                     val);
                        if (oReader == nullptr) {
                            LOG(ERROR) << "Bad format row";
                            return folly::none;
                        }
                    }
                }

                if (!val.empty()) {
                    auto oi = normalIndexKey(partId, vId, oReader.get(), index);
                    if (!oi.empty()) {
                        // Check the index is building for the specified partition or not.
                        if (env_->checkRebuilding(spaceId_, partId, indexId)) {
                            auto deleteOpKey = OperationKeyUtils::deleteOperationKey(partId);
                            batchHolder->put(std::move(deleteOpKey), std::move(oi));
                        } else if (env_->checkIndexLocked(spaceId_, partId, indexId)) {
                            LOG(ERROR) << "The index has been locked: " << index->get_index_name();
                            return folly::none;
                        } else {
                            batchHolder->remove(std::move(oi));
                        }
                    }
                }

                /*
                 * step 2 , Insert new vertex normal index
                 */
                if (nReader == nullptr) {
                    nReader = RowReaderWrapper::getTagPropReader(env_->schemaMan_,
                                                                 spaceId_,
                                                                 tagId,
                                                                 v.second);
                    if (nReader == nullptr) {
                        LOG(ERROR) << "Bad format row";
                        return folly::none;
                    }
                }
                auto ni = normalIndexKey(partId, vId, nReader.get(), index);
                if (!ni.empty()) {
                    auto retRebuild = rebuildingModifyOp(spaceId_,
                                                         partId,
                                                         indexId,
                                                         ni,
                                                         batchHolder.get());
                    if (retRebuild == folly::none) {
                        return folly::none;
                    }
                }
            }
        }

        // step 3, insert vertex index data.
        // To simplify, directly overwrite
        // TODO(pandasheep) handle ttl in index
        auto vIndexIt = tagIdToIndexId_.find(tagId);
        if (vIndexIt != tagIdToIndexId_.end()) {
            auto indexId = vIndexIt->second;
            auto vertexIndexKey = StatisticsIndexKeyUtils::vertexIndexKey(spaceVidLen_,
                                                                          partId,
                                                                          indexId,
                                                                          vId);
            if (!vertexIndexKey.empty()) {
                auto retRebuild = rebuildingModifyOp(spaceId_,
                                                     partId,
                                                     indexId,
                                                     vertexIndexKey,
                                                     batchHolder.get());
                if (retRebuild == folly::none) {
                    return folly::none;
                }
            }
        }

        /*
         * step 4, for statistics all vertex count
         */
        if (allVertexStatIndex_ != nullptr) {
            vertexIds.emplace(vId);
        }

        /*
         * step 5, Insert new vertex data
         */
        auto key = v.first;
        auto prop = v.second;
        batchHolder->put(std::move(key), std::move(prop));
    }

    /*
     * step 6, upsert statistic all vertex index data
     */
    if (allVertexStatIndex_ != nullptr) {
        auto vertexCountIndexId = allVertexStatIndex_->get_index_id();
        int64_t countVal = 0;
        for (auto vid : vertexIds) {
            auto prefix = NebulaKeyUtils::vertexPrefix(spaceVidLen_, partId, vid);
            std::unique_ptr<kvstore::KVIterator> iter;
            auto ret = env_->kvstore_->prefix(spaceId_, partId, prefix, &iter);
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                LOG(ERROR) << "Error! ret = " << static_cast<int32_t>(ret)
                           << ", spaceId " << spaceId_;
                return folly::none;
            }
            bool isExist = false;
            while (iter && iter->valid()) {
                auto key = iter->key();
                if (!NebulaKeyUtils::isVertex(spaceVidLen_, key)) {
                    iter->next();
                    continue;
                }
                isExist = true;
                break;
            }
            if (!isExist) {
                countVal++;
            }
        }

        // New vertex added
        if (countVal != 0) {
            std::string val;
            auto vCountIndexKey = StatisticsIndexKeyUtils::countIndexKey(partId,
                                                                         vertexCountIndexId);
            auto ret = env_->kvstore_->get(spaceId_, partId, vCountIndexKey, &val);
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                if (ret != kvstore::ResultCode::ERR_KEY_NOT_FOUND) {
                    LOG(ERROR) << "Get all vertex count index error";
                    return folly::none;
                }
                // key does not exist
            } else {
                countVal += *reinterpret_cast<const int64_t*>(val.c_str());
            }

            if (!vCountIndexKey.empty()) {
                auto newCount = std::string(reinterpret_cast<const char*>(&countVal),
                                            sizeof(int64_t));
                auto retRebuild = rebuildingModifyOp(spaceId_,
                                                     partId,
                                                     vertexCountIndexId,
                                                     vCountIndexKey,
                                                     batchHolder.get(),
                                                     newCount);
                if (retRebuild == folly::none) {
                    return folly::none;
                }
            }
        }
    }

    return encodeBatchValue(batchHolder->getBatch());
}

folly::Optional<std::string>
AddVerticesProcessor::findOldValue(PartitionID partId, VertexID vId, TagID tagId) {
    auto prefix = NebulaKeyUtils::vertexPrefix(spaceVidLen_, partId, vId, tagId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env_->kvstore_->prefix(spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Error! ret = " << static_cast<int32_t>(ret)
                   << ", spaceId " << spaceId_;
        return folly::none;
    }
    if (iter && iter->valid()) {
        return iter->val().str();
    }
    return std::string();
}

std::string
AddVerticesProcessor::normalIndexKey(PartitionID partId,
                                     VertexID vId,
                                     RowReader* reader,
                                     std::shared_ptr<nebula::meta::cpp2::IndexItem> index) {
    std::vector<Value::Type> colsType;
    auto values = IndexKeyUtils::collectIndexValues(reader, index->get_fields(), colsType);
    if (!values.ok()) {
        return "";
    }

    return IndexKeyUtils::vertexIndexKey(spaceVidLen_, partId,
                                         index->get_index_id(),
                                         vId, values.value(),
                                         colsType);
}

}  // namespace storage
}  // namespace nebula
