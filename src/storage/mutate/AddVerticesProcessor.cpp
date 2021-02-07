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

ProcessorCounters kAddVerticesCounters;


void AddVerticesProcessor::process(const cpp2::AddVerticesRequest& req) {
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
    auto iRet = env_->indexMan_->getTagIndexes(spaceId_);
    if (iRet.ok()) {
        indexes_ = std::move(iRet).value();
    }

    CHECK_NOTNULL(env_->kvstore_);
    for (auto& part : partVertices) {
        auto partId = part.first;
        const auto& vertices = part.second;

        std::vector<kvstore::KV> data;
        data.reserve(32);
        for (auto& vertex : vertices) {
            auto vid = vertex.get_id().getStr();
            const auto& newTags = vertex.get_tags();

            if (!NebulaKeyUtils::isValidVidLen(spaceVidLen_, vid)) {
                LOG(ERROR) << "Space " << spaceId_ << ", vertex length invalid, "
                           << " space vid len: " << spaceVidLen_ << ",  vid is " << vid;
                pushResultCode(cpp2::ErrorCode::E_INVALID_VID, partId);
                onFinished();
                return;
            }

            for (auto& newTag : newTags) {
                auto tagId = newTag.get_tag_id();
                VLOG(3) << "PartitionID: " << partId << ", VertexID: " << vid
                        << ", TagID: " << tagId;

                auto schema = env_->schemaMan_->getTagSchema(spaceId_, tagId);
                if (!schema) {
                    LOG(ERROR) << "Space " << spaceId_ << ", Tag " << tagId << " invalid";
                    pushResultCode(cpp2::ErrorCode::E_TAG_NOT_FOUND, partId);
                    onFinished();
                    return;
                }

                auto key = NebulaKeyUtils::vertexKey(spaceVidLen_, partId, vid, tagId);
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
                    vertexCache_->evict(std::make_pair(vid, tagId));
                    VLOG(3) << "Evict cache for vId " << vid
                            << ", tagId " << tagId;
                }
            }
        }
        if (indexes_.empty()) {
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
    IndexCountWrapper wrapper(env_);
    std::unique_ptr<kvstore::BatchHolder> batchHolder = std::make_unique<kvstore::BatchHolder>();
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
                  [&newVertices](const auto& v) {
                      newVertices[v.first] = v.second;
                  });

    for (auto& v : newVertices) {
        std::string val;
        RowReaderWrapper oReader;
        RowReaderWrapper nReader;
        auto tagId = NebulaKeyUtils::getTagId(spaceVidLen_, v.first);
        auto vId = NebulaKeyUtils::getVertexId(spaceVidLen_, v.first).str();

        for (auto& index : indexes_) {
            if (tagId == index->get_schema_id().get_tag_id()) {
                /*
                 * step 1 , Delete old version index if exists.
                 */
                if (val.empty()) {
                    auto obsIdx = findOldValue(partId, vId, tagId);
                    if (obsIdx == folly::none) {
                        return folly::none;
                    }
                    val = std::move(obsIdx).value();

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
                    auto oi = indexKey(partId, vId, oReader.get(), index);
                    if (!oi.empty()) {
                        // Check the index is building for the specified partition or not.
                        auto indexState = env_->getIndexState(spaceId_, partId);
                        if (env_->checkRebuilding(indexState)) {
                            auto deleteOpKey = OperationKeyUtils::deleteOperationKey(partId);
                            batchHolder->put(std::move(deleteOpKey), std::move(oi));
                        } else if (env_->checkIndexLocked(indexState)) {
                            LOG(ERROR) << "The index has been locked: " << index->get_index_name();
                            return folly::none;
                        } else {
                            batchHolder->remove(std::move(oi));
                        }
                    }
                }

                /*
                 * step 2 , Insert new vertex index
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
                auto ni = indexKey(partId, vId, nReader.get(), index);
                if (!ni.empty()) {
                    // Check the index is building for the specified partition or not.
                    auto indexState = env_->getIndexState(spaceId_, partId);
                    if (env_->checkRebuilding(indexState)) {
                        auto modifyOpKey = OperationKeyUtils::modifyOperationKey(partId, ni);
                        batchHolder->put(std::move(modifyOpKey), "");
                    } else if (env_->checkIndexLocked(indexState)) {
                        LOG(ERROR) << "The index has been locked: " << index->get_index_name();
                        return folly::none;
                    } else {
                        batchHolder->put(std::move(ni), "");
                    }
                }
            }
        }
        /*
         * step 3 , Insert new vertex data
         */
        auto key = v.first;
        auto prop = v.second;
        batchHolder->put(std::move(key), std::move(prop));
    }

    return encodeBatchValue(batchHolder->getBatch());
}

folly::Optional<std::string>
AddVerticesProcessor::findOldValue(PartitionID partId, const VertexID& vId, TagID tagId) {
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

std::string AddVerticesProcessor::indexKey(PartitionID partId,
                                           const VertexID& vId,
                                           RowReader* reader,
                                           std::shared_ptr<nebula::meta::cpp2::IndexItem> index) {
    auto values = IndexKeyUtils::collectIndexValues(reader, index->get_fields());
    if (!values.ok()) {
        return "";
    }

    return IndexKeyUtils::vertexIndexKey(spaceVidLen_, partId,
                                         index->get_index_id(),
                                         vId, std::move(values).value());
}

}  // namespace storage
}  // namespace nebula
