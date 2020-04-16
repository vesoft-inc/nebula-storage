/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/mutate/AddVerticesProcessor.h"
#include "common/NebulaKeyUtils.h"
#include <algorithm>
#include <limits>
#include "time/WallClock.h"
#include "codec/RowWriterV2.h"
#include "storage/StorageFlags.h"

DECLARE_bool(enable_vertex_cache);

namespace nebula {
namespace storage {

void AddVerticesProcessor::process(const cpp2::AddVerticesRequest& req) {
    auto version =
        std::numeric_limits<int64_t>::max() - time::WallClock::fastNowInMicroSec();
    // Switch version to big-endian, make sure the key is in ordered.
    version = folly::Endian::big(version);

    spaceId_ = req.get_space_id();
    const auto& partVertices = req.get_parts();
    const auto& propNamesMap = req.get_prop_names();

    CHECK_NOTNULL(env_->schemaMan_);
    auto ret = env_->schemaMan_->getSpaceVidLen(spaceId_);
    if (!ret.ok()) {
        LOG(ERROR) << "Space " << spaceId_ << " VertexId length invalid."
                   << ret.status().toString();
        cpp2::PartitionResult thriftRet;
        thriftRet.set_code(cpp2::ErrorCode::E_INVALID_SPACEVIDLEN);
        codes_.emplace_back(std::move(thriftRet));
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
    if (indexes_.empty()) {
        for (auto& part : partVertices) {
            auto partId = part.first;
            const auto& newVertices = part.second;

            std::vector<kvstore::KV> data;
            data.reserve(32);
            for (auto& newVertex : newVertices) {
                auto vid = newVertex.get_id();
                const auto& newTags = newVertex.get_tags();
                for (auto& newTag : newTags) {
                    auto tagId = newTag.get_tag_id();
                    VLOG(3) << "PartitionID: " << partId << ", VertexID: " << vid
                            << ", TagID: " << tagId << ", TagVersion: " << version;

                    auto key = NebulaKeyUtils::vertexKey(spaceVidLen_, partId, vid,
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
                    RowWriterV2 rowWrite(schema.get());
                    if (iter != propNamesMap.end()) {
                        auto colNames = iter->second();
                        for (size_t i = 0; i < colNames.size(); i++) {
                            auto wRet = rowWrite.setValue(colNames[i], props[i]);
                            if (wRet != WriteResult::SUCCEEDED) {
                                LOG(ERROR) << "Add vertex faild";
                                pushResultCode(cpp2::ErrorCode::E_DATA_TYPE_MISMATCH, partId);
                                onFinished();
                                return;
                            }
                        }
                    } else {
                        for (size_t i = 0; i < props.size(); i++) {
                            auto wRet = rowWrite.setValue(i, props[i]);
                            if (wRet != WriteResult::SUCCEEDED) {
                                LOG(ERROR) << "Add vertex faild";
                                pushResultCode(cpp2::ErrorCode::E_DATA_TYPE_MISMATCH, partId);
                                onFinished();
                                return;
                            }
                        }
                    }
                    auto wRet = rowWrite.finish();
                    if (wRet != WriteResult::SUCCEEDED) {
                        LOG(ERROR) << "Add vertex faild";
                        pushResultCode(cpp2::ErrorCode::E_DATA_TYPE_MISMATCH, partId);
                        onFinished();
                        return;
                    }

                    std::string encode = std::move(rowWrite).moveEncodedStr();
                    data.emplace_back(std::move(key), std::move(encode));

                    if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
                        vertexCache_->evict(std::make_pair(vid, tagId), partId);
                        VLOG(3) << "Evict cache for vId " << vid
                                << ", tagId " << tagId;
                    }
                }
            }
            doPut(spaceId_, partId, std::move(data));
        }
    } else {
        for (auto& part : partVertices) {
            auto partId = part.first;
            auto atomic = [version, partId, vertices = std::move(part.second), this]()
                          -> folly::Optional<std::string> {
                return this->addVertices(version, partId, vertices);
            };
            auto callback = [partId, this](kvstore::ResultCode code) {
                this->handleAsync(this->spaceId_, partId, code);
            };
            env_->kvstore_->asyncAtomicOp(spaceId_, partId, atomic, callback);
        }
    }
}

std::string AddVerticesProcessor::addVertices(int64_t version, PartitionID partId,
                                              const std::vector<cpp2::NewVertex>& vertices) {
    UNUSED(version);
    UNUSED(partId);
    UNUSED(vertices);
#if 0
    std::unique_ptr<kvstore::BatchHolder> batchHolder = std::make_unique<kvstore::BatchHolder>();
    /*
     * Define the map newIndexes to avoid inserting duplicate vertex.
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
    std::map<std::string, std::string> newVertices;
    std::for_each(vertices.begin(), vertices.end(), [&](auto& v) {
        auto vId = v.get_id();
        const auto& tags = v.get_tags();
        std::for_each(tags.begin(), tags.end(), [&](auto& tag) {
            auto tagId = tag.get_tag_id();
            auto prop = tag.get_props();
            VLOG(3) << "PartitionID: " << partId << ", VertexID: " << vId
                    << ", TagID: " << tagId << ", TagVersion: " << version;
            auto key = NebulaKeyUtils::vertexKey(partId, vId, tagId, version);
            newVertices[key] = std::move(prop);
            if (FLAGS_enable_vertex_cache && this->vertexCache_ != nullptr) {
                this->vertexCache_->evict(std::make_pair(vId, tagId), partId);
                VLOG(3) << "Evict cache for vId " << vId << ", tagId " << tagId;
            }
        });
    });

    for (auto& v : newVertices) {
        std::string val;
        std::unique_ptr<RowReader> nReader;
        auto tagId = NebulaKeyUtils::getTagId(v.first);
        auto vId = NebulaKeyUtils::getVertexId(v.first);
        for (auto& index : indexes_) {
            if (tagId == index->get_schema_id().get_tag_id()) {
                /*
                 * step 1 , Delete old version index if exists.
                 */
                if (val.empty()) {
                    val = findObsoleteIndex(partId, vId, tagId);
                }
                if (!val.empty()) {
                    auto reader = RowReader::getTagPropReader(this->schemaMan_,
                                                              val,
                                                              spaceId_,
                                                              tagId);
                    auto oi = indexKey(partId, vId, reader.get(), index);
                    if (!oi.empty()) {
                        batchHolder->remove(std::move(oi));
                    }
                }
                /*
                 * step 2 , Insert new vertex index
                 */
                if (nReader == nullptr) {
                    nReader = RowReader::getTagPropReader(this->schemaMan_,
                                                          v.second,
                                                          spaceId_,
                                                          tagId);
                }
                auto ni = indexKey(partId, vId, nReader.get(), index);
                batchHolder->put(std::move(ni), "");
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
#endif
    return std::string("");
}
/*
std::string AddVerticesProcessor::findObsoleteIndex(PartitionID partId,
                                                    VertexID vId,
                                                    TagID tagId) {
    auto prefix = NebulaKeyUtils::vertexPrefix(partId, vId, tagId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(this->spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Error! ret = " << static_cast<int32_t>(ret)
                   << ", spaceId " << this->spaceId_;
        return "";
    }
    if (iter && iter->valid()) {
        return iter->val().str();
    }
    return "";
}

std::string AddVerticesProcessor::indexKey(PartitionID partId,
                                           VertexID vId,
                                           RowReader* reader,
                                           std::shared_ptr<nebula::cpp2::IndexItem> index) {
    auto values = collectIndexValues(reader, index->get_fields());
    return NebulaKeyUtils::vertexIndexKey(partId,
                                          index->get_index_id(),
                                          vId, values);
}
*/

}  // namespace storage
}  // namespace nebula
