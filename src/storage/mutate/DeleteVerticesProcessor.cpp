/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/StorageFlags.h"
#include "storage/mutate/DeleteVerticesProcessor.h"
#include "utils/IndexKeyUtils.h"
#include "utils/NebulaKeyUtils.h"
#include "utils/OperationKeyUtils.h"

namespace nebula {
namespace storage {

void DeleteVerticesProcessor::process(const cpp2::DeleteVerticesRequest& req) {
    spaceId_ = req.get_space_id();
    const auto& partVertices = req.get_parts();

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
    if (indexes_.empty() && vertexIndexes_.empty() && allVertexStatIndex_ == nullptr) {
        // Operate every part, the graph layer guarantees the unique of the vid
        std::vector<std::string> keys;
        keys.reserve(32);
        for (auto& part : partVertices) {
            auto partId = part.first;
            const auto& vertexIds = part.second;
            keys.clear();

            for (auto& vid : vertexIds) {
                if (!NebulaKeyUtils::isValidVidLen(spaceVidLen_, vid.getStr())) {
                    LOG(ERROR) << "Space " << spaceId_ << ", vertex length invalid, "
                               << " space vid len: " << spaceVidLen_ << ",  vid is " << vid;
                    pushResultCode(cpp2::ErrorCode::E_INVALID_VID, partId);
                    onFinished();
                    return;
                }

                auto prefix = NebulaKeyUtils::vertexPrefix(spaceVidLen_, partId, vid.getStr());
                std::unique_ptr<kvstore::KVIterator> iter;
                auto retRes = env_->kvstore_->prefix(spaceId_, partId, prefix, &iter);
                if (retRes != kvstore::ResultCode::SUCCEEDED) {
                    VLOG(3) << "Error! ret = " << static_cast<int32_t>(retRes)
                            << ", spaceID " << spaceId_;
                    handleErrorCode(retRes, spaceId_, partId);
                    onFinished();
                    return;
                }
                while (iter && iter->valid()) {
                    auto key = iter->key();
                    if (NebulaKeyUtils::isVertex(spaceVidLen_, key)) {
                        auto tagId = NebulaKeyUtils::getTagId(spaceVidLen_, key);
                        // Evict vertices from cache
                        if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
                            VLOG(3) << "Evict vertex cache for VID " << vid
                                    << ", TagID " << tagId;
                            vertexCache_->evict(std::make_pair(vid.getStr(), tagId));
                        }
                        keys.emplace_back(key.str());
                    }
                    iter->next();
                }
            }
            doRemove(spaceId_, partId, keys);
        }
    } else {
        std::for_each(partVertices.begin(), partVertices.end(), [this](auto &pv) {
            auto partId = pv.first;
            auto atomic = [partId, v = std::move(pv.second),
                           this]() -> folly::Optional<std::string> {
                return deleteVertices(partId, v);
            };

            auto callback = [partId, this](kvstore::ResultCode code) {
                VLOG(3) << "partId:" << partId << ", code:" << static_cast<int32_t>(code);
                handleAsync(spaceId_, partId, code);
            };
            env_->kvstore_->asyncAtomicOp(spaceId_, partId, atomic, callback);
        });
    }
}

folly::Optional<std::string>
DeleteVerticesProcessor::deleteVertices(PartitionID partId,
                                        const std::vector<Value>& vertices) {
    env_->onFlyingRequest_.fetch_add(1);
    std::unique_ptr<kvstore::BatchHolder> batchHolder = std::make_unique<kvstore::BatchHolder>();
    // Statistics of the vertexId deleted for each part
    std::unordered_set<VertexID>    vertexIds;

    for (auto& vertex : vertices) {
        auto prefix = NebulaKeyUtils::vertexPrefix(spaceVidLen_, partId, vertex.getStr());
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = env_->kvstore_->prefix(spaceId_, partId, prefix, &iter);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            VLOG(3) << "Error! ret = " << static_cast<int32_t>(ret)
                    << ", spaceId " << spaceId_;
            return folly::none;
        }

        TagID latestTagId = -1;
        while (iter && iter->valid()) {
            auto key = iter->key();
            if (!NebulaKeyUtils::isVertex(spaceVidLen_, key)) {
                iter->next();
                continue;
            }

            auto tagId = NebulaKeyUtils::getTagId(spaceVidLen_, key);
            if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
                VLOG(3) << "Evict vertex cache for vertex ID " << vertex << ", tagId " << tagId;
                vertexCache_->evict(std::make_pair(vertex.getStr(), tagId), partId);
            }

            /**
             * example ,the prefix result as below :
             *     V1_tag1_version3
             *     V1_tag1_version2
             *     V1_tag1_version1
             *     V1_tag2_version3
             *     V1_tag2_version2
             *     V1_tag2_version1
             *     V1_tag3_version3
             *     V1_tag3_version2
             *     V1_tag3_version1
             * Because index depends on latest version of tag.
             * So only V1_tag1_version3, V1_tag2_version3 and V1_tag3_version3 are needed,
             * Using latestTagId to identify if it is the latest version
             */
            if (latestTagId != tagId) {
                RowReaderWrapper reader;
                /*
                 * Step 1, Delete the normal index of the vertex and the tag if exists.
                 */
                for (auto& index : indexes_) {
                    if (index->get_schema_id().get_tag_id() == tagId) {
                        auto indexId = index->get_index_id();

                        if (reader == nullptr) {
                            reader = RowReaderWrapper::getTagPropReader(env_->schemaMan_,
                                                                        spaceId_,
                                                                        tagId,
                                                                        iter->val());
                            if (reader == nullptr) {
                                LOG(WARNING) << "Bad format row";
                                return folly::none;
                            }
                        }
                        std::vector<Value::Type> colsType;
                        const auto& cols = index->get_fields();
                        auto valuesRet = IndexKeyUtils::collectIndexValues(reader.get(),
                                                                           cols,
                                                                           colsType);
                        if (!valuesRet.ok()) {
                            continue;
                        }
                        auto indexKey = IndexKeyUtils::vertexIndexKey(spaceVidLen_,
                                                                      partId,
                                                                      indexId,
                                                                      vertex.getStr(),
                                                                      valuesRet.value(),
                                                                      colsType);

                        // Check the index is building for the specified partition or not
                        if (env_->checkRebuilding(spaceId_, partId, indexId)) {
                            auto deleteOpKey = OperationKeyUtils::deleteOperationKey(partId);
                            batchHolder->put(std::move(deleteOpKey), std::move(indexKey));
                        } else if (env_->checkIndexLocked(spaceId_, partId, indexId)) {
                            LOG(ERROR) << "The index has been locked: " << index->get_index_name();
                            return folly::none;
                        } else {
                            batchHolder->remove(std::move(indexKey));
                        }
                    }
                }

                /*
                 * Step 2, Delete vertex index
                 */
                auto vIndexIt = tagIdToIndexId_.find(tagId);
                if (vIndexIt != tagIdToIndexId_.end()) {
                    auto vIndexId = vIndexIt->second;
                    auto vIndexKey = StatisticsIndexKeyUtils::vertexIndexKey(spaceVidLen_,
                                                                             partId,
                                                                             vIndexId,
                                                                             vertex.getStr());
                    if (!vIndexKey.empty()) {
                        // Check the index is building for the specified partition or not
                        if (env_->checkRebuilding(spaceId_, partId, vIndexId)) {
                            auto deleteOpKey = OperationKeyUtils::deleteOperationKey(partId);
                            batchHolder->put(std::move(deleteOpKey), std::move(vIndexKey));
                        } else if (env_->checkIndexLocked(spaceId_, partId, vIndexId)) {
                            LOG(ERROR) << "The index has been locked, index id " << vIndexId;
                            return folly::none;
                        } else {
                            batchHolder->remove(std::move(vIndexKey));
                        }
                    }
                }
                latestTagId = tagId;
            }

            /*
             * Step 3, Delete the vertex data
             */
            batchHolder->remove(key.str());

            /*
             * Step 4, for statistics all vertex count
             */
            if (allVertexStatIndex_ != nullptr) {
                vertexIds.emplace(vertex.getStr());
            }

            iter->next();
        }
    }

    /*
     * Step 5, update statistic all vertex index data
     * When value is 0, remove index data
     */
    if (allVertexStatIndex_ != nullptr) {
        auto vCountIndexId = allVertexStatIndex_->get_index_id();
        int64_t countVal = vertexIds.size();
        if (countVal != 0) {
            std::string val;
            auto vCountIndexKey = StatisticsIndexKeyUtils::countIndexKey(partId, vCountIndexId);
            auto ret = env_->kvstore_->get(spaceId_, partId, vCountIndexKey, &val);
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                if (ret != kvstore::ResultCode::ERR_KEY_NOT_FOUND) {
                    LOG(ERROR) << "Get statistics index error";
                    return folly::none;
                } else {
                    VLOG(3) << "Statistic all vertex index data not exist, partID " << partId;
                }
            } else {
                countVal = *reinterpret_cast<const int64_t*>(val.c_str()) - countVal;

                if (countVal > 0) {
                    auto newCount = std::string(reinterpret_cast<const char*>(&countVal),
                                                sizeof(int64_t));
                    auto retRebuild = rebuildingModifyOp(spaceId_,
                                                         partId,
                                                         vCountIndexId,
                                                         vCountIndexKey,
                                                         batchHolder.get(),
                                                         newCount);
                    if (retRebuild == folly::none) {
                        return folly::none;
                    }
                } else {
                    if (countVal < 0) {
                        LOG(ERROR) << "statistics all vertex index value is illegal, "
                                   << "please rebuild index";
                    }
                    // Remove statistic all vertex index data
                    if (env_->checkRebuilding(spaceId_, partId, vCountIndexId)) {
                        auto deleteOpKey = OperationKeyUtils::deleteOperationKey(partId);
                        batchHolder->put(std::move(deleteOpKey), std::move(vCountIndexKey));
                    } else if (env_->checkIndexLocked(spaceId_, partId, vCountIndexId)) {
                        LOG(ERROR) << "The index has been locked, index id " << vCountIndexId;
                        return folly::none;
                    } else {
                        batchHolder->remove(std::move(vCountIndexKey));
                    }
                }
            }
        }
    }

    return encodeBatchValue(batchHolder->getBatch());
}

}  // namespace storage
}  // namespace nebula
