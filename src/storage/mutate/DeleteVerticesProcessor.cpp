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

ProcessorCounters kDelVerticesCounters;

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
    auto iRet = env_->indexMan_->getTagIndexes(spaceId_);
    if (!iRet.ok()) {
        LOG(ERROR) << iRet.status();
        for (auto& part : partVertices) {
            pushResultCode(cpp2::ErrorCode::E_SPACE_NOT_FOUND, part.first);
        }
        onFinished();
        return;
    }
    indexes_ = std::move(iRet).value();

    CHECK_NOTNULL(env_->kvstore_);
    if (indexes_.empty()) {
        doProcess(req);
    } else {
        doProcessWithIndex(req);
    }
}

void DeleteVerticesProcessor::doProcess(const cpp2::DeleteVerticesRequest& req) {
    for (auto& part : req.get_parts()) {
        auto partId = part.first;
        const auto& vertexIds = part.second;
        // Operate every part, the graph layer guarantees the unique of the vid
        std::vector<std::string> keys;
        keys.reserve(32);

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
            while (iter->valid()) {
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
        doRemove(spaceId_, partId, std::move(keys));
    }
}

void DeleteVerticesProcessor::doProcessWithIndex(const cpp2::DeleteVerticesRequest& req) {
    for (auto& part : req.get_parts()) {
        auto partId = part.first;
        const auto& vertices = part.second;
        IndexCountWrapper wrapper(env_);
        std::unique_ptr<kvstore::BatchHolder> batchHolder =
        std::make_unique<kvstore::BatchHolder>();
        std::vector<VMLI> dummyLock;
        for (auto& vertex : vertices) {
            auto prefix = NebulaKeyUtils::vertexPrefix(spaceVidLen_, partId, vertex.getStr());
            std::unique_ptr<kvstore::KVIterator> iter;
            auto ret = env_->kvstore_->prefix(spaceId_, partId, prefix, &iter);
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                VLOG(3) << "Error! ret = " << static_cast<int32_t>(ret)
                        << ", spaceId " << spaceId_;
                handleErrorCode(ret, spaceId_, partId);
                onFinished();
                return;
            }

            while (iter->valid()) {
                auto key = iter->key();
                if (!NebulaKeyUtils::isVertex(spaceVidLen_, key)) {
                    iter->next();
                    continue;
                }

                auto tagId = NebulaKeyUtils::getTagId(spaceVidLen_, key);

                RowReaderWrapper reader;
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
                                handleErrorCode(kvstore::ResultCode::ERR_INVALID_DATA,
                                                spaceId_,
                                                partId);
                                onFinished();
                                return;
                            }
                        }

                        const auto& cols = index->get_fields();
                        auto valuesRet = IndexKeyUtils::collectIndexValues(reader.get(),
                                                                           cols);
                        if (!valuesRet.ok()) {
                            continue;
                        }
                        auto indexKey = IndexKeyUtils::vertexIndexKey(
                            spaceVidLen_, partId, indexId, vertex.getStr(),
                            std::move(valuesRet).value());

                        // Check the index is building for the specified partition or not
                        auto indexState = env_->getIndexState(spaceId_, partId);
                        if (env_->checkRebuilding(indexState)) {
                            auto deleteOpKey = OperationKeyUtils::deleteOperationKey(partId);
                            batchHolder->put(std::move(deleteOpKey), std::move(indexKey));
                        } else if (env_->checkIndexLocked(indexState)) {
                            LOG(ERROR) << "The index has been locked: "
                                       << index->get_index_name();
                            pushResultCode(cpp2::ErrorCode::E_CONSENSUS_ERROR, partId);
                            onFinished();
                            return;
                        } else {
                            batchHolder->remove(std::move(indexKey));
                        }
                    }
                }
                dummyLock.emplace_back(std::make_tuple(spaceId_, partId, tagId, vertex.getStr()));

                if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
                    VLOG(3) << "Evict vertex cache for vertex ID " << vertex << ", tagId " << tagId;
                    vertexCache_->evict(std::make_pair(vertex.getStr(), tagId));
                }
                batchHolder->remove(key.str());
                iter->next();
            }
            auto atomic = encodeBatchValue(std::move(batchHolder)->getBatch());
            if (atomic.empty()) {
                handleAsync(spaceId_, partId, kvstore::ResultCode::SUCCEEDED);
            } else {
                nebula::MemoryLockGuard<VMLI> lg(env_->verticesML_.get(), dummyLock, true);
                if (!lg) {
                    auto conflict = lg.conflictKey();
                    LOG(ERROR) << "edge conflict "
                               << std::get<0>(conflict) << ":"
                               << std::get<1>(conflict) << ":"
                               << std::get<2>(conflict) << ":"
                               << std::get<3>(conflict);
                    pushResultCode(cpp2::ErrorCode::E_CONSENSUS_ERROR, partId);
                    onFinished();
                    return;
                }
                auto callback = [partId, this](kvstore::ResultCode code) {
                    handleAsync(spaceId_, partId, code);
                };
                env_->kvstore_->asyncAppendBatch(spaceId_, partId, std::move(atomic), callback);
            }
        }
    }
}

}  // namespace storage
}  // namespace nebula
