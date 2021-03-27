/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/mutate/DeleteEdgesProcessor.h"
#include <algorithm>
#include "utils/IndexKeyUtils.h"
#include "utils/NebulaKeyUtils.h"
#include "utils/OperationKeyUtils.h"

namespace nebula {
namespace storage {

ProcessorCounters kDelEdgesCounters;

void DeleteEdgesProcessor::process(const cpp2::DeleteEdgesRequest& req) {
    spaceId_ = req.get_space_id();
    const auto& partEdges = req.get_parts();

    CHECK_NOTNULL(env_->schemaMan_);
    auto ret = env_->schemaMan_->getSpaceVidLen(spaceId_);
    if (!ret.ok()) {
        LOG(ERROR) << ret.status();
        for (auto& part : partEdges) {
            pushResultCode(cpp2::ErrorCode::E_INVALID_SPACEVIDLEN, part.first);
        }
        onFinished();
        return;
    }
    spaceVidLen_ = ret.value();
    callingNum_ = partEdges.size();

    CHECK_NOTNULL(env_->indexMan_);
    auto iRet = env_->indexMan_->getEdgeIndexes(spaceId_);
    if (!iRet.ok()) {
        LOG(ERROR) << iRet.status();
        for (auto& part : partEdges) {
            pushResultCode(cpp2::ErrorCode::E_SPACE_NOT_FOUND, part.first);
        }
        onFinished();
        return;
    }
    indexes_ = std::move(iRet).value();

    CHECK_NOTNULL(env_->kvstore_);
    if (indexes_.empty()) {
        // Operate every part, the graph layer guarantees the unique of the edgeKey
        for (auto& part : partEdges) {
            std::vector<std::string> keys;
            keys.reserve(32);
            auto partId = part.first;
            cpp2::ErrorCode code = cpp2::ErrorCode::SUCCEEDED;
            for (auto& edgeKey : part.second) {
                if (!NebulaKeyUtils::isValidVidLen(
                        spaceVidLen_, edgeKey.src.getStr(), edgeKey.dst.getStr())) {
                    LOG(ERROR) << "Space " << spaceId_ << " vertex length invalid, "
                               << "space vid len: " << spaceVidLen_
                               << ", edge srcVid: " << edgeKey.src << " dstVid: " << edgeKey.dst;
                    code = cpp2::ErrorCode::E_INVALID_VID;
                    break;
                }
                // todo(doodle): delete lock in toss
                auto edge = NebulaKeyUtils::edgeKey(spaceVidLen_,
                                                    partId,
                                                    edgeKey.src.getStr(),
                                                    edgeKey.edge_type,
                                                    edgeKey.ranking,
                                                    edgeKey.dst.getStr());
                keys.emplace_back(edge.data(), edge.size());
            }
            if (code != cpp2::ErrorCode::SUCCEEDED) {
                handleAsync(spaceId_, partId, code);
                continue;
            }
            doRemove(spaceId_, partId, std::move(keys));
        }
    } else {
        for (auto& part : partEdges) {
            IndexCountWrapper wrapper(env_);
            auto partId = part.first;
            std::vector<EMLI> dummyLock;
            dummyLock.reserve(part.second.size());

            for (const auto& edgeKey : part.second) {
                dummyLock.emplace_back(std::make_tuple(spaceId_,
                                                       partId,
                                                       edgeKey.src.getStr(),
                                                       edgeKey.edge_type,
                                                       edgeKey.ranking,
                                                       edgeKey.dst.getStr()));
            }
            auto batch = deleteEdges(partId, std::move(part.second));
            if (!nebula::ok(batch)) {
                handleAsync(spaceId_, partId, nebula::error(batch));
                continue;
            }
            DCHECK(!nebula::value(batch).empty());
            nebula::MemoryLockGuard<EMLI> lg(env_->edgesML_.get(), std::move(dummyLock), true);
            if (!lg) {
                auto conflict = lg.conflictKey();
                LOG(ERROR) << "edge conflict "
                        << std::get<0>(conflict) << ":"
                        << std::get<1>(conflict) << ":"
                        << std::get<2>(conflict) << ":"
                        << std::get<3>(conflict) << ":"
                        << std::get<4>(conflict) << ":"
                        << std::get<5>(conflict);
                handleAsync(spaceId_, partId, cpp2::ErrorCode::E_DATA_CONFLICT_ERROR);
                continue;
            }
            env_->kvstore_->asyncAppendBatch(spaceId_, partId, std::move(nebula::value(batch)),
                [l = std::move(lg), icw = std::move(wrapper), partId, this] (
                    kvstore::ResultCode code) {
                    UNUSED(l);
                    UNUSED(icw);
                    handleAsync(spaceId_, partId, code);
                });
        }
    }
}


ErrorOr<kvstore::ResultCode, std::string>
DeleteEdgesProcessor::deleteEdges(PartitionID partId, const std::vector<cpp2::EdgeKey>& edges) {
    std::unique_ptr<kvstore::BatchHolder> batchHolder = std::make_unique<kvstore::BatchHolder>();
    for (auto& edge : edges) {
        auto type = edge.edge_type;
        auto srcId = edge.src.getStr();
        auto rank = edge.ranking;
        auto dstId = edge.dst.getStr();
        auto prefix = NebulaKeyUtils::edgePrefix(spaceVidLen_, partId, srcId, type, rank, dstId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = env_->kvstore_->prefix(spaceId_, partId, prefix, &iter);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            VLOG(3) << "Error! ret = " << static_cast<int32_t>(ret)
                    << ", spaceId " << spaceId_;
            return ret;
        }

        while (iter->valid() && NebulaKeyUtils::isLock(spaceVidLen_, iter->key())) {
            batchHolder->remove(iter->key().str());
            iter->next();
        }

        if (iter->valid() && NebulaKeyUtils::isEdge(spaceVidLen_, iter->key())) {
            /**
             * just get the latest version edge for index.
             */
            RowReaderWrapper reader;
            for (auto& index : indexes_) {
                if (type == index->get_schema_id().get_edge_type()) {
                    auto indexId = index->get_index_id();

                    if (reader == nullptr) {
                        reader = RowReaderWrapper::getEdgePropReader(env_->schemaMan_,
                                                                     spaceId_,
                                                                     type,
                                                                     iter->val());
                        if (reader == nullptr) {
                            LOG(WARNING) << "Bad format row!";
                            return kvstore::ResultCode::ERR_INVALID_DATA;
                        }
                    }
                    auto valuesRet = IndexKeyUtils::collectIndexValues(reader.get(),
                                                                       index->get_fields());
                    if (!valuesRet.ok()) {
                        continue;
                    }
                    auto indexKey = IndexKeyUtils::edgeIndexKey(spaceVidLen_, partId,
                                                                indexId,
                                                                srcId,
                                                                rank,
                                                                dstId,
                                                                std::move(valuesRet).value());

                    auto indexState = env_->getIndexState(spaceId_, partId);
                    if (env_->checkRebuilding(indexState)) {
                        auto deleteOpKey = OperationKeyUtils::deleteOperationKey(partId);
                        batchHolder->put(std::move(deleteOpKey), std::move(indexKey));
                    } else if (env_->checkIndexLocked(indexState)) {
                        LOG(ERROR) << "The index has been locked: " << index->get_index_name();
                        return kvstore::ResultCode::ERR_DATA_CONFLICT_ERROR;
                    } else {
                        batchHolder->remove(std::move(indexKey));
                    }
                }
            }

            batchHolder->remove(iter->key().str());
            iter->next();
        }

        while (iter->valid()) {
            batchHolder->remove(iter->key().str());
            iter->next();
        }
    }

    return encodeBatchValue(batchHolder->getBatch());
}

}  // namespace storage
}  // namespace nebula

