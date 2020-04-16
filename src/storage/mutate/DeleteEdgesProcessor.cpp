/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "storage/mutate/DeleteEdgesProcessor.h"
#include <algorithm>
#include <limits>
#include "common/NebulaKeyUtils.h"

namespace nebula {
namespace storage {

void DeleteEdgesProcessor::process(const cpp2::DeleteEdgesRequest& req) {
    spaceId_ = req.get_space_id();
    const auto& partEdges = req.get_parts();

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
    auto spaceVidLen = ret.value();
    callingNum_ = partEdges.size();

    CHECK_NOTNULL(env_->indexMan_);
    auto iRet = env_->indexMan_->getEdgeIndexes(spaceId_);
    if (iRet.ok()) {
        indexes_ = std::move(iRet).value();
    }

    CHECK_NOTNULL(env_->kvstore_);
    if (indexes_.empty()) {
        // Operate every part, the graph layer guarantees the unique of the edgeKey
        std::vector<std::string> keys;
        keys.reserve(32);
        for (auto& part : partEdges) {
            auto partId = part.first;
            keys.clear();
            for (auto& edgeKey : part.second) {
                auto start = NebulaKeyUtils::edgeKey(spaceVidLen,
                                                     partId,
                                                     edgeKey.src,
                                                     edgeKey.edge_type,
                                                     edgeKey.ranking,
                                                     edgeKey.dst,
                                                     0);
                auto end = NebulaKeyUtils::edgeKey(spaceVidLen,
                                                   partId,
                                                   edgeKey.src,
                                                   edgeKey.edge_type,
                                                   edgeKey.ranking,
                                                   edgeKey.dst,
                                                   std::numeric_limits<int64_t>::max());
                std::unique_ptr<kvstore::KVIterator> iter;
                auto retRes = env_->kvstore_->range(spaceId_, partId, start, end, &iter);
                if (retRes != kvstore::ResultCode::SUCCEEDED) {
                    VLOG(3) << "Error! ret = " << static_cast<int32_t>(retRes)
                            << ", spaceID " << spaceId_;
                    this->handleErrorCode(retRes, spaceId_, partId);
                    this->onFinished();
                    return;
                }
                while (iter && iter->valid()) {
                    auto key = iter->key();
                    keys.emplace_back(key.data(), key.size());
                    iter->next();
                }
            }
            doRemove(spaceId_, partId, keys);
        }
    } else {
        for (auto& part : partEdges) {
            auto partId = part.first;
            auto atomic = [partId, edgeKeys = std::move(part.second), this]()
                          -> folly::Optional<std::string> {
               return this->deleteEdges(partId, edgeKeys);
            };
            auto callback = [partId, this](kvstore::ResultCode code) {
                this->handleAsync(this->spaceId_, partId, code);
            };
            env_->kvstore_->asyncAtomicOp(spaceId_, partId, atomic, callback);
        }
    }
}

folly::Optional<std::string>
DeleteEdgesProcessor::deleteEdges(PartitionID partId,
                                  const std::vector<cpp2::EdgeKey>& edges) {
    UNUSED(partId);
    UNUSED(edges);
    return std::string("");
#if 0
    std::unique_ptr<kvstore::BatchHolder> batchHolder = std::make_unique<kvstore::BatchHolder>();
    for (auto& edge : edges) {
        auto type = edge.edge_type;
        auto srcId = edge.src;
        auto rank = edge.ranking;
        auto dstId = edge.dst;
        auto prefix = NebulaKeyUtils::edgePrefix(partId, srcId, type, rank, dstId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = this->kvstore_->prefix(spaceId, partId, prefix, &iter);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            VLOG(3) << "Error! ret = " << static_cast<int32_t>(ret)
                    << ", spaceId " << spaceId;
            return folly::none;
        }
        bool isLatestVE = true;
        while (iter->valid()) {
            /**
             * just get the latest version edge for index.
             */
            if (isLatestVE) {
                std::unique_ptr<RowReader> reader;
                for (auto& index : indexes_) {
                    auto indexId = index->get_index_id();
                    if (type == index->get_schema_id().get_edge_type()) {
                        if (reader == nullptr) {
                            reader = RowReader::getEdgePropReader(this->schemaMan_,
                                                                  iter->val(),
                                                                  spaceId,
                                                                  type);
                        }
                        auto values = collectIndexValues(reader.get(),
                                                         index->get_fields());
                        auto indexKey = NebulaKeyUtils::edgeIndexKey(partId,
                                                                     indexId,
                                                                     srcId,
                                                                     rank,
                                                                     dstId,
                                                                     values);
                        batchHolder->remove(std::move(indexKey));
                    }
                }
                isLatestVE = false;
            }
            batchHolder->remove(iter->key().str());
            iter->next();
        }
    }
    return encodeBatchValue(batchHolder->getBatch());
#endif
}

}  // namespace storage
}  // namespace nebula

