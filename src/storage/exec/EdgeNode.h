/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_EDGENODE_H_
#define STORAGE_EXEC_EDGENODE_H_

#include "base/Base.h"
#include "storage/exec/RelNode.h"
#include "storage/exec/StorageIterator.h"

namespace nebula {
namespace storage {

// used to save stat value of each vertex
struct PropStat {
    PropStat() = default;

    explicit PropStat(const cpp2::StatType& statType) : statType_(statType) {}

    cpp2::StatType statType_;
    mutable Value sum_ = 0L;
    mutable int32_t count_ = 0;
};

// EdgeNode will return a StorageIterator which iterates over the specified
// edgeType of given vertexId
class EdgeNode : public RelNode {
public:
    EdgeNode(EdgeContext* ctx,
             StorageEnv* env,
             GraphSpaceID spaceId,
             size_t vIdLen,
             EdgeType edgeType,
             const std::vector<PropContext>* props)
        : edgeContext_(ctx)
        , env_(env)
        , spaceId_(spaceId)
        , vIdLen_(vIdLen)
        , edgeType_(edgeType)
        , props_(props) {}

    EdgeNode(EdgeContext* ctx,
             StorageEnv* env,
             GraphSpaceID spaceId,
             size_t vIdLen)
        : edgeContext_(ctx)
        , env_(env)
        , spaceId_(spaceId)
        , vIdLen_(vIdLen) {}

    StorageIterator* iter() {
        return iter_.get();
    }

protected:
    EdgeContext* edgeContext_;
    StorageEnv* env_;
    GraphSpaceID spaceId_;
    size_t vIdLen_;
    EdgeType edgeType_;
    const std::vector<PropContext>* props_;

    std::unique_ptr<StorageIterator> iter_;
    std::string prefix_;
};

class EdgeTypePrefixScanNode final : public EdgeNode {
public:
    EdgeTypePrefixScanNode(EdgeContext* ctx,
                           StorageEnv* env,
                           GraphSpaceID spaceId,
                           size_t vIdLen,
                           EdgeType edgeType,
                           const std::vector<PropContext>* props)
        : EdgeNode(ctx, env, spaceId, vIdLen, edgeType, props) {}

    folly::Future<kvstore::ResultCode> execute(PartitionID partId, const VertexID& vId) override {
        VLOG(1) << "partId " << partId << ", vId " << vId << ", edgeType " << edgeType_
                << ", prop size " << props_->size();
        auto schemaIter = edgeContext_->schemas_.find(std::abs(edgeType_));
        if (schemaIter == edgeContext_->schemas_.end() || schemaIter->second.empty()) {
            return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
        }

        std::unique_ptr<kvstore::KVIterator> iter;
        prefix_ = NebulaKeyUtils::edgePrefix(vIdLen_, partId, vId, edgeType_);
        auto ret = env_->kvstore_->prefix(spaceId_, partId, prefix_, &iter);
        if (ret == kvstore::ResultCode::SUCCEEDED && iter && iter->valid()) {
            iter_.reset(new SingleEdgeIterator(std::move(iter), edgeType_, vIdLen_));
        }
        return kvstore::ResultCode::SUCCEEDED;
    }
};

class VertexPrefixScanNode final : public EdgeNode {
public:
    VertexPrefixScanNode(EdgeContext* ctx,
                         StorageEnv* env,
                         GraphSpaceID spaceId,
                         size_t vIdLen)
        : EdgeNode(ctx, env, spaceId, vIdLen) {}

    folly::Future<kvstore::ResultCode> execute(PartitionID partId, const VertexID& vId) override {
        VLOG(1) << "partId " << partId << ", vId " << vId << ", scan all edges";
        std::unique_ptr<kvstore::KVIterator> iter;
        prefix_ = NebulaKeyUtils::edgePrefix(vIdLen_, partId, vId);
        auto ret = env_->kvstore_->prefix(spaceId_, partId, prefix_, &iter);
        if (ret == kvstore::ResultCode::SUCCEEDED && iter && iter->valid()) {
            iter_.reset(new AllEdgeIterator(edgeContext_, std::move(iter), vIdLen_));
        }
        return kvstore::ResultCode::SUCCEEDED;
    }
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_EDGENODE_H_
