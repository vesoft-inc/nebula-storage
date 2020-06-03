/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef STORAGE_EXEC_INDEXEDGENODE_H_
#define STORAGE_EXEC_INDEXEDGENODE_H_

#include "common/base/Base.h"
#include "storage/exec/RelNode.h"

namespace nebula {
namespace storage {

template<typename T>
class IndexEdgeNode final : public RelNode<T> {
public:
    IndexEdgeNode(StorageEnv* env,
                  GraphSpaceID spaceId,
                  size_t vIdLen,
                  EdgeType edgeType,
                  IndexScanNode<T>* indexScanNode)
        : env_(env)
        , spaceId_(spaceId)
        , vIdLen_(vIdLen)
        , edgeType_(edgeType)
        , indexScanNode_(indexScanNode) {}

    kvstore::ResultCode execute(PartitionID partId) override {
        auto ret = RelNode<T>::execute(partId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }
        auto* iter = static_cast<EdgeIndexIterator*>(indexScanNode_->iterator());
        while (iter->valid()) {
            auto src = IndexKeyUtils::getIndexSrcId(vIdLen_, iter->key());
            auto dst = IndexKeyUtils::getIndexDstId(vIdLen_, iter->key());
            auto rank = IndexKeyUtils::getIndexRank(vIdLen_, iter->key());
            auto prefix = NebulaKeyUtils::edgePrefix(vIdLen_, partId, src.str(),
                                                     edgeType_, rank, dst.str());
            std::unique_ptr<kvstore::KVIterator> eIter;
            ret = env_->kvstore_->prefix(spaceId_, partId, prefix, &eIter);
            if (ret == kvstore::ResultCode::SUCCEEDED && eIter && eIter->valid()) {
                data_.emplace_back(iter->val());
            } else {
                return ret;
            }
            iter->next();
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

    const std::vector<std::string>& getData() const {
        return std::move(data_);
    }

private:
    StorageEnv*              env_;
    GraphSpaceID             spaceId_;
    size_t                   vIdLen_;
    EdgeType                 edgeType_;
    IndexScanNode<T>*        indexScanNode_;
    std::vector<std::string> data_;
};

}  // namespace storage
}  // namespace nebula
#endif   // STORAGE_EXEC_INDEXEDGENODE_H_
