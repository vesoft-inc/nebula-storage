/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef STORAGE_EXEC_INDEXVERTEXNODE_H_
#define STORAGE_EXEC_INDEXVERTEXNODE_H_

#include "common/base/Base.h"
#include "storage/exec/RelNode.h"
#include "storage/exec/IndexScanNode.h"

namespace nebula {
namespace storage {

template<typename T>
class IndexVertexNode final : public RelNode<T> {
public:
    IndexVertexNode(StorageEnv* env,
                    GraphSpaceID spaceId,
                    size_t vIdLen,
                    TagID tagId,
                    VertexCache* vertexCache,
                    IndexScanNode<T>* indexScanNode)
        : env_(env)
        , spaceId_(spaceId)
        , vIdLen_(vIdLen)
        , tagId_(tagId)
        , vertexCache_(vertexCache)
        , indexScanNode_(indexScanNode) {}

    kvstore::ResultCode execute(PartitionID partId) override {
        auto ret = RelNode<T>::execute(partId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }
        auto* iter = static_cast<VertexIndexIterator*>(indexScanNode_->iterator());
        while (iter->valid()) {
            auto vId = iter->vId();
            VLOG(1) << "partId " << partId << ", vId " << vId << ", tagId " << tagId_;
            if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
                auto result = vertexCache_->get(std::make_pair(vId, tagId_), partId);
                if (result.ok()) {
                    data_.emplace_back(std::move(result).value());
                    continue;
                } else {
                    VLOG(1) << "Miss cache for vId " << vId << ", tagId " << tagId_;
                }
            }

            std::unique_ptr<kvstore::KVIterator> vIter;
            auto prefix = NebulaKeyUtils::vertexPrefix(vIdLen_, partId, vId, tagId_);
            ret = env_->kvstore_->prefix(spaceId_, partId, prefix, &vIter);
            if (ret == kvstore::ResultCode::SUCCEEDED && vIter && vIter->valid()) {
                data_.emplace_back(vIter->val());
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
    TagID                    tagId_;
    VertexCache*             vertexCache_;
    IndexScanNode<T>*        indexScanNode_;
    std::vector<std::string> data_;
};

}  // namespace storage
}  // namespace nebula
#endif   // STORAGE_EXEC_INDEXVERTEXNODE_H_
