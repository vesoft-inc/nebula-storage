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
    IndexVertexNode(PlanContext* planCtx,
                    VertexCache* vertexCache,
                    IndexScanNode<T>* indexScanNode,
                    std::shared_ptr<const meta::NebulaSchemaProvider> schema,
                    std::string&& schemaName)
        : planContext_(planCtx)
        , vertexCache_(vertexCache)
        , indexScanNode_(indexScanNode)
        , schema_(schema)
        , schemaName_(std::move(schemaName)) {}

    kvstore::ResultCode execute(PartitionID partId) override {
        auto ret = RelNode<T>::execute(partId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }
        data_.clear();
        std::vector<VertexID> vids;
        auto* iter = static_cast<VertexIndexIterator*>(indexScanNode_->iterator());
        while (iter && iter->valid()) {
            vids.emplace_back(iter->vId());
            iter->next();
        }
        for (const auto& vId : vids) {
            VLOG(1) << "partId " << partId << ", vId " << vId << ", tagId " << planContext_->tagId_;
            if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
                auto result = vertexCache_->get(std::make_pair(vId, planContext_->tagId_), partId);
                if (result.ok()) {
                    auto vertexKey = NebulaKeyUtils::vertexKey(planContext_->vIdLen_,
                                                               partId,
                                                               vId,
                                                               planContext_->tagId_,
                                                               0);
                    data_.emplace_back(std::move(vertexKey), std::move(result).value());
                    continue;
                } else {
                    VLOG(1) << "Miss cache for vId " << vId << ", tagId " << planContext_->tagId_;
                }
            }

            std::unique_ptr<kvstore::KVIterator> vIter;
            auto prefix = NebulaKeyUtils::vertexPrefix(planContext_->vIdLen_, partId,
                                                       vId, planContext_->tagId_);
            ret = planContext_->env_->kvstore_->prefix(planContext_->spaceId_,
                                                       partId, prefix, &vIter);
            if (ret == kvstore::ResultCode::SUCCEEDED && vIter && vIter->valid()) {
                data_.emplace_back(vIter->key(), vIter->val());
            } else {
                return ret;
            }
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

    const std::vector<kvstore::KV>& getData() const {
        return data_;
    }

    const meta::NebulaSchemaProvider* getSchema() {
        return schema_.get();
    }

    const std::string& getSchemaName() {
        return schemaName_;
    }

private:
    PlanContext*                                      planContext_;
    VertexCache*                                      vertexCache_;
    IndexScanNode<T>*                                 indexScanNode_;
    std::vector<kvstore::KV>                          data_;
    std::shared_ptr<const meta::NebulaSchemaProvider> schema_{nullptr};
    const std::string&                                schemaName_;
};

}  // namespace storage
}  // namespace nebula
#endif   // STORAGE_EXEC_INDEXVERTEXNODE_H_
