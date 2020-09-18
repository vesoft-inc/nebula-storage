/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef STORAGE_EXEC_INDEXNODE_H_
#define STORAGE_EXEC_INDEXNODE_H_

#include "common/base/Base.h"
#include "storage/exec/RelNode.h"

namespace nebula {
namespace storage {

template<typename T>
class IndexNode : public RelNode<T> {
public:
    using RelNode<T>::execute;

    IndexNode(PlanContext* planCtx,
              std::shared_ptr<const meta::NebulaSchemaProvider> schema,
              std::string&& schemaName)
        : planContext_(planCtx)
        , schema_(schema)
        , schemaName_(std::move(schemaName)) {}

    std::vector<kvstore::KV> moveData() {
        return std::move(data_);
    }

    const meta::NebulaSchemaProvider* getSchema() {
        return schema_.get();
    }

    const std::string& getSchemaName() {
        return schemaName_;
    }

protected:
    PlanContext*                                      planContext_;
    std::vector<kvstore::KV>                          data_;
    std::shared_ptr<const meta::NebulaSchemaProvider> schema_{nullptr};
    const std::string&                                schemaName_;
};

class IndexVertexNode final : public IndexNode<IndexID> {
public:
    using RelNode<IndexID>::execute;

    IndexVertexNode(PlanContext* planCtx,
                    IndexScanNode<IndexID>* indexScanNode,
                    std::shared_ptr<const meta::NebulaSchemaProvider> schema,
                    std::string&& schemaName,
                    VertexCache* vertexCache)
        : IndexNode(planCtx, schema, std::forward<std::string>(schemaName))
        , vertexCache_(vertexCache)
        , indexScanNode_(indexScanNode) {}

    kvstore::ResultCode execute(PartitionID partId) override {
        auto ret = RelNode<IndexID>::execute(partId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }
        data_.clear();

        auto* iter = static_cast<VertexIndexIterator*>(indexScanNode_->iterator());
        while (iter && iter->valid()) {
            auto vId = iter->vId();
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
            iter->next();
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

private:
    VertexCache*                                      vertexCache_;
    IndexScanNode<IndexID>*                           indexScanNode_;
};

class IndexEdgeNode final : public IndexNode<IndexID> {
public:
    using RelNode<IndexID>::execute;

    IndexEdgeNode(PlanContext* planCtx,
                  IndexScanNode<IndexID>* indexScanNode,
                  std::shared_ptr<const meta::NebulaSchemaProvider> schema,
                  std::string&& schemaName)
        : IndexNode(planCtx, schema, std::forward<std::string>(schemaName))
        , indexScanNode_(indexScanNode) {}

    kvstore::ResultCode execute(PartitionID partId) override {
        auto ret = RelNode<IndexID>::execute(partId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }
        data_.clear();

        auto* iter = static_cast<EdgeIndexIterator*>(indexScanNode_->iterator());
        while (iter && iter->valid()) {
            auto prefix = NebulaKeyUtils::edgePrefix(planContext_->vIdLen_,
                                                     partId,
                                                     iter->srcId(),
                                                     planContext_->edgeType_,
                                                     iter->ranking(),
                                                     iter->dstId());
            std::unique_ptr<kvstore::KVIterator> eIter;
            ret = planContext_->env_->kvstore_->prefix(planContext_->spaceId_,
                                                       partId, prefix, &eIter);
            if (ret == kvstore::ResultCode::SUCCEEDED && eIter && eIter->valid()) {
                data_.emplace_back(eIter->key(), eIter->val());
            } else {
                return ret;
            }
            iter->next();
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

private:
    IndexScanNode<IndexID>*                      indexScanNode_;
};

}  // namespace storage
}  // namespace nebula
#endif   // STORAGE_EXEC_INDEXNODE_H_
