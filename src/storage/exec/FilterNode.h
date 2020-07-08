/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_FILTERNODE_H_
#define STORAGE_EXEC_FILTERNODE_H_

#include "common/base/Base.h"
#include "common/expression/Expression.h"
#include "storage/exec/HashJoinNode.h"
#include "storage/context/StorageExpressionContext.h"

namespace nebula {
namespace storage {

<<<<<<< HEAD

=======
>>>>>>> unify filterNode
/*
FilterNode will receive the result from upstream, check whether tag data or edge data
could pass the expression filter. FilterNode can only accept one upstream node, user
must make sure that the upstream only output only tag data or edge data, but not both.

As for GetNeighbors, it will have filter that involves both tag and edge expression. In
that case, FilterNode has a upstream of HashJoinNode, which will keeps poping out edge
data. All tage data has been put into ExpressionContext before FilterNode is executed.
By that means, it can check the filter of tag + edge.
*/
template<typename T>
class FilterNode : public IterateNode<T> {
public:
    FilterNode(PlanContext* planCtx,
               IterateNode<T>* upstream,
               bool isEdge = true,
               StorageExpressionContext* expCtx = nullptr,
               Expression* exp = nullptr,
               bool isUpdate = false,
               TagContext* tctx = nullptr,
               EdgeContext* ectx = nullptr)
        : IterateNode<T>(upstream)
        , planContext_(planCtx)
        , isEdge_(isEdge)
        , expCtx_(expCtx)
        , filterExp_(exp)
        , isUpdate_(isUpdate)
        , tagContext_(tctx)
        , edgeContext_(ectx) {}

    kvstore::ResultCode execute(PartitionID partId, const T& vId) override {
        auto ret = RelNode<T>::execute(partId, vId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        if (isEdge_) {
            entryName_ = planContext_->edgeName_;
            schema_ = planContext_->edgeSchema_;
        } else {
            entryName_ = planContext_->tagName_;
            schema_ = planContext_->tagSchema_;
        }

        if (isUpdate_) {
            if (isEdge_) {
                if (!schema_) {
                    VLOG(1) << "Fail to get schema in edgeType " << planContext_->edgeType_;
                    return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
                }
            } else {
                if (!schema_) {
                    VLOG(1) << "Fail to get schema in TagId " << planContext_->tagId_;
                    return kvstore::ResultCode::ERR_TAG_NOT_FOUND;
                }
            }
            // when this->valid() is false, filter is always true
            if (this->dataError()) {
                return kvstore::ResultCode::ERR_INVALID_DATA;
            }
            if (this->valid() && !check()) {
                return kvstore::ResultCode::ERR_RESULT_FILTERED;
            } else {
                return kvstore::ResultCode::SUCCEEDED;
            }
        } else {
            while (this->valid() && !check()) {
                this->next();
            }
            return kvstore::ResultCode::SUCCEEDED;
        }
    }

private:
    // return true when the value iter points to a value which can filter
    bool check() override {
        if (filterExp_ != nullptr) {
            expCtx_->reset(this->reader(), this->key(), entryName_, schema_, isEdge_);
            // result is false when filter out
            auto result = filterExp_->eval(*expCtx_);
            // NULL is always false
            auto ret = result.toBool();
            if (ret.ok() && ret.value()) {
                return true;
            } else {
                return false;
            }
        }
        return true;
    }

private:
    PlanContext                      *planContext_;
    bool                              isEdge_;
    StorageExpressionContext         *expCtx_;
    Expression                       *filterExp_;
    bool                              isUpdate_;
    TagContext                       *tagContext_;
    EdgeContext                      *edgeContext_;
    const meta::NebulaSchemaProvider *schema_;
    std::string                       entryName_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_FILTERNODE_H_
