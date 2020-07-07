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

namespace nebula {
namespace storage {

// FilterNode will receive the result from upstream, check whether tag data or edge data
// could pass the expression filter
class FilterNode : public IterateNode<VertexID> {
public:
    FilterNode(PlanContext* planCtx,
               IterateNode* upstream,
               StorageExpressionContext* expCtx = nullptr,
               Expression* exp = nullptr)
        : IterateNode(upstream)
        , planContext_(planCtx)
        , expCtx_(expCtx)
        , exp_(exp) {
        UNUSED(planContext_);
    }

    kvstore::ResultCode execute(PartitionID partId, const VertexID& vId) override {
        auto ret = RelNode::execute(partId, vId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        while (upstream_->valid() && !check()) {
            upstream_->next();
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

private:
    // return true when the value iter points to a value which can filter
    bool check() override {
        if (exp_ != nullptr) {
            expCtx_->reset(upstream_->reader(), upstream_->key(), true);
            auto result = exp_->eval(*expCtx_);
            if (result.type() == Value::Type::BOOL) {
                return result.getBool();
            } else {
                return false;
            }
        }
        return true;
    }

private:
    PlanContext* planContext_;
    StorageExpressionContext* expCtx_;
    Expression* exp_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_FILTERNODE_H_
