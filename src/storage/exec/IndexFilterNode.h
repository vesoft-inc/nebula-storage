/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef STORAGE_EXEC_INDEXFILTERNODE_H_
#define STORAGE_EXEC_INDEXFILTERNODE_H_

#include "common/base/Base.h"
#include "common/expression/Expression.h"
#include "common/context/ExpressionContext.h"
#include "storage/exec/RelNode.h"

namespace nebula {
namespace storage {

template<typename T>
class IndexFilterNode final : public RelNode<T> {
public:
    IndexFilterNode(IndexScanNode<T>* indexScanNode,
                    const std::string& filter)
        : indexScanNode_(indexScanNode) {
        expr_ = Expression::decode(filter);
        evalExprByIndex_ = true;
    }

    IndexFilterNode(IndexEdgeNode<T>* indexEdgeNode,
                    const std::string& filter)
        : indexEdgeNode_(indexEdgeNode) {
        expr_ = Expression::decode(filter);
        evalExprByIndex_ = false;
        isEdge_ = true;
    }

    IndexFilterNode(IndexVertexNode<T>* indexVertexNode,
                    const std::string& filter)
        : indexVertexNode_(indexVertexNode) {
        expr_ = Expression::decode(filter);
        evalExprByIndex_ = false;
        isEdge_ = false;
    }

    kvstore::ResultCode execute(PartitionID partId) override {
        auto ret = RelNode<T>::execute(partId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }
        std::vector<std::string> datas = {};
        if (evalExprByIndex_) {
            datas = indexScanNode_->getData();
        } else if (isEdge_) {
            datas = indexEdgeNode_->getData();
        } else {
            datas = indexVertexNode_->getData();
        }
        for (const auto& k : datas) {
            if (check(k)) {
                data_.emplace_back(k);
            }
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

    const std::vector<std::string>& getData() const {
        return std::move(data_);
    }

private:
    bool check(folly::StringPiece raw) {
        // TODO(sky) :  eval expression by index key or data
        UNUSED(raw);
        return false;
    }

private:
    IndexScanNode<T>*           indexScanNode_{nullptr};
    IndexEdgeNode<T>*           indexEdgeNode_{nullptr};
    IndexVertexNode<T>*         indexVertexNode_{nullptr};
    std::unique_ptr<Expression> expr_{nullptr};
    bool                        isEdge_{false};
    bool                        evalExprByIndex_{false};
    std::vector<std::string>    data_{};
};

}  // namespace storage
}  // namespace nebula

#endif   // STORAGE_EXEC_INDEXFILTERNODE_H_
