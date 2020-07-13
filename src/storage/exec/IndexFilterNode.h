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
                    const std::string& filter,
                    size_t vIdLen,
                    int32_t vColNum,
                    bool hasNullableCol,
                    bool isEdge,
                    const std::vector<std::pair<std::string, Value::Type>>& indexCols)
        : indexScanNode_(indexScanNode)
        , isEdge_(isEdge)
        , vColNum_(vColNum)
        , hasNullableCol_(hasNullableCol)
        , indexCols_(indexCols) {
        expr_ = Expression::decode(filter);
        expCtx_ = std::make_unique<StorageExpressionContext>(vIdLen,
                                                             vColNum,
                                                             hasNullableCol,
                                                             indexCols);
        evalExprByIndex_ = true;
    }

    IndexFilterNode(size_t vIdLen,
                    IndexEdgeNode<T>* indexEdgeNode,
                    const std::string& filter)
        : indexEdgeNode_(indexEdgeNode) {
        expr_ = Expression::decode(filter);
        expCtx_ = std::make_unique<StorageExpressionContext>(vIdLen);
        evalExprByIndex_ = false;
        isEdge_ = true;
    }

    IndexFilterNode(size_t vIdLen,
                    IndexVertexNode<T>* indexVertexNode,
                    const std::string& filter)
        : indexVertexNode_(indexVertexNode)  {
        expr_ = Expression::decode(filter);
        expCtx_ = std::make_unique<StorageExpressionContext>(vIdLen);
        evalExprByIndex_ = false;
        isEdge_ = false;
    }

    kvstore::ResultCode execute(PartitionID partId) override {
        data_.clear();
        auto ret = RelNode<T>::execute(partId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }
        std::vector<kvstore::KV> data;
        if (evalExprByIndex_) {
            data = indexScanNode_->getData();
        } else if (isEdge_) {
            data = indexEdgeNode_->getData();
        } else {
            data = indexVertexNode_->getData();
        }
        for (const auto& k : data) {
            if (evalExprByIndex_) {
                if (check(k.first)) {
                    data_.emplace_back(k.first, k.second);
                }
            } else {
                const auto* schema = isEdge_ ? indexEdgeNode_->getSchema()
                                             : indexVertexNode_->getSchema();
                const auto& schemaName = isEdge_ ? indexEdgeNode_->getSchemaName()
                                                 : indexVertexNode_->getSchemaName();
                auto reader = RowReader::getRowReader(schema, k.second);
                if (!reader) {
                    continue;
                }
                if (check(reader.get(), schemaName, k.first)) {
                    data_.emplace_back(k.first, k.second);
                }
            }
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

    const std::vector<kvstore::KV>& getData() const {
        return std::move(data_);
    }

    const meta::NebulaSchemaProvider* getSchema() {
        if (evalExprByIndex_) {
            return nullptr;
        }
        return isEdge_ ? indexEdgeNode_->getSchema()
                       : indexVertexNode_->getSchema();
    }

    const int32_t vColNum() const {
        return vColNum_;
    }

    const bool hasNullableCol() const {
        return hasNullableCol_;
    }

    const std::vector<std::pair<std::string, Value::Type>>& indexCols() const {
        return indexCols_;
    }

private:
    bool check(folly::StringPiece raw) {
        if (expr_ != nullptr) {
            expCtx_->reset(raw.str(), isEdge_);
            auto result = expr_->eval(*expCtx_);
            if (result.type() == Value::Type::BOOL) {
                return result.getBool();
            } else {
                return false;
            }
        }
        return false;
    }

    bool check(RowReader* reader, folly::StringPiece name, folly::StringPiece raw) {
        if (expr_ != nullptr) {
            expCtx_->reset(reader, raw.str(), name.str(), isEdge_);
            auto result = expr_->eval(*expCtx_);
            if (result.type() == Value::Type::BOOL) {
                return result.getBool();
            } else {
                return false;
            }
        }
        return false;
    }

private:
    IndexScanNode<T>*                                 indexScanNode_{nullptr};
    IndexEdgeNode<T>*                                 indexEdgeNode_{nullptr};
    IndexVertexNode<T>*                               indexVertexNode_{nullptr};
    std::unique_ptr<Expression>                       expr_{nullptr};
    std::unique_ptr<StorageExpressionContext>         expCtx_{nullptr};
    bool                                              isEdge_{false};
    bool                                              evalExprByIndex_{false};
    std::vector<kvstore::KV>                          data_{};
    size_t                                            vIdLen_{0};
    int32_t                                           vColNum_{0};
    bool                                              hasNullableCol_{false};
    std::vector<std::pair<std::string, Value::Type>>  indexCols_{};
};

}  // namespace storage
}  // namespace nebula

#endif   // STORAGE_EXEC_INDEXFILTERNODE_H_
