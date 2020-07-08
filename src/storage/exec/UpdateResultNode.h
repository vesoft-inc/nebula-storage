/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_UPDATERESULTNODE_H_
#define STORAGE_EXEC_UPDATERESULTNODE_H_

#include "common/base/Base.h"
#include "storage/exec/UpdateNode.h"
#include "storage/context/StorageExpressionContext.h"

namespace nebula {
namespace storage {

class UpdateTagResNode : public RelNode<VertexID>  {
public:
    UpdateTagResNode(UpdateTagNode* updateTagNode,
                     std::vector<Expression*> returnPropsExp,
                     nebula::DataSet* result)
        : updateTagNode_(updateTagNode)
        , returnPropsExp_(returnPropsExp)
        , result_(result) {
        }

    kvstore::ResultCode execute(PartitionID partId, const VertexID& vId) override {
        auto ret = RelNode::execute(partId, vId);
        if (ret != kvstore::ResultCode::SUCCEEDED &&
            ret != kvstore::ResultCode::ERR_RESULT_FILTERED) {
            return ret;
        }

        insert_ = updateTagNode_->getInsert();
        expCtx_ = updateTagNode_->getExpressionContext();

        // Note: If filtered out, the result of tag prop is old
        result_->colNames.emplace_back("_inserted");
        std::vector<Value> row;
        row.emplace_back(insert_);

        for (auto& retExp : returnPropsExp_) {
            auto& val = retExp->eval(*expCtx_);
            auto sourceExp = dynamic_cast<const SourcePropertyExpression*>(retExp);
            if (sourceExp) {
                result_->colNames.emplace_back(folly::stringPrintf("%s:%s",
                                               sourceExp->sym()->c_str(),
                                               sourceExp->prop()->c_str()));
            } else {
                VLOG(1) << "Can't get expression name";
                result_->colNames.emplace_back("NULL");
            }
            row.emplace_back(std::move(val));
        }
        result_->rows.emplace_back(std::move(row));
        return ret;
    }

private:
    UpdateTagNode                                                                  *updateTagNode_;
    std::vector<Expression*>                                                        returnPropsExp_;
    StorageExpressionContext                                                       *expCtx_;

    // return prop sets
    nebula::DataSet                                                                *result_;
    bool                                                                            insert_{false};
};

class UpdateEdgeResNode : public RelNode<cpp2::EdgeKey>  {
public:
    UpdateEdgeResNode(UpdateEdgeNode* updateEdgeNode,
                     std::vector<Expression*> returnPropsExp,
                     nebula::DataSet* result)
        : updateEdgeNode_(updateEdgeNode)
        , returnPropsExp_(returnPropsExp)
        , result_(result) {
        }

    kvstore::ResultCode execute(PartitionID partId, const cpp2::EdgeKey& edgeKey) override {
        auto ret = RelNode::execute(partId, edgeKey);
        if (ret != kvstore::ResultCode::SUCCEEDED &&
            ret != kvstore::ResultCode::ERR_RESULT_FILTERED) {
            return ret;
        }

        insert_ = updateEdgeNode_->getInsert();
        expCtx_ = updateEdgeNode_->getExpressionContext();

        // Note: If filtered out, the result of edge prop is old
        result_->colNames.emplace_back("_inserted");
        std::vector<Value> row;
        row.emplace_back(insert_);

        for (auto& retExp : returnPropsExp_) {
            auto& val = retExp->eval(*expCtx_);
            auto edgeSrcIdExp = dynamic_cast<const EdgeSrcIdExpression*>(retExp);
            auto edgeDstIdExp = dynamic_cast<const EdgeDstIdExpression*>(retExp);
            auto edgeRankExp = dynamic_cast<const EdgeRankExpression*>(retExp);
            auto edgeTypeExp = dynamic_cast<const EdgeTypeExpression*>(retExp);
            auto edgePropExp = dynamic_cast<const EdgePropertyExpression*>(retExp);

            if (edgeSrcIdExp || edgeDstIdExp || edgeRankExp || edgeTypeExp || edgePropExp) {
                auto edgeExp = dynamic_cast<const SymbolPropertyExpression*>(retExp);
                result_->colNames.emplace_back(folly::stringPrintf("%s:%s",
                                               edgeExp->sym()->c_str(),
                                               edgeExp->prop()->c_str()));
            } else {
                VLOG(1) << "Can't get expression name";
                result_->colNames.emplace_back("NULL");
            }
            row.emplace_back(std::move(val));
        }
        result_->rows.emplace_back(std::move(row));
        return ret;
    }

private:
    UpdateEdgeNode                                                                 *updateEdgeNode_;
    std::vector<Expression*>                                                        returnPropsExp_;
    StorageExpressionContext                                                       *expCtx_;

    // return prop sets
    nebula::DataSet                                                                *result_;
    bool                                                                            insert_{false};
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_UPDATERESULTNODE_H_
