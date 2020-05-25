/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_UPDATETAGRESNODE_H_
#define STORAGE_EXEC_UPDATETAGRESNODE_H_

#include "common/base/Base.h"
#include "storage/exec/UpdateNode.h"
#include "storage/context/UpdateExpressionContext.h"

namespace nebula {
namespace storage {

class UpdateTagResNode : public RelNode<VertexID>  {
public:
    UpdateTagResNode(StorageEnv* env,
                     GraphSpaceID spaceId,
                     UpdateTagNode* updateTagNode,
                     std::vector<Expression*> returnPropsExp,
                     nebula::DataSet* result)
        : env_(env)
        , spaceId_(spaceId)
        , updateTagNode_(updateTagNode)
        , returnPropsExp_(returnPropsExp)
        , result_(result) {
        }

    kvstore::ResultCode execute(PartitionID partId, const VertexID& vId) override {
        auto ret = RelNode::execute(partId, vId);
        if (ret == kvstore::ResultCode::SUCCEEDED ||
            ret == kvstore::ResultCode::ERR_RESULT_FILTERED) {
            filter_ = updateTagNode_->getFilterCont();
            insert_ = updateTagNode_->getInsert();
            expCtx_ = updateTagNode_->getExpressionContext();
        }

        if (ret != kvstore::ResultCode::SUCCEEDED) {
            // Note: If filtered out, the result of every tag prop is NULl
            if (ret == kvstore::ResultCode::ERR_RESULT_FILTERED) {
                result_->colNames.emplace_back("_inserted");
                nebula::Row row;
                row.columns.emplace_back(false);

                for (auto& retExp : returnPropsExp_) {
                    auto sourceExp = dynamic_cast<const SourcePropertyExpression*>(retExp);
                    if (sourceExp) {
                        result_->colNames.emplace_back(folly::stringPrintf("%s:%s",
                                                       sourceExp->sym()->c_str(),
                                                       sourceExp->prop()->c_str()));
                    } else {
                        VLOG(1) << "Can't get expression name";
                        result_->colNames.emplace_back("NULL");
                    }
                    row.columns.emplace_back(NullType::__NULL__);
                }
                result_->rows.emplace_back(std::move(row));
            }
            return ret;
        }

        result_->colNames.emplace_back("_inserted");
        nebula::Row row;
        row.columns.emplace_back(insert_);
        Value val;

        for (auto& retExp : returnPropsExp_) {
            val = retExp->eval(*expCtx_);
            auto sourceExp = dynamic_cast<const SourcePropertyExpression*>(retExp);
            if (sourceExp) {
                result_->colNames.emplace_back(folly::stringPrintf("%s:%s",
                                               sourceExp->sym()->c_str(),
                                               sourceExp->prop()->c_str()));
            } else {
                VLOG(1) << "Can't get expression name";
                result_->colNames.emplace_back("NULL");
            }
            row.columns.emplace_back(std::move(val));
        }
        result_->rows.emplace_back(std::move(row));
        return kvstore::ResultCode::SUCCEEDED;
    }

private:
    // =================================== input =======================================
    StorageEnv                                                                     *env_;
    GraphSpaceID                                                                    spaceId_;
    UpdateTagNode                                                                  *updateTagNode_;
    std::vector<Expression*>                                                        returnPropsExp_;
    UpdateExpressionContext                                                        *expCtx_;

    // ================================== output =========================================
    // return prop sets
    nebula::DataSet                                                                *result_;
    FilterContext                                                                  *filter_;
    bool                                                                            insert_{false};
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_UPDATETAGRESNODE_H_
