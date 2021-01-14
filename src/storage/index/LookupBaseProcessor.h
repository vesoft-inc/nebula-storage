/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_QUERY_LOOKUPBASEPROCESSOR_H_
#define STORAGE_QUERY_LOOKUPBASEPROCESSOR_H_

#include "common/base/Base.h"
#include "storage/BaseProcessor.h"
#include "storage/exec/StoragePlan.h"
#include "storage/exec/AggregateNode.h"
#include "storage/exec/IndexScanNode.h"
#include "storage/exec/IndexVertexNode.h"
#include "storage/exec/IndexEdgeNode.h"
#include "storage/exec/IndexFilterNode.h"
#include "storage/exec/IndexOutputNode.h"
#include "storage/exec/DeDupNode.h"
#include "storage/exec/IntersectNode.h"

namespace nebula {
namespace storage {
using IndexFilterItem =
    std::unordered_map<int32_t, std::pair<std::unique_ptr<StorageExpressionContext>,
                                          std::unique_ptr<Expression>>>;

template<typename REQ, typename RESP>
class LookupBaseProcessor : public BaseProcessor<RESP> {
public:
    virtual ~LookupBaseProcessor() = default;

    virtual void process(const REQ& req) = 0;

protected:
    LookupBaseProcessor(StorageEnv* env,
                        stats::Stats* stats = nullptr,
                        VertexCache* cache = nullptr)
        : BaseProcessor<RESP>(env, stats)
        , vertexCache_(cache) {}

    virtual void onProcessFinished() = 0;

    cpp2::ErrorCode requestCheck(const cpp2::LookupIndexRequest& req);

    bool isOutsideIndex(Expression* filter, const meta::cpp2::IndexItem* index);

    StatusOr<StoragePlan<IndexID>> buildPlan();

    Status buildSubPlan(int32_t& filterId,
                        StoragePlan<IndexID>& plan,
                        nebula::DataSet* resultSet,
                        const cpp2::IndexQueryContext& ctx,
                        std::unique_ptr<IndexOutputNode<IndexID>>& out);

    StatusOr<StoragePlan<IndexID>> buildUnionPlan();

    StatusOr<StoragePlan<IndexID>> buildIntersectPlan();

    StatusOr<StoragePlan<IndexID>> buildExceptPlan();

    std::unique_ptr<IndexOutputNode<IndexID>>
    buildPlanBasic(const cpp2::IndexQueryContext& ctx,
                   StoragePlan<IndexID>& plan,
                   bool hasNullableCol,
                   const std::vector<meta::cpp2::ColumnDef>& fields,
                   nebula::DataSet* resultSet);

    std::unique_ptr<IndexOutputNode<IndexID>>
    buildPlanWithData(const cpp2::IndexQueryContext& ctx,
                      StoragePlan<IndexID>& plan,
                      nebula::DataSet* resultSet);

    std::unique_ptr<IndexOutputNode<IndexID>>
    buildPlanWithFilter(const cpp2::IndexQueryContext& ctx,
                        StoragePlan<IndexID>& plan,
                        StorageExpressionContext* exprCtx,
                        Expression* exp,
                        nebula::DataSet* resultSet);

    std::unique_ptr<IndexOutputNode<IndexID>>
    buildPlanWithDataAndFilter(const cpp2::IndexQueryContext& ctx,
                               StoragePlan<IndexID>& plan,
                               StorageExpressionContext* exprCtx,
                               Expression* exp,
                               nebula::DataSet* resultSet);

protected:
    GraphSpaceID                                spaceId_;
    std::unique_ptr<PlanContext>                planContext_;
    VertexCache*                                vertexCache_{nullptr};
    nebula::DataSet                             resultDataSet_;
    std::vector<cpp2::IndexQueryContext>        contexts_{};
    std::vector<cpp2::IndexReturnColumn>        yieldCols_{};
    IndexFilterItem                             filterItems_;
    // Save schema when column is out of index, need to read from data
    std::map<int32_t, std::shared_ptr<const meta::NebulaSchemaProvider>> schemas_;
    std::vector<size_t>                         deDupColPos_;
    cpp2::AggregateType                         aggrType_{cpp2::AggregateType::UNION};
    // used intersect set
    std::vector<nebula::DataSet>                resultDataSets_;
};
}  // namespace storage
}  // namespace nebula
#include "storage/index/LookupBaseProcessor.inl"
#endif  // STORAGE_QUERY_LOOKUPBASEPROCESSOR_H_
