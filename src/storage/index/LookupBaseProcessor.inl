/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "LookupBaseProcessor.h"
namespace nebula {
namespace storage {

template<typename REQ, typename RESP>
cpp2::ErrorCode LookupBaseProcessor<REQ, RESP>::requestCheck(const cpp2::LookupIndexRequest& req) {
    spaceId_ = req.get_space_id();
    auto retCode = this->getSpaceVidLen(spaceId_);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        return retCode;
    }

    planContext_ = std::make_unique<PlanContext>(
        this->env_, spaceId_, this->spaceVidLen_, this->isIntId_);
    const auto& indices = req.get_indices();
    planContext_->isEdge_ = indices.get_is_edge();
    aggrType_ = indices.get_aggr_type();

    if (indices.get_contexts().empty() || !req.__isset.return_columns ||
        req.get_return_columns()->empty()) {
        return cpp2::ErrorCode::E_INVALID_OPERATION;
    }
    contexts_ = indices.get_contexts();
    for (const auto& ctx : contexts_) {
        auto schemaId = ctx.get_tag_or_edge_id();
        auto schema = planContext_->isEdge_
                      ? this->env_->schemaMan_->getEdgeSchema(spaceId_, schemaId)
                      : this->env_->schemaMan_->getTagSchema(spaceId_, schemaId);
        if (!schema) {
            LOG(ERROR) << "Space " << spaceId_ << ", schema " << schemaId << " invalid";
            return cpp2::ErrorCode::E_SPACE_NOT_FOUND;
        }
        schemas_.emplace(schemaId, schema);

        if (aggrType_ == cpp2::AggregateType::INTERSECT) {
            resultDataSets_.emplace_back(nebula::DataSet());
        }
    }

    for (const auto& col : *req.get_return_columns()) {
        yieldCols_.emplace_back(col);
    }

    // TODO (sky) : just support return columns(kVid, kTag, kSrc, kType, kRank, kDst)
    //              when aggr type is AggregateType::INTERSECT
    if (aggrType_ == cpp2::AggregateType::INTERSECT) {
        for (const auto& col : yieldCols_) {
            if (QueryUtils::toReturnColType(col.get_prop())
                == QueryUtils::ReturnColType::kOther) {
                    // It should be checked in QE layer
                    return cpp2::ErrorCode::E_UNKNOWN;
                }
        }
    }
    for (size_t i = 0; i < yieldCols_.size(); i++) {
        auto id = yieldCols_[i].get_tag_or_edge_id();
        auto schemaName = planContext_->isEdge_
                          ? this->env_->schemaMan_->toEdgeName(spaceId_, id)
                          : this->env_->schemaMan_->toTagName(spaceId_, id);
        if (!schemaName.ok()) {
            LOG(ERROR) << "Space " << spaceId_ << ", schema " << id << " invalid";
            return cpp2::ErrorCode::E_SPACE_NOT_FOUND;
        }
        resultDataSet_.colNames.emplace_back(schemaName.value() + "." + yieldCols_[i].get_prop());
        if (QueryUtils::toReturnColType(yieldCols_[i].get_prop())
            != QueryUtils::ReturnColType::kOther) {
            deDupColPos_.emplace_back(i);
        }
    }

    return cpp2::ErrorCode::SUCCEEDED;
}

template<typename REQ, typename RESP>
bool LookupBaseProcessor<REQ, RESP>::isOutsideIndex(Expression* filter,
                                                    const meta::cpp2::IndexItem* index) {
    static const std::set<std::string> propsInEdgeKey{kSrc, kType, kRank, kDst};
    auto fields = index->get_fields();
    switch (filter->kind()) {
        case Expression::Kind::kLogicalOr :
        case Expression::Kind::kLogicalAnd : {
            auto *lExpr = static_cast<LogicalExpression*>(filter);
            for (auto &expr : lExpr->operands()) {
                auto ret = isOutsideIndex(expr.get(), index);
                if (ret) {
                    return ret;
                }
            }
            break;
        }
        case Expression::Kind::kRelLE :
        case Expression::Kind::kRelIn :
        case Expression::Kind::kRelGE :
        case Expression::Kind::kRelEQ :
        case Expression::Kind::kRelLT :
        case Expression::Kind::kRelGT :
        case Expression::Kind::kRelNE :
        case Expression::Kind::kRelNotIn : {
            auto* rExpr = static_cast<RelationalExpression*>(filter);
            auto ret = isOutsideIndex(rExpr->left(), index);
            if (ret) {
                return ret;
            }
            ret = isOutsideIndex(rExpr->right(), index);
            if (ret) {
                return ret;
            }
            break;
        }
        case Expression::Kind::kEdgeSrc:
        case Expression::Kind::kEdgeType:
        case Expression::Kind::kEdgeRank:
        case Expression::Kind::kEdgeDst: {
            auto* sExpr = static_cast<PropertyExpression*>(filter);
            auto propName = *(sExpr->prop());
            return propsInEdgeKey.find(propName) == propsInEdgeKey.end();
        }
        case Expression::Kind::kTagProperty:
        case Expression::Kind::kEdgeProperty: {
            auto* sExpr = static_cast<PropertyExpression*>(filter);
            auto propName = *(sExpr->prop());
            auto it = std::find_if(fields.begin(), fields.end(), [&propName] (const auto& f) {
                return f.get_name() == propName;
            });
            return it == fields.end();
        }
        default: {
            return false;
        }
    }
    return false;
}

template<typename REQ, typename RESP>
StatusOr<StoragePlan<IndexID>> LookupBaseProcessor<REQ, RESP>::buildPlan() {
    switch (aggrType_) {
        case cpp2::AggregateType::UNION:
            return buildUnionPlan();
        case cpp2::AggregateType::INTERSECT:
            return buildIntersectPlan();
        case cpp2::AggregateType::EXCEPT:
            return buildExceptPlan();
    }
    // Only used for compilation error
    return Status::Error();
}

template<typename REQ, typename RESP>
Status
LookupBaseProcessor<REQ, RESP>::buildSubPlan(int32_t& filterId,
                                             StoragePlan<IndexID>& plan,
                                             nebula::DataSet* resultSet,
                                             const cpp2::IndexQueryContext& ctx,
                                             std::unique_ptr<IndexOutputNode<IndexID>>& out) {
    const auto& indexId = ctx.get_index_id();
    auto needFilter = ctx.__isset.filter && !ctx.get_filter().empty();
    const auto& schemaId = ctx.get_tag_or_edge_id();

    // Check whether a data node is required.
    // If a non-indexed column appears in the WHERE clause or YIELD clause,
    // That means need to query the corresponding data.
    bool needData = false;
    auto index = planContext_->isEdge_
        ? this->env_->indexMan_->getEdgeIndex(spaceId_, indexId)
        : this->env_->indexMan_->getTagIndex(spaceId_, indexId);
    if (!index.ok()) {
        return Status::IndexNotFound();
    }

    // check nullable column
    bool hasNullableCol = false;

    auto* indexItem = index.value().get();
    auto fields = indexItem->get_fields();

    for (const auto& col : fields) {
        if (!hasNullableCol && col.get_nullable()) {
            hasNullableCol = true;
            break;
        }
    }

    for (const auto& yieldCol : yieldCols_) {
        if (yieldCol.get_tag_or_edge_id() != schemaId) {
            continue;
        }
        static const std::set<std::string> propsInKey{kVid, kTag, kSrc, kType, kRank, kDst};
        if (propsInKey.count(yieldCol.get_prop())) {
            continue;
        }
        auto it = std::find_if(fields.begin(),
                                fields.end(),
                                [&yieldCol] (const auto& columnDef) {
                                    return yieldCol.get_prop() == columnDef.get_name();
                                });
        if (it == fields.end()) {
            needData = true;
            break;
        }
    }
    auto colHints = ctx.get_column_hints();

    // Check WHERE clause contains columns that ware not indexed
    if (ctx.__isset.filter && !ctx.get_filter().empty()) {
        auto filter = Expression::decode(ctx.get_filter());
        auto isFieldsOutsideIndex = isOutsideIndex(filter.get(), indexItem);
        if (isFieldsOutsideIndex) {
            needData = needFilter = true;
        }
    }

    if (!needData && !needFilter) {
        out = buildPlanBasic(ctx, plan, hasNullableCol, fields, resultSet);
    } else if (needData && !needFilter) {
        out = buildPlanWithData(ctx, plan, resultSet);
    } else if (!needData && needFilter) {
        auto expr = Expression::decode(ctx.get_filter());
        auto exprCtx = std::make_unique<StorageExpressionContext>(planContext_->vIdLen_,
                                                                  planContext_->isIntId_,
                                                                  hasNullableCol,
                                                                  fields);
        filterItems_.emplace(filterId, std::make_pair(std::move(exprCtx), std::move(expr)));
        out = buildPlanWithFilter(ctx,
                                  plan,
                                  filterItems_[filterId].first.get(),
                                  filterItems_[filterId].second.get(),
                                  resultSet);
        filterId++;
    } else {
        auto expr = Expression::decode(ctx.get_filter());
        if (schemas_.find(schemaId) == schemas_.end()) {
            return Status::Error("Schema not found");
        }
        // Need to get columns in data, expr ctx need to be aware of schema
        auto schemaName = planContext_->isEdge_
                        ? this->env_->schemaMan_->toEdgeName(spaceId_, schemaId)
                        : this->env_->schemaMan_->toTagName(spaceId_, schemaId);
        if (!schemaName.ok()) {
            return Status::Error("get schema name error");
        }
        auto exprCtx = std::make_unique<StorageExpressionContext>(planContext_->vIdLen_,
                                                                  planContext_->isIntId_,
                                                                  std::move(schemaName).value(),
                                                                  schemas_[schemaId].get(),
                                                                  planContext_->isEdge_);
        filterItems_.emplace(filterId, std::make_pair(std::move(exprCtx), std::move(expr)));
        out = buildPlanWithDataAndFilter(ctx,
                                         plan,
                                         filterItems_[filterId].first.get(),
                                         filterItems_[filterId].second.get(),
                                         resultSet);
        filterId++;
    }
    if (out == nullptr) {
        return Status::Error("Index scan plan error");
    }
    return Status::OK();
}

/**
 * lookup plan should be :
 *              +--------+---------+
 *              |       Plan       |
 *              +--------+---------+
 *                       |
 *              +--------+---------+
 *              |  AggregateNode   |
 *              +--------+---------+
 *                       |
 *              +--------+---------+
 *              |    DeDupNode     |
 *              +--------+---------+
 *                       |
 *            +----------+-----------+
 *            +  IndexOutputNode...  +
 *            +----------+-----------+
**/
template<typename REQ, typename RESP>
StatusOr<StoragePlan<IndexID>> LookupBaseProcessor<REQ, RESP>::buildUnionPlan() {
    StoragePlan<IndexID> plan;
    auto IndexAggr = std::make_unique<AggregateNode<IndexID>>(&resultDataSet_);
    auto deDup = std::make_unique<DeDupNode<IndexID>>(&resultDataSet_, deDupColPos_);
    int32_t filterId = 0;
    std::unique_ptr<IndexOutputNode<IndexID>> out;
    for (const auto& ctx : contexts_) {
        if (!buildSubPlan(filterId, plan, &resultDataSet_, ctx, out).ok()) {
            LOG(ERROR) << "build union plan error";
            return Status::Error("build union plan error");
        }
        deDup->addDependency(out.get());
        plan.addNode(std::move(out));
    }
    IndexAggr->addDependency(deDup.get());
    plan.addNode(std::move(deDup));
    plan.addNode(std::move(IndexAggr));
    return plan;
}

template<typename REQ, typename RESP>
StatusOr<StoragePlan<IndexID>> LookupBaseProcessor<REQ, RESP>::buildIntersectPlan() {
    StoragePlan<IndexID> plan;
    auto IndexAggr = std::make_unique<AggregateNode<IndexID>>(&resultDataSet_);
    auto interNode = std::make_unique<IntersectNode<IndexID>>(&resultDataSet_,
                                                              &resultDataSets_,
                                                              deDupColPos_);
    int32_t filterId = 0;
    std::unique_ptr<IndexOutputNode<IndexID>> out;
    size_t i = 0;
    for (const auto& ctx : contexts_) {
        if (!buildSubPlan(filterId, plan, &(resultDataSets_[i++]), ctx, out).ok()) {
            LOG(ERROR) << "build intersect plan error";
            return Status::Error("build intersect plan error");
        }
        interNode->addDependency(out.get());
        plan.addNode(std::move(out));
    }
    IndexAggr->addDependency(interNode.get());
    plan.addNode(std::move(interNode));
    plan.addNode(std::move(IndexAggr));
    return plan;
}

template<typename REQ, typename RESP>
StatusOr<StoragePlan<IndexID>> LookupBaseProcessor<REQ, RESP>::buildExceptPlan() {
    return Status::NotSupported();
}

/**
 *
 *            +----------+-----------+
 *            +   IndexOutputNode    +
 *            +----------+-----------+
 *                       |
 *            +----------+-----------+
 *            +    IndexScanNode     +
 *            +----------+-----------+
 *
 * If this is a simple index scan, Just having IndexScanNode is enough. for example :
 * tag (c1, c2, c3)
 * index on tag (c1, c2, c3)
 * hint : lookup index where c1 == 1 and c2 == 1 and c3 == 1 yield c1,c2,c3
 **/
template<typename REQ, typename RESP>
std::unique_ptr<IndexOutputNode<IndexID>>
LookupBaseProcessor<REQ, RESP>::buildPlanBasic(
    const cpp2::IndexQueryContext& ctx,
    StoragePlan<IndexID>& plan,
    bool hasNullableCol,
    const std::vector<meta::cpp2::ColumnDef>& fields,
    nebula::DataSet* resultSet) {
    auto indexId = ctx.get_index_id();
    auto colHints = ctx.get_column_hints();
    auto schemaId = ctx.get_tag_or_edge_id();
    auto indexScan = std::make_unique<IndexScanNode<IndexID>>(planContext_.get(),
                                                              indexId,
                                                              std::move(colHints));

    auto output = std::make_unique<IndexOutputNode<IndexID>>(resultSet,
                                                             yieldCols_,
                                                             planContext_.get(),
                                                             indexScan.get(),
                                                             hasNullableCol,
                                                             fields,
                                                             schemaId);
    output->addDependency(indexScan.get());
    plan.addNode(std::move(indexScan));
    return output;
}

/**
 *
 *            +----------+-----------+
 *            +   IndexOutputNode    +
 *            +----------+-----------+
 *                       |
 *      +----------------+-----------------+
 *      + IndexEdgeNode or IndexVertexNode +
 *      +----------------+-----------------+
 *                       |
 *            +----------+-----------+
 *            +    IndexScanNode     +
 *            +----------+-----------+
 *
 * If a non-indexed column appears in the YIELD clause, and no expression filtering is required .
 * for example :
 * tag (c1, c2, c3)
 * index on tag (c1, c2)
 * hint : lookup index where c1 == 1 and c2 == 1 yield c3
 **/
template<typename REQ, typename RESP>
std::unique_ptr<IndexOutputNode<IndexID>>
LookupBaseProcessor<REQ, RESP>::buildPlanWithData(const cpp2::IndexQueryContext& ctx,
                                                  StoragePlan<IndexID>& plan,
                                                  nebula::DataSet* resultSet) {
    auto indexId = ctx.get_index_id();
    auto colHints = ctx.get_column_hints();
    auto schemaId = ctx.get_tag_or_edge_id();

    auto indexScan = std::make_unique<IndexScanNode<IndexID>>(planContext_.get(),
                                                              indexId,
                                                              std::move(colHints));
    if (planContext_->isEdge_) {
        auto edge = std::make_unique<IndexEdgeNode<IndexID>>(planContext_.get(),
                                                             indexScan.get(),
                                                             schemas_[schemaId],
                                                             planContext_->edgeName_,
                                                             schemaId);
        edge->addDependency(indexScan.get());
        auto output = std::make_unique<IndexOutputNode<IndexID>>(resultSet,
                                                                 yieldCols_,
                                                                 planContext_.get(),
                                                                 edge.get());
        output->addDependency(edge.get());
        plan.addNode(std::move(indexScan));
        plan.addNode(std::move(edge));
        return output;
    } else {
        auto vertex = std::make_unique<IndexVertexNode<IndexID>>(planContext_.get(),
                                                                 this->vertexCache_,
                                                                 indexScan.get(),
                                                                 schemas_[schemaId],
                                                                 planContext_->tagName_,
                                                                 schemaId);
        vertex->addDependency(indexScan.get());
        auto output = std::make_unique<IndexOutputNode<IndexID>>(resultSet,
                                                                 yieldCols_,
                                                                 planContext_.get(),
                                                                 vertex.get());
        output->addDependency(vertex.get());
        plan.addNode(std::move(indexScan));
        plan.addNode(std::move(vertex));
        return output;
    }
}

/**
 *
 *            +----------+-----------+
 *            +   IndexOutputNode    +
 *            +----------+-----------+
 *                       |
 *            +----------+-----------+
 *            +  IndexFilterNode     +
 *            +----------+-----------+
 *                       |
 *            +----------+-----------+
 *            +    IndexScanNode     +
 *            +----------+-----------+
 *
 * If have not non-indexed column appears in the YIELD clause, and expression filtering is required .
 * for example :
 * tag (c1, c2, c3)
 * index on tag (c1, c2)
 * hint : lookup index where c1 > 1 and c2 > 1
 **/
template<typename REQ, typename RESP>
std::unique_ptr<IndexOutputNode<IndexID>>
LookupBaseProcessor<REQ, RESP>::buildPlanWithFilter(const cpp2::IndexQueryContext& ctx,
                                                    StoragePlan<IndexID>& plan,
                                                    StorageExpressionContext* exprCtx,
                                                    Expression* exp,
                                                    nebula::DataSet* resultSet) {
    auto indexId = ctx.get_index_id();
    auto colHints = ctx.get_column_hints();
    auto schemaId = ctx.get_tag_or_edge_id();

    auto indexScan = std::make_unique<IndexScanNode<IndexID>>(planContext_.get(),
                                                              indexId,
                                                              std::move(colHints));

    auto filter = std::make_unique<IndexFilterNode<IndexID>>(indexScan.get(),
                                                             exprCtx,
                                                             exp,
                                                             planContext_->isEdge_);
    filter->addDependency(indexScan.get());
    auto output = std::make_unique<IndexOutputNode<IndexID>>(resultSet,
                                                             yieldCols_,
                                                             planContext_.get(),
                                                             filter.get(),
                                                             schemaId,
                                                             true);
    output->addDependency(filter.get());
    plan.addNode(std::move(indexScan));
    plan.addNode(std::move(filter));
    return output;
}


/**
 *
 *            +----------+-----------+
 *            +   IndexOutputNode    +
 *            +----------+-----------+
 *                       |
 *            +----------+-----------+
 *            +   IndexFilterNode    +
 *            +----------+-----------+
 *                       |
 *      +----------------+-----------------+
 *      + IndexEdgeNode or IndexVertexNode +
 *      +----------------+-----------------+
 *                       |
 *            +----------+-----------+
 *            +    IndexScanNode     +
 *            +----------+-----------+
 *
 * If a non-indexed column appears in the WHERE clause or YIELD clause,
 * and expression filtering is required .
 * for example :
 * tag (c1, c2, c3)
 * index on tag (c1, c2)
 * hint : lookup index where c1 == 1 and c2 == 1 and c3 > 1 yield c3
 *        lookup index where c1 == 1 and c2 == 1 and c3 > 1
 *        lookup index where c1 == 1 and c3 == 1
 **/
template<typename REQ, typename RESP>
std::unique_ptr<IndexOutputNode<IndexID>>
LookupBaseProcessor<REQ, RESP>::buildPlanWithDataAndFilter(const cpp2::IndexQueryContext& ctx,
                                                           StoragePlan<IndexID>& plan,
                                                           StorageExpressionContext* exprCtx,
                                                           Expression* exp,
                                                           nebula::DataSet* resultSet) {
    auto indexId = ctx.get_index_id();
    auto colHints = ctx.get_column_hints();
    auto schemaId = ctx.get_tag_or_edge_id();

    auto indexScan = std::make_unique<IndexScanNode<IndexID>>(planContext_.get(),
                                                              indexId,
                                                              std::move(colHints));
    if (planContext_->isEdge_) {
        auto edge = std::make_unique<IndexEdgeNode<IndexID>>(planContext_.get(),
                                                             indexScan.get(),
                                                             schemas_[schemaId],
                                                             planContext_->edgeName_,
                                                             schemaId);
        edge->addDependency(indexScan.get());
        auto filter = std::make_unique<IndexFilterNode<IndexID>>(edge.get(),
                                                                 exprCtx,
                                                                 exp);
        filter->addDependency(edge.get());

        auto output = std::make_unique<IndexOutputNode<IndexID>>(resultSet,
                                                                 yieldCols_,
                                                                 planContext_.get(),
                                                                 filter.get(),
                                                                 schemaId);
        output->addDependency(filter.get());
        plan.addNode(std::move(indexScan));
        plan.addNode(std::move(edge));
        plan.addNode(std::move(filter));
        return output;
    } else {
        auto vertex = std::make_unique<IndexVertexNode<IndexID>>(planContext_.get(),
                                                                 this->vertexCache_,
                                                                 indexScan.get(),
                                                                 schemas_[schemaId],
                                                                 planContext_->tagName_,
                                                                 schemaId);
        vertex->addDependency(indexScan.get());
        auto filter = std::make_unique<IndexFilterNode<IndexID>>(vertex.get(),
                                                                 exprCtx,
                                                                 exp);
        filter->addDependency(vertex.get());

        auto output = std::make_unique<IndexOutputNode<IndexID>>(&resultDataSet_,
                                                                 yieldCols_,
                                                                 planContext_.get(),
                                                                 filter.get(),
                                                                 schemaId);
        output->addDependency(filter.get());
        plan.addNode(std::move(indexScan));
        plan.addNode(std::move(vertex));
        plan.addNode(std::move(filter));
        return output;
    }
}

}  // namespace storage
}  // namespace nebula
