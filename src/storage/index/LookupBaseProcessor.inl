/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

namespace nebula {
namespace storage {

template<typename REQ, typename RESP>
cpp2::ErrorCode LookupBaseProcessor<REQ, RESP>::requestCheck(const cpp2::LookupIndexRequest& req) {
    spaceId_ = req.get_space_id();
    auto retCode = this->getSpaceVidLen(spaceId_);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        return retCode;
    }

    if (req.get_contexts().empty()) {
        return cpp2::ErrorCode::E_INVALID_OPERATION;
    }
    contexts_ = req.get_contexts();

    isEdge_ = req.get_is_edge();

    tagOrEdgeId_ = req.get_tag_or_edge_id();

    // setup yield columns.
    if (req.__isset.return_columns) {
        const auto& retcols = *req.get_return_columns();
        yieldCols_ = retcols;
    }

    // setup result set columns.
    if (isEdge_) {
        resultDataSet_.colNames.emplace_back("_src");
        // for nebula 1.0, the column _type don't need to return.
        // Because the index can identify which edgeType.
        // TODO (sky) : The above contents need to be confirmed
        resultDataSet_.colNames.emplace_back("_type");
        resultDataSet_.colNames.emplace_back("_ranking");
        resultDataSet_.colNames.emplace_back("_dst");
    } else {
        resultDataSet_.colNames.emplace_back("_vid");
    }

    for (const auto& col : yieldCols_) {
        resultDataSet_.colNames.emplace_back(col);
    }

    return cpp2::ErrorCode::SUCCEEDED;
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
 *            +----------+-----------+
 *            +  IndexOutputNode...  +
 *            +----------+-----------+
**/

template<typename REQ, typename RESP>
StatusOr<StoragePlan<IndexID>> LookupBaseProcessor<REQ, RESP>::buildPlan() {
    StoragePlan<IndexID> plan;
    auto aggrNode = std::make_unique<AggregateNode<IndexID>>(&resultDataSet_);
    for (const auto& ctx : contexts_) {
        const auto& indexId = ctx.get_index_id();
        auto needFilter = ctx.__isset.filter && !ctx.get_filter().empty();

        // Check whether a data node is required.
        // If a non-indexed column appears in the WHERE clause or YIELD clause,
        // That means need to query the corresponding data.
        bool needData = false;
        auto index = isEdge_ ? this->env_->indexMan_->getEdgeIndex(spaceId_, indexId)
                             : this->env_->indexMan_->getTagIndex(spaceId_, indexId);
        if (!index.ok()) {
            return Status::IndexNotFound();
        }

        // check nullable column
        bool hasNullableCol = false;
        int32_t vColNum = 0;
        std::vector<std::pair<std::string, Value::Type>> indexCols;
        for (const auto& col : index.value()->get_fields()) {
            indexCols.emplace_back(col.get_name(), IndexKeyUtils::toValueType(col.get_type()));
            if (IndexKeyUtils::toValueType(col.get_type()) == Value::Type::STRING) {
                vColNum++;
            }
            if (!hasNullableCol && col.get_nullable()) {
                hasNullableCol = true;
            }
        }

        for (const auto& yieldCol : yieldCols_) {
            auto it = std::find_if(index.value()->get_fields().begin(),
                                   index.value()->get_fields().end(),
                                   [&yieldCol] (const auto& columnDef) {
                                       return yieldCol == columnDef.get_name();
                                   });
            if (it == index.value()->get_fields().end()) {
                needData = true;
                break;
            }
        }
        auto colHints = ctx.get_column_hints();

        // TODO (sky) ： Check WHERE clause contains columns that ware not indexed

        std::unique_ptr<IndexOutputNode<IndexID>> out;
        if (!needData && !needFilter) {
            out = buildPlanBasic(ctx, plan, indexCols, vColNum, hasNullableCol);
        } else if (needData && !needFilter) {
            out = buildPlanWithData(ctx, plan);
        } else if (!needData && needFilter) {
            out = buildPlanWithFilter(ctx, plan);
        } else {
            out = buildPlanWithDataAndFilter(ctx, plan);
        }
        aggrNode->addDependency(out.get());
        plan.addNode(std::move(out));
    }
    plan.addNode(std::move(aggrNode));
    return plan;
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
    std::vector<std::pair<std::string, Value::Type>>& cols,
    int32_t vColNum,
    bool hasNullableCol) {
    auto indexId = ctx.get_index_id();
    auto colHints = ctx.get_column_hints();
    auto indexScan = std::make_unique<IndexScanNode<IndexID>>(this->env_,
                                                              this->spaceId_,
                                                              indexId,
                                                              this->spaceVidLen_,
                                                              this->isEdge_,
                                                              true,
                                                              std::move(colHints));

    auto output = std::make_unique<IndexOutputNode<IndexID>>(&resultDataSet_,
                                                             this->spaceVidLen_,
                                                             indexScan.get(),
                                                             cols,
                                                             vColNum,
                                                             hasNullableCol,
                                                             this->isEdge_);
    output->addDependency(indexScan.get());
    plan.addNode(std::move(indexScan));
    return std::move(output);
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
                                                  StoragePlan<IndexID>& plan) {
    auto indexId = ctx.get_index_id();
    auto colHints = ctx.get_column_hints();
    auto schema = this->isEdge_
                  ? this->env_->schemaMan_->getEdgeSchema(this->spaceId_, this->tagOrEdgeId_)
                  : this->env_->schemaMan_->getTagSchema(this->spaceId_, this->tagOrEdgeId_);

    auto indexScan = std::make_unique<IndexScanNode<IndexID>>(this->env_,
                                                              this->spaceId_,
                                                              indexId,
                                                              this->spaceVidLen_,
                                                              this->isEdge_,
                                                              true,
                                                              std::move(colHints));
    if (this->isEdge_) {
        auto edge = std::make_unique<IndexEdgeNode<IndexID>>(this->env_,
                                                             this->spaceId_,
                                                             this->spaceVidLen_,
                                                             this->tagOrEdgeId_,
                                                             indexScan.get());
        edge->addDependency(indexScan.get());
        auto output = std::make_unique<IndexOutputNode<IndexID>>(&resultDataSet_,
                                                                 this->spaceVidLen_,
                                                                 edge.get(),
                                                                 std::move(schema));
        output->addDependency(edge.get());
        plan.addNode(std::move(indexScan));
        plan.addNode(std::move(edge));
        return std::move(output);
    } else {
        auto vertex = std::make_unique<IndexVertexNode<IndexID>>(this->env_,
                                                                 this->spaceId_,
                                                                 this->spaceVidLen_,
                                                                 this->tagOrEdgeId_,
                                                                 this->vertexCache_,
                                                                 indexScan.get());
        vertex->addDependency(indexScan.get());
        auto output = std::make_unique<IndexOutputNode<IndexID>>(&resultDataSet_,
                                                                 this->spaceVidLen_,
                                                                 vertex.get(),
                                                                 std::move(schema));
        output->addDependency(vertex.get());
        plan.addNode(std::move(indexScan));
        plan.addNode(std::move(vertex));
        return std::move(output);
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
                                                    StoragePlan<IndexID>& plan) {
    auto indexId = ctx.get_index_id();
    auto colHints = ctx.get_column_hints();

    auto indexScan = std::make_unique<IndexScanNode<IndexID>>(this->env_,
                                                              this->spaceId_,
                                                              indexId,
                                                              this->spaceVidLen_,
                                                              this->isEdge_,
                                                              true,
                                                              std::move(colHints));

    auto filter = std::make_unique<IndexFilterNode<IndexID>>(indexScan.get(), ctx.get_filter());
    filter->addDependency(indexScan.get());
    auto output = std::make_unique<IndexOutputNode<IndexID>>(&resultDataSet_,
                                                             this->spaceVidLen_,
                                                             filter.get(),
                                                             this->isEdge_);
    output->addDependency(filter.get());
    plan.addNode(std::move(indexScan));
    plan.addNode(std::move(filter));
    return std::move(output);
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
                                                           StoragePlan<IndexID>& plan) {
    auto indexId = ctx.get_index_id();
    auto colHints = ctx.get_column_hints();
    auto schema = this->isEdge_
                  ? this->env_->schemaMan_->getEdgeSchema(this->spaceId_, this->tagOrEdgeId_)
                  : this->env_->schemaMan_->getTagSchema(this->spaceId_, this->tagOrEdgeId_);

    auto indexScan = std::make_unique<IndexScanNode<IndexID>>(this->env_,
                                                              this->spaceId_,
                                                              indexId,
                                                              this->spaceVidLen_,
                                                              this->isEdge_,
                                                              true,
                                                              std::move(colHints));
    if (this->isEdge_) {
        auto edge = std::make_unique<IndexEdgeNode<IndexID>>(this->env_,
                                                             this->spaceId_,
                                                             this->spaceVidLen_,
                                                             this->tagOrEdgeId_,
                                                             indexScan.get());
        edge->addDependency(indexScan.get());
        auto filter = std::make_unique<IndexFilterNode<IndexID>>(edge.get(), ctx.get_filter());
        filter->addDependency(edge.get());

        auto output = std::make_unique<IndexOutputNode<IndexID>>(&resultDataSet_,
                                                                 this->spaceVidLen_,
                                                                 filter.get(),
                                                                 std::move(schema),
                                                                 this->isEdge_);
        output->addDependency(filter.get());
        plan.addNode(std::move(indexScan));
        plan.addNode(std::move(edge));
        plan.addNode(std::move(filter));
        return std::move(output);
    } else {
        auto vertex = std::make_unique<IndexVertexNode<IndexID>>(this->env_,
                                                                 this->spaceId_,
                                                                 this->spaceVidLen_,
                                                                 this->tagOrEdgeId_,
                                                                 this->vertexCache_,
                                                                 indexScan.get());
        vertex->addDependency(indexScan.get());
        auto filter = std::make_unique<IndexFilterNode<IndexID>>(vertex.get(),
                                                                 ctx.get_filter());
        filter->addDependency(vertex.get());

        auto output = std::make_unique<IndexOutputNode<IndexID>>(&resultDataSet_,
                                                                 this->spaceVidLen_,
                                                                 filter.get(),
                                                                 std::move(schema),
                                                                 this->isEdge_);
        output->addDependency(filter.get());
        plan.addNode(std::move(indexScan));
        plan.addNode(std::move(vertex));
        plan.addNode(std::move(filter));
        return std::move(output);
    }
}

}  // namespace storage
}  // namespace nebula
