/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/query/GetNeighborsProcessor.h"
#include "storage/StorageFlags.h"
#include "storage/exec/TagNode.h"
#include "storage/exec/EdgeNode.h"
#include "storage/exec/HashJoinNode.h"
#include "storage/exec/FilterNode.h"
#include "storage/exec/AggregateNode.h"
#include "storage/exec/GetNeighborsNode.h"

namespace nebula {
namespace storage {

void GetNeighborsProcessor::process(const cpp2::GetNeighborsRequest& req) {
    spaceId_ = req.get_space_id();
    auto retCode = getSpaceVidLen(spaceId_);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        for (auto& p : req.get_parts()) {
            pushResultCode(retCode, p.first);
        }
        onFinished();
        return;
    }
    planContext_ = std::make_unique<PlanContext>(env_, spaceId_, spaceVidLen_);

    retCode = checkAndBuildContexts(req);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        for (auto& p : req.get_parts()) {
            pushResultCode(retCode, p.first);
        }
        onFinished();
        return;
    }

    auto plan = buildPlan(&resultDataSet_);
    std::unordered_set<PartitionID> failedParts;
    for (const auto& partEntry : req.get_parts()) {
        auto partId = partEntry.first;
        for (const auto& input : partEntry.second) {
            CHECK_GE(input.columns.size(), 1);
            auto vId = input.columns[0].getStr();

            // the first column of each row would be the vertex id
            auto ret = plan.go(partId, vId);
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                if (failedParts.find(partId) == failedParts.end()) {
                    failedParts.emplace(partId);
                    handleErrorCode(ret, spaceId_, partId);
                }
            }
        }
    }
    onProcessFinished();
    onFinished();
}

StoragePlan<VertexID> GetNeighborsProcessor::buildPlan(nebula::DataSet* result) {
    /*
    The StoragePlan looks like this:
                 +--------+---------+
                 | GetNeighborsNode |
                 +--------+---------+
                          |
                 +--------+---------+
                 |   AggregateNode  |
                 +--------+---------+
                          |
                 +--------+---------+
                 |    FilterNode    |
                 +--------+---------+
                          |
                 +--------+---------+
             +-->+   HashJoinNode   +<----+
             |   +------------------+     |
    +--------+---------+        +---------+--------+
    |     TagNodes     |        |     EdgeNodes    |
    +------------------+        +------------------+
    */
    StoragePlan<VertexID> plan;
    std::vector<TagNode*> tags;
    for (const auto& tc : tagContext_.propContexts_) {
        auto tag = std::make_unique<TagNode>(
                planContext_.get(), &tagContext_, tc.first, &tc.second, expCtx_.get());
        tags.emplace_back(tag.get());
        plan.addNode(std::move(tag));
    }
    std::vector<EdgeNode<VertexID>*> edges;
    for (const auto& ec : edgeContext_.propContexts_) {
        auto edge = std::make_unique<SingleEdgeNode>(
                planContext_.get(), &edgeContext_, ec.first, &ec.second, expCtx_.get());
        edges.emplace_back(edge.get());
        plan.addNode(std::move(edge));
    }

    auto hashJoin = std::make_unique<HashJoinNode>(
            tags, edges, &tagContext_, &edgeContext_, expCtx_.get());
    for (auto* tag : tags) {
        hashJoin->addDependency(tag);
    }
    for (auto* edge : edges) {
        hashJoin->addDependency(edge);
    }
    auto filter = std::make_unique<FilterNode>(
            hashJoin.get(), &tagContext_, &edgeContext_, expCtx_.get(), exp_.get());
    filter->addDependency(hashJoin.get());
    auto agg = std::make_unique<AggregateNode>(filter.get(), &edgeContext_);
    agg->addDependency(filter.get());
    auto output = std::make_unique<GetNeighborsNode>(
            hashJoin.get(), agg.get(), &edgeContext_, result);
    output->addDependency(agg.get());

    plan.addNode(std::move(hashJoin));
    plan.addNode(std::move(filter));
    plan.addNode(std::move(agg));
    plan.addNode(std::move(output));
    return plan;
}

cpp2::ErrorCode GetNeighborsProcessor::checkAndBuildContexts(const cpp2::GetNeighborsRequest& req) {
    resultDataSet_.colNames.emplace_back("_vid");
    resultDataSet_.colNames.emplace_back("_stats");

    auto code = getSpaceVertexSchema();
    if (code != cpp2::ErrorCode::SUCCEEDED) {
        return code;
    }
    code = getSpaceEdgeSchema();
    if (code != cpp2::ErrorCode::SUCCEEDED) {
        return code;
    }
    code = buildTagContext(req);
    if (code != cpp2::ErrorCode::SUCCEEDED) {
        return code;
    }
    code = buildEdgeContext(req);
    if (code != cpp2::ErrorCode::SUCCEEDED) {
        return code;
    }
    code = buildFilter(req);
    if (code != cpp2::ErrorCode::SUCCEEDED) {
        return code;
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

cpp2::ErrorCode GetNeighborsProcessor::buildTagContext(const cpp2::GetNeighborsRequest& req) {
    std::vector<ReturnProp> returnProps;
    if (!req.__isset.vertex_props) {
        // If the list is not given, no prop will be returned.
        return cpp2::ErrorCode::SUCCEEDED;
    } else if (req.vertex_props.empty()) {
        // If no prpos specified, get all property of all tagId in space
        returnProps = buildAllTagProps();
    } else {
        auto ret = prepareVertexProps(req.vertex_props, returnProps);
        if (ret != cpp2::ErrorCode::SUCCEEDED) {
            return ret;
        }
    }
    // generate tag prop context
    auto ret = handleVertexProps(returnProps);
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        return ret;
    }
    buildTagTTLInfo();
    return cpp2::ErrorCode::SUCCEEDED;
}

cpp2::ErrorCode GetNeighborsProcessor::buildEdgeContext(const cpp2::GetNeighborsRequest& req) {
    edgeContext_.offset_ = tagContext_.propContexts_.size() + 2;
    std::vector<ReturnProp> returnProps;
    if (!req.__isset.edge_props) {
        // If the list is not given, no prop will be returned.
        return cpp2::ErrorCode::SUCCEEDED;
    } else if (req.edge_props.empty()) {
        // If no props specified, get all property of all edge type in space
        returnProps = buildAllEdgeProps(req.edge_direction);
    } else {
        // generate related props if no edge type or property specified
        auto ret = prepareEdgeProps(req.edge_props, returnProps);
        if (ret != cpp2::ErrorCode::SUCCEEDED) {
            return ret;
        }
    }

    // generate edge prop context
    auto ret = handleEdgeProps(returnProps);
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        return ret;
    }
    ret = handleEdgeStatProps(req.stat_props);
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        return ret;
    }
    buildEdgeTTLInfo();
    return cpp2::ErrorCode::SUCCEEDED;
}

cpp2::ErrorCode GetNeighborsProcessor::handleEdgeStatProps(
        const std::vector<cpp2::StatProp>& statProps) {
    edgeContext_.statCount_ = statProps.size();
    // todo(doodle): since we only keep one kind of stat in PropContext, there could be a problem
    // if we specified multiple stat of same prop
    for (size_t idx = 0; idx < statProps.size(); idx++) {
        // todo(doodle): wait
        /*
        const auto& prop = statProps[idx];
        const auto edgeType = prop.type;
        const auto& name = prop.name;

        auto schema = env_->schemaMan_->getEdgeSchema(spaceId_, std::abs(edgeType));
        if (!schema) {
            VLOG(1) << "Can't find spaceId " << spaceId_ << " edgeType " << edgeType;
            return cpp2::ErrorCode::E_EDGE_NOT_FOUND;
        }
        const meta::SchemaProviderIf::Field* field = nullptr;
        if (name != "_rank") {
            field = schema->field(name);
            if (field == nullptr) {
                VLOG(1) << "Can't find prop " << name << " edgeType " << edgeType;
                return cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
            }
            auto ret = checkStatType(field->type(), prop.stat);
            if (ret != cpp2::ErrorCode::SUCCEEDED) {
                return ret;
            }
        }

        // find if corresponding edgeType contexts exists
        auto edgeIter = edgeContext_.indexMap_.find(edgeType);
        if (edgeIter != edgeContext_.indexMap_.end()) {
            // find if corresponding PropContext exists
            auto& ctxs = edgeContext_.propContexts_[edgeIter->second].second;
            auto propIter = std::find_if(ctxs.begin(), ctxs.end(),
                [&] (const auto& propContext) {
                    return propContext.name_ == name;
                });
            if (propIter != ctxs.end()) {
                propIter->hasStat_ = true;
                propIter->statIndex_ = idx;
                propIter->statType_ = prop.stat;
                continue;
            } else {
                auto ctx = buildPropContextWithStat(name, idx, prop.stat, field);
                ctxs.emplace_back(std::move(ctx));
            }
        } else {
            std::vector<PropContext> ctxs;
            auto ctx = buildPropContextWithStat(name, idx, prop.stat, field);
            ctxs.emplace_back(std::move(ctx));
            edgeContext_.propContexts_.emplace_back(edgeType, std::move(ctxs));
            edgeContext_.indexMap_.emplace(edgeType, edgeContext_.propContexts_.size() - 1);
        }
        */
    }

    return cpp2::ErrorCode::SUCCEEDED;
}

PropContext GetNeighborsProcessor::buildPropContextWithStat(
        const std::string& name,
        size_t idx,
        const cpp2::StatType& statType,
        const meta::SchemaProviderIf::Field* field) {
    PropContext ctx(name.c_str());
    ctx.hasStat_ = true;
    ctx.statIndex_ = idx;
    ctx.statType_ = statType;
    ctx.field_ = field;
    // for rank stat
    if (name == "_rank") {
        ctx.propInKeyType_ = PropContext::PropInKeyType::RANK;
    }
    return ctx;
}

cpp2::ErrorCode GetNeighborsProcessor::checkStatType(const meta::cpp2::PropertyType& fType,
                                                     cpp2::StatType statType) {
    switch (statType) {
        case cpp2::StatType::SUM:
        case cpp2::StatType::AVG:
        case cpp2::StatType::MIN:
        case cpp2::StatType::MAX: {
            if (fType == meta::cpp2::PropertyType::INT64 ||
                fType == meta::cpp2::PropertyType::INT32 ||
                fType == meta::cpp2::PropertyType::INT16 ||
                fType == meta::cpp2::PropertyType::INT8 ||
                fType == meta::cpp2::PropertyType::FLOAT ||
                fType == meta::cpp2::PropertyType::DOUBLE) {
                return cpp2::ErrorCode::SUCCEEDED;
            }
            return cpp2::ErrorCode::E_INVALID_STAT_TYPE;
        }
        case cpp2::StatType::COUNT: {
             break;
        }
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

void GetNeighborsProcessor::onProcessFinished() {
    resp_.set_vertices(std::move(resultDataSet_));
}


}  // namespace storage
}  // namespace nebula
