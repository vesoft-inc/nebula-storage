/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/query/GetPropProcessor.h"
#include "storage/exec/GetPropNode.h"

namespace nebula {
namespace storage {

void GetPropProcessor::process(const cpp2::GetPropRequest& req) {
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
    expCtx_ = std::make_unique<StorageExpressionContext>(spaceVidLen_);

    retCode = checkAndBuildContexts(req);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        for (auto& p : req.get_parts()) {
            pushResultCode(retCode, p.first);
        }
        onFinished();
        return;
    }

    std::unordered_set<PartitionID> failedParts;
    if (!isEdge_) {
        auto plan = buildTagPlan(&resultDataSet_);
        for (const auto& partEntry : req.get_parts()) {
            auto partId = partEntry.first;
            for (const auto& row : partEntry.second) {
                auto vId = row.columns[0].getStr();
                auto ret = plan.go(partId, vId);
                if (ret != kvstore::ResultCode::SUCCEEDED &&
                    failedParts.find(partId) == failedParts.end()) {
                    failedParts.emplace(partId);
                    handleErrorCode(ret, spaceId_, partId);
                }
            }
        }
    } else {
        auto plan = buildEdgePlan(&resultDataSet_);
        for (const auto& partEntry : req.get_parts()) {
            auto partId = partEntry.first;
            for (const auto& row : partEntry.second) {
                cpp2::EdgeKey edgeKey;
                edgeKey.src = row.columns[0].getStr();
                edgeKey.edge_type = row.columns[1].getInt();
                edgeKey.ranking = row.columns[2].getInt();
                edgeKey.dst = row.columns[3].getStr();
                auto ret = plan.go(partId, edgeKey);
                if (ret != kvstore::ResultCode::SUCCEEDED &&
                    failedParts.find(partId) == failedParts.end()) {
                    failedParts.emplace(partId);
                    handleErrorCode(ret, spaceId_, partId);
                }
            }
        }
    }
    onProcessFinished();
    onFinished();
}

StoragePlan<VertexID> GetPropProcessor::buildTagPlan(nebula::DataSet* result) {
    StoragePlan<VertexID> plan;
    std::vector<TagNode*> tags;
    for (const auto& tc : tagContext_.propContexts_) {
        auto tag = std::make_unique<TagNode>(
            planContext_.get(), &tagContext_, tc.first, &tc.second);
        tags.emplace_back(tag.get());
        plan.addNode(std::move(tag));
    }
    auto output = std::make_unique<GetTagPropNode>(tags, result, expCtx_.get());
    for (auto* tag : tags) {
        output->addDependency(tag);
    }
    plan.addNode(std::move(output));
    return plan;
}

StoragePlan<cpp2::EdgeKey> GetPropProcessor::buildEdgePlan(nebula::DataSet* result) {
    StoragePlan<cpp2::EdgeKey> plan;
    std::vector<EdgeNode<cpp2::EdgeKey>*> edges;
    for (const auto& ec : edgeContext_.propContexts_) {
        auto edge = std::make_unique<FetchEdgeNode>(
            planContext_.get(), &edgeContext_, ec.first, &ec.second);
        edges.emplace_back(edge.get());
        plan.addNode(std::move(edge));
    }
    auto output = std::make_unique<GetEdgePropNode>(edges, spaceVidLen_, result, expCtx_.get());
    for (auto* edge : edges) {
        output->addDependency(edge);
    }
    plan.addNode(std::move(output));
    return plan;
}

cpp2::ErrorCode GetPropProcessor::checkColumnNames(const std::vector<std::string>& colNames) {
    // Column names for the pass-in data. When getting the vertex props, the first
    // column has to be "_vid", when getting the edge props, the first four columns
    // have to be "_src", "_type", "_rank", and "_dst"
    if (colNames.size() != 1 && colNames.size() != 4) {
        return cpp2::ErrorCode::E_INVALID_OPERATION;
    }
    if (colNames.size() == 1 && colNames[0] == "_vid") {
        isEdge_ = false;
        return cpp2::ErrorCode::SUCCEEDED;
    } else if (colNames.size() == 4 &&
               colNames[0] == _SRC &&
               colNames[1] == _TYPE &&
               colNames[2] == _RANK &&
               colNames[3] == _DST) {
        isEdge_ = true;
        return cpp2::ErrorCode::SUCCEEDED;
    }
    return cpp2::ErrorCode::E_INVALID_OPERATION;
}

cpp2::ErrorCode GetPropProcessor::checkAndBuildContexts(const cpp2::GetPropRequest& req) {
    auto code = checkColumnNames(req.column_names);
    if (code != cpp2::ErrorCode::SUCCEEDED) {
        return code;
    }
    if (!isEdge_) {
        code = getSpaceVertexSchema();
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            return code;
        }
        return buildTagContext(req);
    } else {
        code = getSpaceEdgeSchema();
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            return code;
        }
        return buildEdgeContext(req);
    }
}

cpp2::ErrorCode GetPropProcessor::buildTagContext(const cpp2::GetPropRequest& req) {
    cpp2::ErrorCode ret = cpp2::ErrorCode::SUCCEEDED;
    if (req.props.empty()) {
        // If no props specified, get all property of all tagId in space
        auto returnProps = buildAllTagProps();
        // generate tag prop context
        ret = handleVertexProps(returnProps);
        buildColName(returnProps, tagContext_.tagNames_);
    } else {
        ret = prepareVertexProps(req.props);
        buildColName(req.props);
    }

    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        return ret;
    }
    buildTagTTLInfo();
    return cpp2::ErrorCode::SUCCEEDED;
}

cpp2::ErrorCode GetPropProcessor::buildEdgeContext(const cpp2::GetPropRequest& req) {
    cpp2::ErrorCode ret = cpp2::ErrorCode::SUCCEEDED;
    if (req.props.empty()) {
        // If no props specified, get all property of all tagId in space
        auto returnProps = buildAllEdgeProps(cpp2::EdgeDirection::BOTH);
        // generate edge prop context
        ret = handleEdgeProps(returnProps);
        buildColName(returnProps, edgeContext_.edgeNames_);
    } else {
        ret = prepareEdgeProps(req.props);
        buildColName(req.props);
    }

    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        return ret;
    }
    buildEdgeTTLInfo();
    return cpp2::ErrorCode::SUCCEEDED;
}

void GetPropProcessor::buildColName(const std::vector<ReturnProp>& props,
                                    std::unordered_map<int32_t, std::string>& names) {
    for (const auto& prop : props) {
        // entryName is tagName or edgeName
        auto entryName = names[prop.entryId_];
        for (const auto& propName : prop.names_) {
            resultDataSet_.colNames.emplace_back(entryName + ":" + propName);
        }
    }
}

void GetPropProcessor::buildColName(const std::vector<cpp2::EntryProp>& entries) {
    for (const auto& entry : entries) {
        for (const auto& prop : entry.props) {
            resultDataSet_.colNames.emplace_back(std::move(prop.alias));
        }
    }
}

void GetPropProcessor::onProcessFinished() {
    resp_.set_props(std::move(resultDataSet_));
}

}  // namespace storage
}  // namespace nebula
