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
        auto dag = buildTagDAG(&resultDataSet_);
        for (const auto& partEntry : req.get_parts()) {
            auto partId = partEntry.first;
            for (const auto& row : partEntry.second) {
                auto vId = row.columns[0].getStr();
                auto ret = dag.go(partId, vId);
                if (ret != kvstore::ResultCode::SUCCEEDED &&
                    failedParts.find(partId) == failedParts.end()) {
                    failedParts.emplace(partId);
                    handleErrorCode(ret, spaceId_, partId);
                }
            }
        }
    } else {
        auto dag = buildEdgeDAG(&resultDataSet_);
        for (const auto& partEntry : req.get_parts()) {
            auto partId = partEntry.first;
            for (const auto& row : partEntry.second) {
                cpp2::EdgeKey edgeKey;
                edgeKey.src = row.columns[0].getStr();
                edgeKey.edge_type = row.columns[1].getInt();
                edgeKey.ranking = row.columns[2].getInt();
                edgeKey.dst = row.columns[3].getStr();
                auto ret = dag.go(partId, edgeKey);
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

StoragePlan<VertexID> GetPropProcessor::buildTagDAG(nebula::DataSet* result) {
    StoragePlan<VertexID> dag;
    std::vector<TagNode*> tags;
    for (const auto& tc : tagContext_.propContexts_) {
        auto tag = std::make_unique<TagNode>(
                &tagContext_, env_, spaceId_, spaceVidLen_, tc.first, &tc.second);
        tags.emplace_back(tag.get());
        dag.addNode(std::move(tag));
    }
    // todo(doodle): add filter
    auto output = std::make_unique<GetTagPropNode>(tags, result);
    for (auto* tag : tags) {
        output->addDependency(tag);
    }
    dag.addNode(std::move(output));
    return dag;
}

StoragePlan<cpp2::EdgeKey> GetPropProcessor::buildEdgeDAG(nebula::DataSet* result) {
    StoragePlan<cpp2::EdgeKey> dag;
    std::vector<EdgeNode<cpp2::EdgeKey>*> edges;
    for (const auto& ec : edgeContext_.propContexts_) {
        auto edge = std::make_unique<FetchEdgeNode>(
                &edgeContext_, env_, spaceId_, spaceVidLen_, ec.first, &ec.second);
        edges.emplace_back(edge.get());
        dag.addNode(std::move(edge));
    }
    // todo(doodle): add filter
    auto output = std::make_unique<GetEdgePropNode>(edges, spaceVidLen_, result);
    for (auto* edge : edges) {
        output->addDependency(edge);
    }
    dag.addNode(std::move(output));
    return dag;
}

cpp2::ErrorCode GetPropProcessor::checkColumnNames(const std::vector<std::string>& colNames) {
    // Column names for the pass-in data. When getting the vertex props, the first
    // column has to be "_vid", when getting the edge props, the first four columns
    // have to be "_src", "_type", "_ranking", and "_dst"
    if (colNames.size() != 1 && colNames.size() != 4) {
        return cpp2::ErrorCode::E_INVALID_OPERATION;
    }
    if (colNames.size() == 1 && colNames[0] == "_vid") {
        isEdge_ = false;
        return cpp2::ErrorCode::SUCCEEDED;
    } else if (colNames.size() == 4 &&
               colNames[0] == "_src" &&
               colNames[1] == "_type" &&
               colNames[2] == "_ranking" &&
               colNames[3] == "_dst") {
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
    std::vector<ReturnProp> returnProps;
    if (req.props.empty()) {
        // If no props specified, get all property of all tagId in space
        returnProps = buildAllTagProps();
    } else {
        auto ret = prepareVertexProps(req.props, returnProps);
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

cpp2::ErrorCode GetPropProcessor::buildEdgeContext(const cpp2::GetPropRequest& req) {
    std::vector<ReturnProp> returnProps;
    if (req.props.empty()) {
        // If no props specified, get all property of all tagId in space
        returnProps = buildAllEdgeProps(cpp2::EdgeDirection::BOTH);
    } else {
        auto ret = prepareEdgeProps(req.props, returnProps);
        if (ret != cpp2::ErrorCode::SUCCEEDED) {
            return ret;
        }
    }
    // generate edge prop context
    auto ret = handleEdgeProps(returnProps);
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        return ret;
    }
    buildEdgeTTLInfo();
    return cpp2::ErrorCode::SUCCEEDED;
}

kvstore::ResultCode GetPropProcessor::processOneVertex(PartitionID partId,
                                                       const std::string& prefix) {
    UNUSED(partId); UNUSED(prefix);
    return kvstore::ResultCode::SUCCEEDED;
}

void GetPropProcessor::onProcessFinished() {
    resp_.set_props(std::move(resultDataSet_));
}

}  // namespace storage
}  // namespace nebula
