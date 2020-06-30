/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "storage/mutate/UpdateEdgeProcessor.h"
#include "utils/NebulaKeyUtils.h"
#include "storage/exec/EdgeNode.h"
#include "storage/exec/FilterNode.h"
#include "storage/exec/UpdateNode.h"
#include "storage/exec/UpdateResultNode.h"

namespace nebula {
namespace storage {

void UpdateEdgeProcessor::process(const cpp2::UpdateEdgeRequest& req) {
    spaceId_ = req.get_space_id();
    auto partId = req.get_part_id();
    edgeKey_ = req.get_edge_key();
    updatedProps_ = req.get_updated_props();
    if (req.__isset.insertable) {
        insertable_ = *req.get_insertable();
    }

    auto retCode = getSpaceVidLen(spaceId_);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        pushResultCode(retCode, partId);
        onFinished();
        return;
    }

    if (!NebulaKeyUtils::isValidVidLen(spaceVidLen_, edgeKey_.src, edgeKey_.dst)) {
        LOG(ERROR) << "Space " << spaceId_ << ", vertex length invalid, "
                   << " space vid len: " << spaceVidLen_ << ",  edge srcVid: " << edgeKey_.src
                   << " dstVid: " << edgeKey_.dst;
        pushResultCode(cpp2::ErrorCode::E_INVALID_VID, partId);
        onFinished();
        return;
    }

    retCode = checkAndBuildContexts(req);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Failure build contexts!";
        pushResultCode(retCode, partId);
        onFinished();
        return;
    }

    CHECK_NOTNULL(env_->indexMan_);
    auto iRet = env_->indexMan_->getEdgeIndexes(spaceId_);
    if (iRet.ok()) {
        indexes_ = std::move(iRet).value();
    }

    VLOG(3) << "Update edge, spaceId: " << spaceId_ << ", partId:  " << partId
            << ", src: " << edgeKey_.get_src() << ", edge_type: " << edgeKey_.get_edge_type()
            << ", dst: " << edgeKey_.get_dst() << ", ranking: " << edgeKey_.get_ranking();

    // Now, the index is not considered
    auto plan = buildPlan(&resultDataSet_);

    auto ret = plan.go(partId, edgeKey_);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        handleErrorCode(ret, spaceId_, partId);
        if (ret == kvstore::ResultCode::ERR_RESULT_FILTERED) {
            onProcessFinished();
        }
    } else {
        onProcessFinished();
    }
    onFinished();
    return;
}

cpp2::ErrorCode
UpdateEdgeProcessor::checkAndBuildContexts(const cpp2::UpdateEdgeRequest& req) {
    // Build edgeContext_.schemas_
    auto retCode = buildEdgeSchema();
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        return retCode;
    }

    // Build edgeContext_.propContexts_ edgeTypeProps_
    retCode = buildEdgeContext(req);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        return retCode;
    }

    // Build edgeContext_.ttlInfo_
    buildEdgeTTLInfo();

    return cpp2::ErrorCode::SUCCEEDED;
}

/*
The storage plan of update(upsert) edge looks like this:
             +--------+----------+
             | UpdateEdgeResNode |
             +--------+----------+
                      |
             +--------+----------+
             |   UpdateEdgeNode  |
             +--------+----------+
                      |
             +--------+----------+
             |   EdgeFilterNode  |
             +--------+----------+
                      |
             +--------+----------+
             |   FetchEdgeNode   |
             +-------------------+
*/
StoragePlan<cpp2::EdgeKey> UpdateEdgeProcessor::buildPlan(nebula::DataSet* result) {
    StoragePlan<cpp2::EdgeKey> plan;
    std::vector<EdgeNode<cpp2::EdgeKey>*> edgeUpdates;
    // because update edgetype is one
    for (const auto& ec : edgeContext_.propContexts_) {
        // Process all need attributes of one edge at a time
        auto edgeUpdate = std::make_unique<FetchEdgeNode>(&edgeContext_,
                                                          env_,
                                                          spaceId_,
                                                          spaceVidLen_,
                                                          ec.first,
                                                          &ec.second,
                                                          nullptr);
        edgeUpdates.emplace_back(edgeUpdate.get());
        plan.addNode(std::move(edgeUpdate));
    }

    auto filterNode = std::make_unique<EdgeFilterNode>(edgeUpdates,
                                                       &edgeContext_,
                                                       filterExp_.get(),
                                                       env_,
                                                       spaceId_,
                                                       expCtx_.get(),
                                                       insertable_,
                                                       edgeKey_,
                                                       spaceVidLen_);

    for (auto* edge : edgeUpdates) {
        filterNode->addDependency(edge);
    }

    auto updateNode = std::make_unique<UpdateEdgeNode>(env_,
                                                       spaceId_,
                                                       spaceVidLen_,
                                                       indexes_,
                                                       updatedProps_,
                                                       filterNode.get());
    updateNode->addDependency(filterNode.get());

    auto resultNode = std::make_unique<UpdateEdgeResNode>(updateNode.get(),
                                                          getReturnPropsExp(),
                                                          result);
    resultNode->addDependency(updateNode.get());
    plan.addNode(std::move(filterNode));
    plan.addNode(std::move(updateNode));
    plan.addNode(std::move(resultNode));
    return plan;
}

// Get all edge schema in spaceID
cpp2::ErrorCode UpdateEdgeProcessor::buildEdgeSchema() {
    auto edges = env_->schemaMan_->getAllVerEdgeSchema(spaceId_);
    if (!edges.ok()) {
        return cpp2::ErrorCode::E_SPACE_NOT_FOUND;
    }

    edgeContext_.schemas_ = std::move(edges).value();
    return cpp2::ErrorCode::SUCCEEDED;
}

// edgeContext.propContexts_ return prop, filter prop, update prop
// returnPropsExp_ has return expression
// filterExp_      has filter expression
// updatedEdgeProps_  has update expression
cpp2::ErrorCode
UpdateEdgeProcessor::buildEdgeContext(const cpp2::UpdateEdgeRequest& req) {
    if (expCtx_ == nullptr) {
        expCtx_ = std::make_unique<UpdateExpressionContext>();
    }

    // Return props
    if (req.__isset.return_props) {
        for (auto& prop : *req.get_return_props()) {
            auto colExp = Expression::decode(prop);
            if (!colExp) {
                VLOG(1) << "Can't decode the return expression";
                return cpp2::ErrorCode::E_INVALID_UPDATER;
            }
            if (!checkExp(colExp.get())) {
                return cpp2::ErrorCode::E_INVALID_UPDATER;
            }
            returnPropsExp_.emplace_back(std::move(colExp));
        }
    }

    // Condition
    if (req.__isset.condition) {
        const auto& filterStr = *req.get_condition();
        if (!filterStr.empty()) {
            filterExp_ = Expression::decode(filterStr);
            if (!filterExp_) {
                VLOG(1) << "Can't decode the filter " << filterStr;
                return cpp2::ErrorCode::E_INVALID_FILTER;
            }
            if (!checkExp(filterExp_.get())) {
                return cpp2::ErrorCode::E_INVALID_FILTER;
            }
        }
    }

    // Build context of the update edge prop
    for (auto& edgeProp : updatedProps_) {
        auto edgeName = env_->schemaMan_->toEdgeName(spaceId_, edgeKey_.edge_type);
        if (!edgeName.ok()) {
            VLOG(1) << "Can't find spaceId " << spaceId_ << " edgeType " << edgeKey_.edge_type;
            return cpp2::ErrorCode::E_EDGE_NOT_FOUND;
        }

        EdgePropertyExpression edgePropExp(new std::string(edgeName.value()),
                                           new std::string(edgeProp.get_name()));
        if (!checkExp(&edgePropExp)) {
            VLOG(1) << "Invalid update edge expression!";
            return cpp2::ErrorCode::E_INVALID_UPDATER;
        }

        auto updateExp = Expression::decode(edgeProp.get_value());
        if (!updateExp) {
            VLOG(1) << "Can't decode the prop's value " << edgeProp.get_value();
            return cpp2::ErrorCode::E_INVALID_UPDATER;
        }
        if (!checkExp(updateExp.get())) {
            return cpp2::ErrorCode::E_INVALID_UPDATER;
        }
    }

    // update edge only handle one edgetype
    // maybe no updated prop, filter prop, return prop
    if (edgeContext_.propContexts_.size() != 1 ||
        edgeContext_.propContexts_[0].first != edgeKey_.edge_type) {
        VLOG(1) << "should only contain one edge in update edge!";
        return cpp2::ErrorCode::E_INVALID_UPDATER;
    }

    return cpp2::ErrorCode::SUCCEEDED;
}

void UpdateEdgeProcessor::onProcessFinished() {
    resp_.set_props(std::move(resultDataSet_));
}

}  // namespace storage
}  // namespace nebula
