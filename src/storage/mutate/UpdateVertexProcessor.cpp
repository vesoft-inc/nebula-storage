/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "storage/mutate/UpdateVertexProcessor.h"
#include "utils/NebulaKeyUtils.h"
#include "kvstore/LogEncoder.h"
#include "codec/RowWriterV2.h"
#include "storage/exec/TagNode.h"
#include "storage/exec/FilterNode.h"
#include "storage/exec/UpdateNode.h"
#include "storage/exec/UpdateTagResNode.h"
#include "common/expression/Expression.h"

namespace nebula {
namespace storage {

void UpdateVertexProcessor::process(const cpp2::UpdateVertexRequest& req) {
    spaceId_ = req.get_space_id();
    auto partId = req.get_part_id();
    auto vId = req.get_vertex_id();
    updatedVertexProps_ = req.get_updated_props();
    if (req.__isset.insertable) {
        insertable_ = *req.get_insertable();
    }

    auto retCode = getSpaceVidLen(spaceId_);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        pushResultCode(retCode, partId);
        onFinished();
        return;
    }

    if (!NebulaKeyUtils::isValidVidLen(spaceVidLen_, vId)) {
        LOG(ERROR) << "Space " << spaceId_ << ", vertex length invalid, "
                   << " space vid len: " << spaceVidLen_ << ",  vid is " << vId;
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
    auto iRet = env_->indexMan_->getTagIndexes(spaceId_);
    if (iRet.ok()) {
        indexes_ = std::move(iRet).value();
    }

    VLOG(3) << "Update vertex, spaceId: " << spaceId_
            << ", partId: " << partId << ", vId: " << vId;

    // Now, the index is not considered
    auto plan = buildPlan(&resultDataSet_);

    auto ret = plan.go(partId, vId);

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
UpdateVertexProcessor::checkAndBuildContexts(const cpp2::UpdateVertexRequest& req) {
    // Build tagContext_.schemas_
    auto retCode = buildTagSchema();
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        return retCode;
    }

    // Build tagContext_.propContexts_  tagIdProps_
    retCode = buildTagContext(req);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        return retCode;
    }

    // Build tagContext_.ttlInfo_
    buildTagTTLInfo();

    return cpp2::ErrorCode::SUCCEEDED;
}

StoragePlan<VertexID> UpdateVertexProcessor::buildPlan(nebula::DataSet* result) {
    StoragePlan<VertexID> plan;
    std::vector<TagNode*> tagUpdates;
    // handle tag props, return prop, filter prop, update prop
    for (const auto& tc : tagContext_.propContexts_) {
        // Process all need attributes of one tag at a time
        auto tagUpdate = std::make_unique<TagNode>(&tagContext_,
                                                   env_,
                                                   spaceId_,
                                                   spaceVidLen_,
                                                   tc.first,
                                                   &tc.second,
                                                   nullptr);
        tagUpdates.emplace_back(tagUpdate.get());
        plan.addNode(std::move(tagUpdate));
    }

    std::vector<EdgeNode<VertexID>*> edgeNodes;
    EdgeContext* edgeContext = nullptr;
    auto filterNode = std::make_unique<UpdateFilterNode>(tagUpdates,
                                                         edgeNodes,
                                                         &tagContext_,
                                                         edgeContext,
                                                         filterExp_.get(),
                                                         env_,
                                                         spaceId_,
                                                         expCtx_.get(),
                                                         insertable_,
                                                         updateTagIds_,
                                                         spaceVidLen_);

    for (auto* tagUpdate : tagUpdates) {
        filterNode->addDependency(tagUpdate);
    }

    auto updateNode = std::make_unique<UpdateTagNode>(env_,
                                                      spaceId_,
                                                      updatedVertexProps_,
                                                      filterNode.get());
    updateNode->addDependency(filterNode.get());

    auto resultNode = std::make_unique<UpdateTagResNode>(updateNode.get(),
                                                         getReturnPropsExp(),
                                                         result);
    resultNode->addDependency(updateNode.get());
    plan.addNode(std::move(filterNode));
    plan.addNode(std::move(updateNode));
    plan.addNode(std::move(resultNode));
    return plan;
}

// Get all tag schema in spaceID
cpp2::ErrorCode UpdateVertexProcessor::buildTagSchema() {
    auto tags = env_->schemaMan_->getAllVerTagSchema(spaceId_);
    if (!tags.ok()) {
        return cpp2::ErrorCode::E_SPACE_NOT_FOUND;
    }
    tagContext_.schemas_ = std::move(tags).value();
    return cpp2::ErrorCode::SUCCEEDED;
}

// tagContext_.propContexts has return prop, filter prop, update prop
// returnPropsExp_ has return expression
// filterExp_      has filter expression
// updatedVertexProps_  has update expression
cpp2::ErrorCode
UpdateVertexProcessor::buildTagContext(const cpp2::UpdateVertexRequest& req) {
    if (expCtx_ == nullptr) {
        expCtx_ = std::make_unique<UpdateExpressionContext>();
    }

    // Return props
    if (req.__isset.return_props) {
        for (auto& prop : *req.get_return_props()) {
            auto colExp = Expression::decode(prop);
            if (!colExp) {
                return cpp2::ErrorCode::E_INVALID_UPDATER;
            }
            if (!checkExp(colExp.get())) {
                return cpp2::ErrorCode::E_INVALID_UPDATER;
            }
            returnPropsExp_.emplace_back(std::move(colExp));
        }
    }

    // Condition(where)
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

    auto partId = req.get_part_id();
    auto vId = req.get_vertex_id();
    // Build context of the update vertex prop
    for (auto& vertexProp : updatedVertexProps_) {
        auto tagId = vertexProp.get_tag_id();

        auto tagName = env_->schemaMan_->toTagName(spaceId_, tagId);
        if (!tagName.ok()) {
            VLOG(1) << "Can't find spaceId " << spaceId_ << " tagId " << tagId;
            return cpp2::ErrorCode::E_TAG_NOT_FOUND;
        }

        SourcePropertyExpression sourcePropExp(new std::string(tagName.value()),
                                               new std::string(vertexProp.get_name()));
        if (!checkExp(&sourcePropExp)) {
            return cpp2::ErrorCode::E_INVALID_UPDATER;
        }

        // update, evict the old elements
        if (FLAGS_enable_vertex_cache && tagContext_.vertexCache_ != nullptr) {
            VLOG(1) << "Evict cache for vId " << vId << ", tagId " << tagId;
            tagContext_.vertexCache_->evict(std::make_pair(vId, tagId), partId);
        }

        // Todo spport UPSERT VERTEX 202 SET tag; must have tagId
        updateTagIds_.emplace(tagId);
        auto updateExp = Expression::decode(vertexProp.get_value());
        if (!updateExp) {
            VLOG(1) << "Can't decode the prop's value " << vertexProp.get_value();
            return cpp2::ErrorCode::E_INVALID_UPDATER;
        }
        if (!checkExp(updateExp.get())) {
            return cpp2::ErrorCode::E_INVALID_UPDATER;
        }
    }

    // update vertex only handle one tagId
    if (this->tagContext_.propContexts_.size() > 1) {
        VLOG(1) << "should only contain one tag in update vertex!";
        return cpp2::ErrorCode::E_INVALID_UPDATER;
    }

    return cpp2::ErrorCode::SUCCEEDED;
}

void UpdateVertexProcessor::onProcessFinished() {
    resp_.set_props(std::move(resultDataSet_));
}

}  // namespace storage
}  // namespace nebula
