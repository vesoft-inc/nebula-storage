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
#include "storage/exec/CatenateNode.h"

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
    auto dag = buildDAG(&resultDataSet_);

    auto ret = dag.go(partId, vId).get();
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        handleErrorCode(ret, spaceId_, partId);
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

StorageDAG UpdateVertexProcessor::buildDAG(nebula::DataSet* result) {
    StorageDAG dag;
    std::vector<TagUpdateNode*> tagUpdates;
    // TODO now, return prop, filter prop, update prop on same tagId
    for (const auto& tc : tagContext_.propContexts_) {
        // Process all need attributes of one tag at a time
        auto tagUpdate = std::make_unique<TagUpdateNode>(&tagContext_,
                                                         env_,
                                                         spaceId_,
                                                         spaceVidLen_,
                                                         tc.first,
                                                         &tc.second,
                                                         insertable_,
                                                         updateTagIds_,
                                                         updatedVertexProps_);
        tagUpdates.emplace_back(TagUpdate.get());
        dag.addNode(std::move(TagUpdate));
    }

    auto filterNode = std::make_unique<TagFilterNode>(env_,
                                                      spaceId_,
                                                      filterExp_.get(),
                                                      tagUpdates,
                                                      &tagContext_);

    for (atuo* tagUpdate : tagUpdates) {
        filterNode->addDependency(tagUpdate);
    }
    dag.addNode(std::move(filterNode));

    auto updateNode = std::make_unique<UpdateNode>(env_,
                                                   spaceId_,
                                                   updatedVertexProps_,
                                                   filterNode.get(),
                                                   expCtx_.get());
    updateNode->addDependency(filterNode.get());
    dag.addNode(std::move(updateNode));

    auto catNode = std::make_unique<CatenateUpdateNode>(env_,
                                                        spaceId_,
                                                        UpdateNode.get(),
                                                        returnPropsExp_,
                                                        result);
    catNode->addDependency(updateNode.get());
    dag.addNode(std::move(catNode));

    return dag;
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
    // TODO QueryBaseProcessor::checkExp to implement
    if (expCtx_ == nullptr) {
        expCtx_ = std::make_unique<ExpressionContext>();
    }

    // Return props
    if (req.__isset.return_props) {
        for (auto& prop : *req.get_return_props()) {
            auto colExpRet = Expression::decode(prop);
            if (!colExpRet.ok()) {
                return cpp2::ErrorCode::E_INVALID_UPDATER;
            }
            auto colExp = std::move(colExpRet).value();
            colExp->setContext(expCtx_.get());
            auto status = colExp->prepare();
            if (!status.ok() || !checkExp(colExp.get())) {
                return cpp2::ErrorCode::E_INVALID_UPDATER;
            }
            returnPropsExp_.emplace_back(std::move(colExp));
        }
    }

    // Condition(where)
    if (req.__isset.condition) {
        const auto& filterStr = *req.get_condition();
        if (!filterStr.empty()) {
            // Todo Expression::decode
            auto expRet = Expression::decode(filterStr);
            if (!expRet.ok()) {
                VLOG(1) << "Can't decode the filter " << filterStr;
                return cpp2::ErrorCode::E_INVALID_FILTER;
            }
            filterExp_ = std::move(expRet).value();
            filterExp_->setContext(expCtx_.get());
            auto status = filterExp_->prepare();
            if (!status.ok() || !checkExp(filterExp_.get())) {
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
        sourcePropExp.setContext(expCtx_.get());
        auto status = sourcePropExp.prepare();
        if (!status.ok() || !checkExp(&sourcePropExp)) {
            return cpp2::ErrorCode::E_INVALID_UPDATER;
        }
        if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
            VLOG(1) << "Evict cache for vId " << vId << ", tagId " << tagId;
            vertexCache_->evict(std::make_pair(vId, tagId), partId);
        }

        updateTagIds_.emplace(tagId);
        auto exp = Expression::decode(vertexProp.get_value());
        if (!exp.ok()) {
            VLOG(1) << "Can't decode the prop's value " << vertexProp.get_value();
            return cpp2::ErrorCode::E_INVALID_UPDATER;
        }
        auto vexp = std::move(exp).value();
        vexp->setContext(expCtx_.get());
        status = vexp->prepare();
        if (!status.ok() || !checkExp(vexp.get())) {
            return cpp2::ErrorCode::E_INVALID_UPDATER;
        }
    }

    if (expCtx_->hasDstTagProp() || expCtx_->hasEdgeProp()
        || expCtx_->hasVariableProp() || expCtx_->hasInputProp()) {
        LOG(ERROR) << "should only contain SrcTagProp expression!";
        return cpp2::ErrorCode::E_INVALID_UPDATER;
    }
}

void UpdateVertexProcessor::onProcessFinished() {
    resp_.set_props(std::move(resultDataSet_));
}

}  // namespace storage
}  // namespace nebula
