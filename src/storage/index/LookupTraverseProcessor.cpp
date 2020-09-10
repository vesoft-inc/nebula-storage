/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/index/LookupTraverseProcessor.h"

namespace nebula {
namespace storage {

void LookupTraverseProcessor::process(const cpp2::LookupAndTraverseRequest& req) {
    auto retCode = requestCheck(req);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        for (auto& p : req.get_parts()) {
            pushResultCode(retCode, p);
        }
        onFinished();
        return;
    }

    // build lookup plan
    auto lookupPlan = buildPlan();
    if (!lookupPlan.ok()) {
        for (auto& p : req.get_parts()) {
            pushResultCode(cpp2::ErrorCode::E_INDEX_NOT_FOUND, p);
        }
        onFinished();
        return;
    }

    traverse_.spaceId_ = spaceId_;
    traverse_.spaceVidLen_ = spaceVidLen_;
    traverse_.planContext_ = std::make_unique<PlanContext>(env_, spaceId_, spaceVidLen_);
    traverse_.expCtx_ = std::make_unique<StorageExpressionContext>(spaceVidLen_);

    retCode = traverse_.checkAndBuildContexts(req.traverse_spec);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        for (auto& p : req.get_parts()) {
            pushResultCode(retCode, p);
        }
        onFinished();
        return;
    }
    // build traverse plan
    auto goPlan = traverse_.buildPlan(&traverse_.resultDataSet_,
                                      traverse_.limit_,
                                      traverse_.random_);

    for (const auto& partId : req.get_parts()) {
        resultDataSet_.clear();
        auto ret = lookupPlan.value().go(partId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            handleErrorCode(ret, spaceId_, partId);
            continue;
        }
        // Get result of lookup, then go from these vertices. When we look up index,
        // we could only use src to traverse later, because dst could belongs to other part.
        // So we only use the first column of look up result, which is the vId or srcId.
        auto vIds = getStartPoints();
        for (const auto& vId : vIds) {
            ret = goPlan.go(partId, vId);
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                handleErrorCode(ret, spaceId_, partId);
            }
        }
    }
    onProcessFinished();
    onFinished();
}

std::unordered_set<VertexID> LookupTraverseProcessor::getStartPoints() {
    std::unordered_set<VertexID> result;
    for (const auto row : resultDataSet_.rows) {
        auto vId = row.values[0].getStr();
        result.emplace(vId.substr(0, vId.find_first_of('\0')));
    }
    return result;
}

void LookupTraverseProcessor::onProcessFinished() {
    resp_.set_vertices(std::move(traverse_.resultDataSet_));
}

}  // namespace storage
}  // namespace nebula
