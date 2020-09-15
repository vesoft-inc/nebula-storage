/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/index/LookupProcessor.h"

namespace nebula {
namespace storage {

void LookupProcessor::process(const cpp2::LookupIndexRequest& req) {
    auto retCode = requestCheck(req);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        for (auto& p : req.get_parts()) {
            pushResultCode(retCode, p);
        }
        onFinished();
        return;
    }

    auto plan = buildPlan();
    if (!plan.ok()) {
        for (auto& p : req.get_parts()) {
            pushResultCode(cpp2::ErrorCode::E_INDEX_NOT_FOUND, p);
        }
        onFinished();
        return;
    }

    std::unordered_set<PartitionID> failedParts;
    for (const auto& partId : req.get_parts()) {
        auto ret = plan.value().go(partId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            if (failedParts.find(partId) == failedParts.end()) {
                failedParts.emplace(partId);
                handleErrorCode(ret, spaceId_, partId);
            }
        }
    }
    onProcessFinished();
    onFinished();
}

void LookupProcessor::onProcessFinished() {
    resp_.set_data(std::move(resultDataSet_));
}

}  // namespace storage
}  // namespace nebula
