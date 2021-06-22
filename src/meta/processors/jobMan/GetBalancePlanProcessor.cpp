/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/jobMan/GetBalancePlanProcessor.h"

namespace nebula {
namespace meta {

void GetBalancePlanProcessor::process(const cpp2::GetBalancePlanReq& req) {
    auto spaceId = req.get_space_id();
    CHECK_SPACE_ID_AND_RETURN(spaceId);

    auto balanceKey = MetaServiceUtils::balancePlanKey(spaceId);
    std::string val;
    auto ret = kvstore_->get(kDefaultSpaceId, kDefaultPartId, balanceKey, &val);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "SpaceId " << spaceId << " no balance data, "
                   << "please submit balance job under the space.";
        handleErrorCode(ret);
        onFinished();
        return;
    }

    auto planItem = MetaServiceUtils::parseBalancePlanVal(val);
    auto jobStatus = planItem.get_status();
    LOG(INFO) << "Job Status: " << static_cast<int32_t>(jobStatus);
    if (jobStatus != cpp2::JobStatus::FINISHED) {
        LOG(ERROR) << "SpaceId " << spaceId << " balance job is running or failed, "
                   << "please show jobs.";
        handleErrorCode(nebula::cpp2::ErrorCode::E_JOB_NOT_FINISHED);
        onFinished();
        return;
    }

    resp_.set_plan(std::move(planItem));
    handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
    onFinished();
}

}  // namespace meta
}  // namespace nebula
