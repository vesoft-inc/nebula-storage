/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/index/LookupIndexProcessor.h"

namespace nebula {
namespace storage {

void LookupIndexProcessor::process(const cpp2::LookupIndexRequest& req) {
    /**
     * step 1 : prepare index meta and structure of return columns.
     */
    auto ret = prepareRequest(req);
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Prepare Request Failed";
        putResultCodes(ret, req.get_parts());
        return;
    }

    /**
     * step 2 : execute index scan.
     */
    for (auto partId : req.get_parts()) {
        auto code = executeExecutionPlan(partId);
        if (code != kvstore::ResultCode::SUCCEEDED) {
            LOG(ERROR) << "Execute Execution Plan! ret = " << static_cast<int32_t>(code)
                       << ", spaceId = " << spaceId_
                       << ", partId =  " << partId;
            if (code == kvstore::ResultCode::ERR_LEADER_CHANGED) {
                this->handleLeaderChanged(spaceId_, partId);
            } else {
                this->pushResultCode(this->to(code), partId);
            }
            this->onFinished();
            return;
        }
    }

    /**
     * step 3 : collect result.
     */
    this->resp_.set_data(std::move(returnData_));

    this->onFinished();
}

}  // namespace storage
}  // namespace nebula

