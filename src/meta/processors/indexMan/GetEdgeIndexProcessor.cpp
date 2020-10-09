/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/indexMan/GetEdgeIndexProcessor.h"

namespace nebula {
namespace meta {

void GetEdgeIndexProcessor::process(const cpp2::GetEdgeIndexReq& req) {
    auto spaceID = req.get_space_id();
    CHECK_SPACE_ID_AND_RETURN(spaceID);
    auto indexName = req.get_index_name();
    folly::SharedMutex::ReadHolder rHolder(LockUtils::edgeIndexLock());

    auto edgeIndexIDResult = getIndexID(spaceID, indexName);
    if (!edgeIndexIDResult.ok()) {
        LOG(ERROR) << "Get Edge Index SpaceID: " << spaceID
                   << " Index Name: " << indexName << " not found";
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    LOG(INFO) << "Get Edge Index SpaceID: " << spaceID << " Index Name: " << indexName;
    auto indexId = edgeIndexIDResult.value();

    auto retItem = getIndexItem(spaceID, indexId);
    if (!retItem.ok()) {
        LOG(ERROR) << "Get Edge Index Failed: SpaceID " << spaceID
                   << " Index Name: " << indexName << " status: " << retItem.status();
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_item(std::move(retItem.value()));
    onFinished();
}

}  // namespace meta
}  // namespace nebula
