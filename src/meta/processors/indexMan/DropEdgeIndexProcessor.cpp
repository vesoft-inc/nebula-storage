/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/indexMan/DropEdgeIndexProcessor.h"

namespace nebula {
namespace meta {

void DropEdgeIndexProcessor::process(const cpp2::DropEdgeIndexReq& req) {
    auto spaceID = req.get_space_id();
    auto indexName = req.get_index_name();
    CHECK_SPACE_ID_AND_RETURN(spaceID);
    folly::SharedMutex::WriteHolder wHolder(LockUtils::edgeIndexLock());

    auto edgeIndexID = getIndexID(spaceID, indexName);
    if (!edgeIndexID.ok()) {
        LOG(ERROR) << "Edge Index not exists in Space: " << spaceID << " Index name: " << indexName;
        if (req.get_if_exists()) {
            handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
        } else {
            handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        }
        onFinished();
        return;
    }

    auto indexId = edgeIndexID.value();
    // Now, there are multiple index types.
    // when we delete the index, there is no need to specify the index type
    // Find indextype by indexId
    auto ret = getIndexType(spaceID, indexId);
    if (!ret.ok()) {
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    auto indexType = ret.value();

    std::vector<std::string> keys;
    keys.emplace_back(MetaServiceUtils::indexIndexKey(spaceID, indexName));
    keys.emplace_back(MetaServiceUtils::indexKey(spaceID, indexType, indexId));

    LOG(INFO) << "Drop Edge Index " << indexName;
    resp_.set_id(to(indexId, EntryType::INDEX));
    doSyncMultiRemoveAndUpdate(std::move(keys));
}

}  // namespace meta
}  // namespace nebula

