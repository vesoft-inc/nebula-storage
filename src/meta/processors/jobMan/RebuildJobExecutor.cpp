/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/network/NetworkUtils.h"
#include "meta/ActiveHostsMan.h"
#include "meta/MetaServiceUtils.h"
#include "meta/common/MetaCommon.h"
#include "meta/processors/Common.h"
#include "meta/processors/jobMan/RebuildJobExecutor.h"
#include "utils/Utils.h"

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace meta {

bool RebuildJobExecutor::check() {
    return paras_.size() == 2;
}

cpp2::ErrorCode RebuildJobExecutor::prepare() {
    auto spaceRet = getSpaceIdFromName(paras_[1]);
    if (!nebula::ok(spaceRet)) {
        LOG(ERROR) << "Can't find the space: " << paras_[1];
        return nebula::error(spaceRet);
    }
    space_ = nebula::value(spaceRet);

    std::vector<std::string> indexes;
    folly::split(',', paras_[0], indexes);
    for (auto& indexName : indexes) {
        std::string indexValue;
        auto indexKey = MetaServiceUtils::indexIndexKey(space_, indexName);
        auto result = kvstore_->get(kDefaultSpaceId, kDefaultPartId, indexKey, &indexValue);
        if (result != kvstore::ResultCode::SUCCEEDED) {
            LOG(ERROR) << "Get indexKey error indexName: " << indexName;
            return cpp2::ErrorCode::E_NOT_FOUND;
        }

        auto indexId = *reinterpret_cast<const IndexID*>(indexValue.c_str());
        indexIds_.emplace_back(indexId);
        LOG(INFO) << "Rebuild Index Space " << space_ << ", Index " << indexId;
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

meta::cpp2::ErrorCode RebuildJobExecutor::stop() {
    auto errOrTargetHost = getTargetHost(space_);
    if (!nebula::ok(errOrTargetHost)) {
        LOG(ERROR) << "Get target host failed";
        return cpp2::ErrorCode::E_NO_HOSTS;
    }

    auto& hosts = nebula::value(errOrTargetHost);
    std::vector<folly::Future<Status>> futures;
    for (auto& host : hosts) {
        auto future = adminClient_->stopTask({Utils::getAdminAddrFromStoreAddr(host.first)},
                                             jobId_, 0);
        futures.emplace_back(std::move(future));
    }

    folly::collectAll(std::move(futures))
        .thenValue([] (const auto& tries) mutable {
            for (const auto& t : tries) {
                if (!t.value().ok()) {
                    LOG(ERROR) << "Stop Build Index Failed";
                    return cpp2::ErrorCode::E_STOP_JOB_FAILURE;
                }
            }
            return cpp2::ErrorCode::SUCCEEDED;
        })
        .thenError([] (auto &&e) {
            LOG(ERROR) << "Exception caught: " << e.what();
            return cpp2::ErrorCode::E_STOP_JOB_FAILURE;
        }).wait();

    return cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace meta
}  // namespace nebula
