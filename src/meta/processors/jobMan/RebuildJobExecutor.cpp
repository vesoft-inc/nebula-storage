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

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace meta {

bool RebuildJobExecutor::check() {
    if (paras_.size() != 3) {
        return false;
    } else {
        return true;
    }
}

cpp2::ErrorCode RebuildJobExecutor::prepare() {
    std::string spaceName = paras_[0];
    std::string indexName = paras_[1];
    isOffline_ = paras_[2] == "offline" ? true : false;
    LOG(INFO) << "Rebuild Index Space " << spaceName << ", Index Name "
              << indexName << " " << isOffline_;

    auto spaceRet = getSpaceIdFromName(spaceName);
    if (!nebula::ok(spaceRet)) {
        LOG(ERROR) << "Can't find the space: " << spaceName;
        return nebula::error(spaceRet);
    }
    spaceId_ = nebula::value(spaceRet);

    std::string indexValue;
    auto indexKey = MetaServiceUtils::indexIndexKey(spaceId_, indexName);
    auto result = kvstore_->get(kDefaultSpaceId, kDefaultPartId, indexKey, &indexValue);
    if (result != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Get indexKey error indexName: " << indexName;
        return cpp2::ErrorCode::E_NOT_FOUND;
    }
    indexId_ = *reinterpret_cast<const IndexID*>(indexValue.c_str());
    return cpp2::ErrorCode::SUCCEEDED;
}

std::vector<Status>
RebuildJobExecutor::handleRebuildIndexResult(std::vector<folly::Future<Status>> results,
                                             kvstore::KVStore* kvstore,
                                             std::string statusKey) {
    std::vector<Status> codes;
    folly::collectAll(std::move(results))
        .thenValue([statusKey, kvstore, &codes] (const auto& tries) mutable {
            for (const auto& t : tries) {
                if (!t.value().ok()) {
                    LOG(ERROR) << "Build Edge Index Failed";
                    codes.push_back(nebula::Status::Error("Build Edge Index Failed"));
                    if (!MetaCommon::saveRebuildStatus(kvstore, statusKey, "FAILED")) {
                        LOG(ERROR) << "Save rebuild status failed";
                    }
                }
            }

            if (!MetaCommon::saveRebuildStatus(kvstore, std::move(statusKey), "SUCCEEDED")) {
                LOG(ERROR) << "Save rebuild status failed";
            }
            return codes;
        })
        .thenError([statusKey, kvstore, &codes] (auto &&e) {
            LOG(ERROR) << "Exception caught: " << e.what();
            if (!MetaCommon::saveRebuildStatus(kvstore, std::move(statusKey), "FAILED")) {
                LOG(ERROR) << "Save rebuild status failed";
            }
            return codes;
        });
        return codes;
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
        auto future = adminClient_->stopTask({host.first}, jobId_, 0);
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
        });

    return cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace meta
}  // namespace nebula
