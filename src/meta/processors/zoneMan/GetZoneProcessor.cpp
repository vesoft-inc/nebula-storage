/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/zoneMan/GetZoneProcessor.h"

namespace nebula {
namespace meta {

void GetZoneProcessor::process(const cpp2::GetZoneReq& req) {
    folly::SharedMutex::ReadHolder rHolder(LockUtils::zoneLock());
    auto zoneName = req.get_zone_name();
    auto zoneIdRet = getZoneId(zoneName);
    if (!zoneIdRet.ok()) {
        LOG(ERROR) << "Zone " << zoneName << " not found";
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    auto zoneKey = MetaServiceUtils::zoneKey(zoneName);
    auto zoneValueRet = doGet(std::move(zoneKey));
    if (!zoneValueRet.ok()) {
        LOG(ERROR) << "Get zone " << zoneName << " failed";
        handleErrorCode(cpp2::ErrorCode::E_STORE_FAILURE);
        onFinished();
        return;
    }
    auto hosts = MetaServiceUtils::parseZoneHosts(std::move(zoneValueRet).value());
    LOG(INFO) << "Get Zone: " << zoneName << " node size: " << hosts.size();
    resp_.set_hosts(std::move(hosts));
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    onFinished();
}

}  // namespace meta
}  // namespace nebula
