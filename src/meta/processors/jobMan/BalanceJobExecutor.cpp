/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/MetaServiceUtils.h"
#include "meta/processors/jobMan/BalanceJobExecutor.h"

namespace nebula {
namespace meta {

BalanceJobExecutor::BalanceJobExecutor(JobID jobId,
                                       kvstore::KVStore* kvstore,
                                       AdminClient* adminClient,
                                       const std::vector<std::string>& paras)
    : MetaJobExecutor(jobId, kvstore, adminClient, paras) {}


bool BalanceJobExecutor::check() {
    return false;
}

cpp2::ErrorCode BalanceJobExecutor::prepare() {
    plan_ = std::make_unique<BalancePlan>(time::WallClock::fastNowInSec(),
                                          kvstore_, adminClient_);

    auto function = [this] () {
        std::lock_guard<std::mutex> lg(lock_);
        auto code = LastUpdateTimeMan::update(kvstore_, time::WallClock::fastNowInMilliSec());
        if (code != kvstore::ResultCode::SUCCEEDED) {
            LOG(ERROR) << "Balance plan " << plan_->id() << " update meta failed";
        }

        CHECK(!lock_.try_lock());
        plan_.reset();
    };

    plan_->setFinishCallback(std::move(function));
    return plan_->taskSize() == 0 ? cpp2::ErrorCode::E_BALANCED : cpp2::ErrorCode::SUCCEEDED;
}

cpp2::ErrorCode
BalanceJobExecutor::getSpaceInfo(GraphSpaceID spaceId,
                                 std::pair<int32_t, bool>& spaceInfo) {
    folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
    auto spaceKey = MetaServiceUtils::spaceKey(spaceId);
    std::string spaceValue;
    auto code = kvstore_->get(kDefaultSpaceId, kDefaultPartId, spaceKey, &spaceValue);
    if (code != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Get space failed";
        return cpp2::ErrorCode::E_STORE_FAILURE;
    }

    auto properties = MetaServiceUtils::parseSpace(std::move(spaceValue));
    bool zoned = properties.__isset.group_name ? true : false;
    spaceInfo = std::make_pair(properties.replica_factor, zoned);
    return cpp2::ErrorCode::SUCCEEDED;
}

ErrorOr<cpp2::ErrorCode, std::vector<BalanceTask>>
BalanceJobExecutor::genBalanceTasks(GraphSpaceID spaceId,
                                    int32_t spaceReplica,
                                    bool dependentOnGroup,
                                    std::vector<HostAddr>&& hostDel) {
    UNUSED(spaceReplica); UNUSED(hostDel);
    std::unordered_map<HostAddr, std::vector<PartitionID>> hostParts;
    // hostParts is current part allocation map
    auto totalParts = getHostParts(spaceId, dependentOnGroup, hostParts);
    if (totalParts == 0 || hostParts.empty()) {
        LOG(ERROR) << "Invalid space " << spaceId;
        return cpp2::ErrorCode::E_NOT_FOUND;
    }

    std::vector<HostAddr> expand;
    std::vector<HostAddr> activeHosts;
    if (dependentOnGroup) {
        activeHosts = ActiveHostsMan::getActiveHostsBySpace(kvstore_, spaceId);
    } else {
        activeHosts = ActiveHostsMan::getActiveHosts(kvstore_);
    }

    return cpp2::ErrorCode::E_NOT_FOUND;
}

cpp2::ErrorCode BalanceJobExecutor::stop() {
    return cpp2::ErrorCode::SUCCEEDED;
}

int32_t BalanceJobExecutor::getHostParts(GraphSpaceID spaceId,
                                         bool dependentOnGroup,
                                         HostParts& hostParts) {
    folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
    int32_t totalParts = 0;
    auto prefix = MetaServiceUtils::partPrefix(spaceId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto code = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (code != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Access kvstore failed, spaceId " << spaceId;
        return totalParts;
    }

    while (iter->valid()) {
        auto key = iter->key();
        PartitionID partId;
        memcpy(&partId, key.data() + prefix.size(), sizeof(PartitionID));
        auto partHosts = MetaServiceUtils::parsePartVal(iter->val());
        for (auto& ph : partHosts) {
            hostParts[ph].emplace_back(partId);
        }
        totalParts++;
        iter->next();
    }

    LOG(INFO) << "Host parts size: " << hostParts.size();
    auto key = MetaServiceUtils::spaceKey(spaceId);
    std::string value;
    code = kvstore_->get(kDefaultSpaceId, kDefaultPartId, key, &value);
    if (code != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Access kvstore failed, spaceId " << spaceId;
        return totalParts;
    }

    auto properties = MetaServiceUtils::parseSpace(value);
    CHECK_EQ(totalParts, properties.get_partition_num());
    if (dependentOnGroup) {
        auto groupName = *properties.get_group_name();
        LOG(INFO) << "Group Name: " << groupName;
        auto groupKey = MetaServiceUtils::groupKey(groupName);
        std::string groupValue;
        code = kvstore_->get(kDefaultSpaceId, kDefaultPartId, groupKey, &groupValue);
        if (code != kvstore::ResultCode::SUCCEEDED) {
            LOG(ERROR) << "Get group " << groupName << " failed";
            return false;
        }

        // zoneHosts use to record this host belong to zone's hosts
        std::unordered_map<std::pair<HostAddr, std::string>, std::vector<HostAddr>> zoneHosts;
        auto zoneNames = MetaServiceUtils::parseZoneNames(std::move(groupValue));
        for (auto zoneName : zoneNames) {
            auto zoneKey = MetaServiceUtils::zoneKey(zoneName);
            std::string zoneValue;
            code = kvstore_->get(kDefaultSpaceId, kDefaultPartId, zoneKey, &zoneValue);
            if (code != kvstore::ResultCode::SUCCEEDED) {
                LOG(ERROR) << "Get zone " << zoneName << " failed";
                return false;
            }

            auto hosts = MetaServiceUtils::parseZoneHosts(std::move(zoneValue));
            for (auto host : hosts) {
                auto pair = std::pair<HostAddr, std::string>(std::move(host),
                                                             std::move(zoneName));
                auto& hs = zoneHosts[std::move(pair)];
                hs.insert(hs.end(), hosts.begin(), hosts.end());
            }
        }

        for (auto it = hostParts.begin(); it != hostParts.end(); it++) {
            auto host = it->first;
            auto zoneIter = std::find_if(zoneHosts.begin(), zoneHosts.end(),
                                         [host](const auto& pair) -> bool {
                return host == pair.first.first;
            });

            if (zoneIter == zoneHosts.end()) {
                LOG(INFO) << it->first << " have lost";
                continue;
            }

            auto& hosts = zoneIter->second;
            auto name = zoneIter->first.second;
            for (auto hostIter = hosts.begin(); hostIter != hosts.end(); hostIter++) {
                auto partIter = hostParts.find(*hostIter);
                zoneParts_[it->first] = ZoneParts(name, partIter->second);
            }
        }
    }
    totalParts *= properties.get_replica_factor();
    return totalParts;
}

folly::Future<Status>
BalanceJobExecutor::executeInternal(HostAddr&& address, std::vector<PartitionID>&& parts) {
    UNUSED(address); UNUSED(parts);
    return Status::OK();
}

}  // namespace meta
}  // namespace nebula

