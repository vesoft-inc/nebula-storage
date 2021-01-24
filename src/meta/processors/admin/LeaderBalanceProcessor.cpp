/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


#include "meta/processors/admin/LeaderBalanceProcessor.h"

DEFINE_double(leader_balance_deviation, 0.05, "after leader balance, leader count should in range "
                                              "[avg * (1 - deviation), avg * (1 + deviation)]");

namespace nebula {
namespace meta {

void LeaderBalanceProcessor::process(const cpp2::LeaderBalanceReq& req) {
    space_ = req.get_space_id();
    LOG(INFO) << "Space " << req.get_space_id();
    auto ret = leaderBalance();
    handleErrorCode(ret);
    onFinished();
}

nebula::cpp2::ErrorCode LeaderBalanceProcessor::leaderBalance() {
    folly::Promise<Status> promise;
    auto future = promise.getFuture();
    auto spaceInfoRet = MetaCommon::getSpaceInfo(kvstore_, space_);
    if (!spaceInfoRet.ok()) {
        LOG(ERROR) << folly::sformat("Can't get space {}", space_);
        return nebula::cpp2::ErrorCode::E_STORE_FAILURE;
    }

    auto spaceInfo = spaceInfoRet.value();
    hostLeaderMap_.reset(new HostLeaderMap);
    auto status = client_->getLeaderDist(hostLeaderMap_.get()).get();
    if (!status.ok() || hostLeaderMap_->empty()) {
        LOG(ERROR) << "Get leader distribution failed";
        return nebula::cpp2::ErrorCode::E_RPC_FAILURE;
    }

    std::vector<folly::SemiFuture<Status>> futures;
    auto replicaFactor = spaceInfo.first;
    auto dependentOnGroup = spaceInfo.second;

    LeaderBalancePlan plan;
    auto balanceResult = buildLeaderBalancePlan(hostLeaderMap_.get(),
                                                replicaFactor,
                                                dependentOnGroup,
                                                plan);
    if (!nebula::ok(balanceResult) || !nebula::value(balanceResult)) {
        LOG(ERROR) << "Building leader balance plan failed Space: " << space_;
        return nebula::cpp2::ErrorCode::E_BALANCER_FAILURE;
    }

    simplifyLeaderBalnacePlan(plan);
    for (const auto& task : plan) {
        futures.emplace_back(client_->transLeader(std::get<0>(task), std::get<1>(task),
                                                  std::move(std::get<2>(task)),
                                                  std::move(std::get<3>(task))));
    }

    int32_t failed = 0;
    folly::collectAll(futures).via(executor_.get()).thenTry([&](const auto& result) {
        auto tries = result.value();
        for (const auto& t : tries) {
            if (!t.value().ok()) {
                ++failed;
            }
        }
    }).wait();

    if (failed != 0) {
        LOG(ERROR) << failed << " partiton failed to transfer leader";
        return nebula::cpp2::ErrorCode::E_BALANCER_FAILURE;
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

ErrorOr<nebula::cpp2::ErrorCode, bool>
LeaderBalanceProcessor::buildLeaderBalancePlan(HostLeaderMap* hostLeaderMap,
                                               int32_t replicaFactor,
                                               bool dependentOnGroup,
                                               LeaderBalancePlan& plan,
                                               bool useDeviation) {
    PartAllocation peersMap;
    // store peers of all paritions in peerMap
    folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());

    int32_t leaderParts = calculateLeaderParts(peersMap);
    if (leaderParts == -1) {
        return false;
    }

    int32_t totalParts = 0;
    HostParts allHostParts;
    auto result = getHostParts(dependentOnGroup, allHostParts, totalParts);
    if (!nebula::ok(result)) {
        return nebula::error(result);
    }

    auto retVal = nebula::value(result);
    if (!retVal || totalParts == 0 || allHostParts.empty()) {
        LOG(ERROR) << "Invalid space " << space_;
        return false;
    }

    HostParts leaderHostParts;
    std::unordered_set<HostAddr> activeHosts;
    for (const auto& host : *hostLeaderMap) {
        // only balance leader between hosts which have valid partition
        if (!allHostParts[host.first].empty()) {
            activeHosts.emplace(host.first);
            leaderHostParts[host.first] = (*hostLeaderMap)[host.first][space_];
        }
    }

    if (activeHosts.empty()) {
        LOG(ERROR) << "No active hosts";
        return false;
    }

    calculateHostBounds(dependentOnGroup, allHostParts, replicaFactor,
                        useDeviation, leaderParts, activeHosts);

    while (true) {
        int32_t taskCount = 0;
        bool hasUnbalancedHost = false;
        for (const auto& hostEntry : leaderHostParts) {
            auto host = hostEntry.first;
            auto& hostMinLoad = hostBounds_[host].first;
            auto& hostMaxLoad = hostBounds_[host].second;
            int32_t partSize = hostEntry.second.size();
            if (hostMinLoad <= partSize && partSize <= hostMaxLoad) {
                VLOG(3) << partSize << " is between min load "
                        << hostMinLoad << " and max load " << hostMaxLoad;
                continue;
            }

            hasUnbalancedHost = true;
            if (partSize < hostMinLoad) {
                // need to acquire leader from other hosts
                LOG(INFO) << "Acquire leaders to host: " << host
                          << " loading: " << partSize
                          << " min loading " << hostMinLoad;
                taskCount += acquireLeaders(allHostParts, leaderHostParts, peersMap,
                                            activeHosts, host, plan);
            } else {
                // need to transfer leader to other hosts
                LOG(INFO) << "Giveup leaders from host: " << host
                          << " loading: " << partSize
                          << " max loading " << hostMaxLoad;
                taskCount += giveupLeaders(leaderHostParts, peersMap,
                                           activeHosts, host, plan);
            }
        }

        // If every host is balanced or no more task during this loop, then the plan is done
        if (!hasUnbalancedHost || taskCount == 0) {
            LOG(INFO) << "Not need balance";
            break;
        }
    }
    return true;
}

int32_t LeaderBalanceProcessor::calculateLeaderParts(PartAllocation& peersMap) {
    auto prefix = MetaServiceUtils::partPrefix(space_);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Access kvstore failed, spaceId " << space_;
        return -1;
    }

    int32_t leaderParts = 0;
    while (iter->valid()) {
        auto key = iter->key();
        PartitionID partId;
        memcpy(&partId, key.data() + prefix.size(), sizeof(PartitionID));
        auto peers = MetaServiceUtils::parsePartVal(iter->val());
        peersMap[partId] = std::move(peers);
        ++leaderParts;
        iter->next();
    }
    return leaderParts;
}
void LeaderBalanceProcessor::calculateHostBounds(bool dependentOnGroup,
                                                 HostParts allHostParts,
                                                 int32_t replicaFactor,
                                                 bool useDeviation,
                                                 size_t leaderParts,
                                                 std::unordered_set<HostAddr> activeHosts) {
    if (dependentOnGroup) {
        for (auto it = allHostParts.begin(); it != allHostParts.end(); it++) {
            auto min = it->second.size() / replicaFactor;
            VLOG(3) << "Host: " << it->first << " Bounds: " << min << " : " << min + 1;
            hostBounds_[it->first] = std::make_pair(min, min + 1);
        }
    } else {
        size_t activeSize = activeHosts.size();
        size_t globalAvg = leaderParts / activeSize;
        size_t globalMin = globalAvg;
        size_t globalMax = globalAvg;
        if (leaderParts % activeSize != 0) {
            globalMax += 1;
        }

        if (useDeviation) {
            globalMin = std::ceil(static_cast<double> (leaderParts) / activeSize *
                                  (1 - FLAGS_leader_balance_deviation));
            globalMax = std::floor(static_cast<double> (leaderParts) / activeSize *
                                   (1 + FLAGS_leader_balance_deviation));
        }
        VLOG(3) << "Build leader balance plan, expected min load: " << globalMin
                << ", max load: " << globalMax << " avg: " << globalAvg;

        for (auto it = allHostParts.begin(); it != allHostParts.end(); it++) {
            hostBounds_[it->first] = std::make_pair(globalMin, globalMax);
        }
    }
}

int32_t LeaderBalanceProcessor::acquireLeaders(HostParts& allHostParts,
                                 HostParts& leaderHostParts,
                                 PartAllocation& peersMap,
                                 std::unordered_set<HostAddr>& activeHosts,
                                 const HostAddr& target,
                                 LeaderBalancePlan& plan) {
    // host will loop for the partition which is not leader, and try to acuire the leader
    int32_t taskCount = 0;
    std::vector<PartitionID> diff;
    std::set_difference(allHostParts[target].begin(), allHostParts[target].end(),
                        leaderHostParts[target].begin(), leaderHostParts[target].end(),
                        std::back_inserter(diff));
    auto& targetLeaders = leaderHostParts[target];
    size_t minLoad = hostBounds_[target].first;
    for (const auto& partId : diff) {
        VLOG(3) << "Try acquire leader for part " << partId;
        // find the leader of partId
        auto sources = peersMap[partId];
        for (const auto& source : sources) {
            if (source == target || !activeHosts.count(source)) {
                continue;
            }

            // if peer is the leader of partId and can transfer, then transfer it to host
            auto& sourceLeaders = leaderHostParts[source];
            VLOG(3) << "Check peer: " << source << " min load: " << minLoad
                    << " peerLeaders size: " << sourceLeaders.size();
            auto it = std::find(sourceLeaders.begin(), sourceLeaders.end(), partId);
            if (it != sourceLeaders.end() && minLoad < sourceLeaders.size()) {
                sourceLeaders.erase(it);
                targetLeaders.emplace_back(partId);
                plan.emplace_back(space_, partId, source, target);
                LOG(INFO) << "acquire plan trans leader space: " << space_
                          << " part: " << partId
                          << " from " << source.host << ":" << source.port
                          << " to " << target.host << ":" << target.port;
                ++taskCount;
                break;
            }
        }

        // if host has enough leader, just return
        if (targetLeaders.size() == minLoad) {
            LOG(INFO) << "Host: " << target  << "'s leader reach " << minLoad;
            break;
        }
    }
    return taskCount;
}

int32_t LeaderBalanceProcessor::giveupLeaders(HostParts& leaderParts,
                                PartAllocation& peersMap,
                                std::unordered_set<HostAddr>& activeHosts,
                                const HostAddr& source,
                                LeaderBalancePlan& plan) {
    int32_t taskCount = 0;
    auto& sourceLeaders = leaderParts[source];
    size_t maxLoad = hostBounds_[source].second;

    // host will try to transfer the extra leaders to other peers
    for (auto it = sourceLeaders.begin(); it != sourceLeaders.end();) {
        // find the leader of partId
        auto partId = *it;
        const auto& targets = peersMap[partId];
        bool isErase = false;

        // leader should move to the peer with lowest loading
        auto target = std::min_element(targets.begin(), targets.end(),
                                       [&](const auto &l, const auto &r) -> bool {
                                           if (source == l || !activeHosts.count(l)) {
                                               return false;
                                           }
                                           return leaderParts[l].size() < leaderParts[r].size();
                                       });

        // If peer can accept this partition leader, than host will transfer to the peer
        if (target != targets.end()) {
            auto& targetLeaders = leaderParts[*target];
            int32_t targetLeaderSize = targetLeaders.size();
            if (targetLeaderSize < hostBounds_[*target].second) {
                it = sourceLeaders.erase(it);
                targetLeaders.emplace_back(partId);
                plan.emplace_back(space_, partId, source, *target);
                LOG(INFO) << "giveup plan trans leader space: " << space_
                          << " part: " << partId
                          << " from " << source.host << ":" << source.port
                          << " to " << target->host << ":" << target->port;
                ++taskCount;
                isErase = true;
            }
        }

        // if host has enough leader, just return
        if (sourceLeaders.size() == maxLoad) {
            LOG(INFO) << "Host: " << source  << "'s leader reach " << maxLoad;
            break;
        }

        if (!isErase) {
            ++it;
        }
    }
    return taskCount;
}

void LeaderBalanceProcessor::simplifyLeaderBalnacePlan(LeaderBalancePlan& plan) {
    // Within a leader balance plan, a partition may be moved several times, but actually
    // we only need to transfer the leadership of a partition from the first host to the
    // last host, and ignore the intermediate ones
    std::unordered_map<PartitionID, LeaderBalancePlan> buckets;
    for (auto& task : plan) {
        buckets[std::get<1>(task)].emplace_back(task);
    }
    plan.clear();
    for (const auto& partEntry : buckets) {
        plan.emplace_back(space_, partEntry.first,
                          std::get<2>(partEntry.second.front()),
                          std::get<3>(partEntry.second.back()));
    }
}

ErrorOr<nebula::cpp2::ErrorCode, bool>
LeaderBalanceProcessor::getHostParts(bool dependentOnGroup,
                                     HostParts& hostParts,
                                     int32_t& totalParts) {
    folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
    auto prefix = MetaServiceUtils::partPrefix(space_);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto code = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Access kvstore failed, spaceId " << space_;
        return false;
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

    LOG(INFO) << "Host size: " << hostParts.size();
    auto key = MetaServiceUtils::spaceKey(space_);
    std::string value;
    code = kvstore_->get(kDefaultSpaceId, kDefaultPartId, key, &value);
    if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Access kvstore failed, spaceId " << space_;
        return false;
    }

    auto properties = MetaServiceUtils::parseSpace(value);
    if (totalParts != properties.get_partition_num()) {
        LOG(ERROR) << "Partition number not equals";
        return false;
    }

    if (properties.group_name_ref().has_value()) {
        auto groupName = *properties.group_name_ref();
        if (dependentOnGroup) {
            auto zonePartsRet = assembleZoneParts(groupName, hostParts);
            if (zonePartsRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
                LOG(ERROR) << "Assemble Zone Parts failed group: " << groupName;
                return zonePartsRet;
            }
        }
    }

    totalParts *= properties.get_replica_factor();
    return true;
}

nebula::cpp2::ErrorCode
LeaderBalanceProcessor::assembleZoneParts(const std::string& groupName, HostParts& hostParts) {
    auto groupKey = MetaServiceUtils::groupKey(groupName);
    std::string groupValue;
    auto code = kvstore_->get(kDefaultSpaceId, kDefaultPartId, groupKey, &groupValue);
    if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Get group " << groupName << " failed"
                   << apache::thrift::util::enumNameSafe(code);
        return code;
    }

    // zoneHosts use to record this host belong to zone's hosts
    std::unordered_map<std::pair<HostAddr, std::string>, std::vector<HostAddr>> zoneHosts;
    auto zoneNames = MetaServiceUtils::parseZoneNames(std::move(groupValue));
    for (auto zoneName : zoneNames) {
        auto zoneKey = MetaServiceUtils::zoneKey(zoneName);
        std::string zoneValue;
        code = kvstore_->get(kDefaultSpaceId, kDefaultPartId, zoneKey, &zoneValue);
        if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
            LOG(ERROR) << "Get zone " << zoneName << " failed"
                       << apache::thrift::util::enumNameSafe(code);
            return code;
        }

        auto hosts = MetaServiceUtils::parseZoneHosts(std::move(zoneValue));
        for (const auto& host : hosts) {
            auto pair = std::pair<HostAddr, std::string>(std::move(host),
                                                         zoneName);
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
            zoneParts_[it->first] = ZoneNameAndParts(name, partIter->second);
        }
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace meta
}  // namespace nebula

