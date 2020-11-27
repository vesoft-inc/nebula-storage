/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/partsMan/ListHostsProcessor.h"
#include "meta/ActiveHostsMan.h"
#include "meta/processors/admin/AdminClient.h"

DECLARE_int32(expired_threshold_sec);
DECLARE_int32(heartbeat_interval_secs);
DEFINE_int32(removed_threshold_sec, 24 * 60 * 60,
             "Hosts will be removed in this time if no heartbeat received");

namespace nebula {
namespace meta {

static cpp2::HostRole toHostRole(cpp2::ListHostType type) {
    switch (type) {
        case cpp2::ListHostType::GRAPH:
            return cpp2::HostRole::GRAPH;
        case cpp2::ListHostType::META:
            return cpp2::HostRole::META;
        case cpp2::ListHostType::STORAGE:
            return cpp2::HostRole::STORAGE;
        default:
            return cpp2::HostRole::UNKNOWN;
    }
}

void printHostItem(const cpp2::HostItem& item) {
    std::ostringstream oss;
    oss << "messi printHostItem()"
        << ", hostAddr=" << item.hostAddr
        << ", HostStatus=" << cpp2::_HostStatus_VALUES_TO_NAMES.at(item.status)
        << ", leader_parts.size()=" << item.leader_parts.size()
        << ", all_parts.size()=" << item.all_parts.size()
        << ", HostRole=" << cpp2::_HostRole_VALUES_TO_NAMES.at(item.role)
        << ", git_info_sha=" << item.git_info_sha;
    LOG(INFO) << oss.str();
}

void printHostItemVec(const std::vector<cpp2::HostItem>& hostItems) {
    LOG(INFO) << "messi hostItems.size()=" << hostItems.size();
    for (auto& item : hostItems) {
        printHostItem(item);
    }
}

void ListHostsProcessor::process(const cpp2::ListHostsReq& req) {
    Status status;
    {
        folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
        auto spaceRet = getSpaceIdNameMap();
        if (!spaceRet.ok()) {
            onFinished();
            return;
        }

        meta::cpp2::ListHostType type = req.get_type();
        LOG(INFO) << "messi list host type = "
                  << meta::cpp2::_ListHostType_VALUES_TO_NAMES.at(type);
        if (type == cpp2::ListHostType::ALLOC) {
            status = fillLeaderAndPartInfoPerHost();
        } else {
            auto hostRole = toHostRole(type);
            status = allHostsWithStatus(hostRole);
        }
        // status = req.__isset.role ? allHostsWithStatus(*req.get_role())
        //                           : fillLeaderAndPartInfoPerHost();
    }
    printHostItemVec(hostItems_);
    if (status.ok()) {
        handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
        resp_.set_hosts(std::move(hostItems_));
    }

    onFinished();
}

/*
 * now(2020-04-29), assume all metad have same gitInfoSHA
 * this will change if some day
 * meta.thrift support interface like getHostStatus()
 * which return a bunch of host infomation
 * it's not necessary add this interface only for gitInfoSHA
 * */
Status ListHostsProcessor::allMetaHostsStatus() {
    LOG(INFO) << "messi allMetaHostsStatus()";
    auto* partManager = kvstore_->partManager();
    auto status = partManager->partMeta(kDefaultSpaceId, kDefaultPartId);
    if (!status.ok()) {
        LOG(ERROR) << "messi Get parts failed: " << status.status();
        return status.status();
    }
    auto partMeta = status.value();
    LOG(INFO) << "messi partMeta.hosts_.size()=" << partMeta.hosts_.size();
    for (auto& host : partMeta.hosts_) {
        cpp2::HostItem item;
        item.set_hostAddr(std::move(host));
        item.set_role(cpp2::HostRole::META);
        item.set_git_info_sha(NEBULA_STRINGIFY(GIT_INFO_SHA));
        item.set_status(cpp2::HostStatus::ONLINE);
        hostItems_.emplace_back(item);
    }

    auto* nbStore = dynamic_cast<kvstore::NebulaStore*>(kvstore_);
    do {
        if (nbStore == nullptr) {
            LOG(INFO) << "messi nbStore == nullptr";
            break;
        }
        auto* raftService = nbStore->getRaftexService();
        if (raftService == nullptr) {
            LOG(INFO) << "messi raftService == nullptr";
            break;
        }
        auto spRaftPart = raftService->findPart(kDefaultSpaceId, kDefaultPartId);
        if (spRaftPart == nullptr) {
            LOG(INFO) << "messi spRaftPart == nullptr";
            break;
        }
        std::vector<HostAddr> metas;
        metas.emplace_back(spRaftPart->leader());
        LOG(INFO) << "messi metas.size()=" << metas.size();
        auto follwers = spRaftPart->peers();
        for (auto& follwer : follwers) {
            metas.emplace_back(follwer);
        }
        LOG(INFO) << "messi metas.size()=" << metas.size();
        for (auto& host : metas) {
            cpp2::HostItem item;
            item.set_hostAddr(std::move(host));
            item.set_role(cpp2::HostRole::META);
            item.set_git_info_sha(NEBULA_STRINGIFY(GIT_INFO_SHA));
            item.set_status(cpp2::HostStatus::ONLINE);
            hostItems_.emplace_back(item);
        }
    } while (0);
    return Status::OK();
}

Status ListHostsProcessor::allHostsWithStatus(cpp2::HostRole role) {
    if (role == cpp2::HostRole::META) {
        return allMetaHostsStatus();
    }
    const auto& hostPrefix = MetaServiceUtils::hostPrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto kvRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, hostPrefix, &iter);
    if (kvRet != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "List Hosts Failed: No hosts";
        handleErrorCode(cpp2::ErrorCode::E_NO_HOSTS);
        return Status::Error("Can't access kvstore, ret = %d", static_cast<int32_t>(kvRet));
    }

    auto now = time::WallClock::fastNowInMilliSec();
    std::vector<std::string> removeHostsKey;
    while (iter->valid()) {
        cpp2::HostItem item;
        auto host = MetaServiceUtils::parseHostKey(iter->key());
        item.set_hostAddr(std::move(host));
        HostInfo info = HostInfo::decode(iter->val());
        if (info.role_ != role) {
            iter->next();
            continue;
        }
        item.set_role(info.role_);
        item.set_git_info_sha(info.gitInfoSha_);
        if (now - info.lastHBTimeInMilliSec_ < FLAGS_removed_threshold_sec * 1000) {
            if (now - info.lastHBTimeInMilliSec_ < FLAGS_expired_threshold_sec * 1000) {
                item.set_status(cpp2::HostStatus::ONLINE);
            } else {
                item.set_status(cpp2::HostStatus::OFFLINE);
            }
            hostItems_.emplace_back(item);
        } else {
            removeHostsKey.emplace_back(iter->key());
        }
        iter->next();
    }

    removeExpiredHosts(std::move(removeHostsKey));
    return Status::OK();
}

Status ListHostsProcessor::fillLeaderAndPartInfoPerHost() {
    auto status = allHostsWithStatus(cpp2::HostRole::STORAGE);
    if (!status.ok()) {
        LOG(ERROR) << "Get all host's status failed";
        return status;
    }

    std::unique_ptr<kvstore::KVIterator> iter;
    const auto& leaderPrefix = MetaServiceUtils::leaderPrefix();
    auto kvRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, leaderPrefix, &iter);
    if (kvRet != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "List Hosts Failed: No leaders";
        handleErrorCode(cpp2::ErrorCode::E_NO_HOSTS);
        return Status::Error("Can't access kvstore, ret = %d", static_cast<int32_t>(kvRet));
    }

    // get hosts which have send heartbeat recently
    auto activeHosts = ActiveHostsMan::getActiveHosts(kvstore_, FLAGS_heartbeat_interval_secs * 2);
    while (iter->valid()) {
        auto host = MetaServiceUtils::parseLeaderKey(iter->key());
        if (std::find(activeHosts.begin(), activeHosts.end(), host) != activeHosts.end()) {
            auto hostIt = std::find_if(hostItems_.begin(), hostItems_.end(), [&](const auto& item) {
                return item.get_hostAddr() == host;
            });
            if (hostIt != hostItems_.end()) {
                LeaderParts leaderParts = MetaServiceUtils::parseLeaderVal(iter->val());
                hostIt->set_leader_parts(getLeaderPartsWithSpaceName(leaderParts));
            }
        }
        iter->next();
    }
    std::unordered_map<HostAddr,
                       std::unordered_map<std::string, std::vector<PartitionID>>> allParts;
    for (const auto& spaceId : spaceIds_) {
        // get space name by space id
        const auto& spaceName = spaceIdNameMap_[spaceId];

        std::unordered_map<HostAddr, std::vector<PartitionID>> hostParts;
        const auto& partPrefix = MetaServiceUtils::partPrefix(spaceId);
        kvRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, partPrefix, &iter);
        if (kvRet != kvstore::ResultCode::SUCCEEDED) {
            LOG(ERROR) << "List Hosts Failed: No partitions";
            handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
            return Status::Error("Can't find any partitions");
        }
        while (iter->valid()) {
            PartitionID partId = MetaServiceUtils::parsePartKeyPartId(iter->key());
            auto partHosts = MetaServiceUtils::parsePartVal(iter->val());
            for (auto& host : partHosts) {
                hostParts[host].emplace_back(partId);
            }
            iter->next();
        }

        for (const auto& hostEntry : hostParts) {
            allParts[hostEntry.first][spaceName] = std::move(hostEntry.second);
        }
    }

    for (const auto& hostEntry : allParts) {
        auto hostAddr = toThriftHost(hostEntry.first);
        auto it = std::find_if(hostItems_.begin(), hostItems_.end(), [&](const auto& item) {
            return item.get_hostAddr() == hostAddr;
        });
        if (it != hostItems_.end()) {
            it->set_all_parts(std::move(hostEntry.second));
        }
    }

    return Status::OK();
}

// Remove hosts that long time at OFFLINE status
void ListHostsProcessor::removeExpiredHosts(std::vector<std::string>&& removeHostsKey) {
    if (removeHostsKey.empty()) {
        return;
    }
    kvstore_->asyncMultiRemove(kDefaultSpaceId,
                               kDefaultPartId,
                               std::move(removeHostsKey),
                               [] (kvstore::ResultCode code) {
            if (code != kvstore::ResultCode::SUCCEEDED) {
                LOG(ERROR) << "Async remove long time offline hosts failed: " << code;
            }
        });
}

Status ListHostsProcessor::getSpaceIdNameMap() {
    // Get all spaces
    const auto& spacePrefix = MetaServiceUtils::spacePrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto kvRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, spacePrefix, &iter);
    if (kvRet != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "List Hosts Failed: No space found";
        handleErrorCode(cpp2::ErrorCode::E_NO_HOSTS);
        return Status::Error("Can't access kvstore, ret = %d", static_cast<int32_t>(kvRet));
    }
    while (iter->valid()) {
        auto spaceId = MetaServiceUtils::spaceId(iter->key());
        spaceIds_.emplace_back(spaceId);
        spaceIdNameMap_.emplace(spaceId, MetaServiceUtils::spaceName(iter->val()));
        iter->next();
    }
    return Status::OK();
}

std::unordered_map<std::string, std::vector<PartitionID>>
ListHostsProcessor::getLeaderPartsWithSpaceName(const LeaderParts& leaderParts) {
    std::unordered_map<std::string, std::vector<PartitionID>> result;
    for (const auto& spaceEntry : leaderParts) {
        GraphSpaceID spaceId = spaceEntry.first;
        auto iter = spaceIdNameMap_.find(spaceId);
        if (iter != spaceIdNameMap_.end()) {
            // ignore space not exists
            result.emplace(iter->second, std::move(spaceEntry.second));
        }
    }
    return result;
}

}  // namespace meta
}  // namespace nebula

