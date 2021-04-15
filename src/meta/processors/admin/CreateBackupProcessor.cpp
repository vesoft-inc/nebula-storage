/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/time/TimeUtils.h"

#include "meta/ActiveHostsMan.h"
#include "meta/processors/admin/CreateBackupProcessor.h"
#include "meta/processors/admin/SnapShot.h"

namespace nebula {
namespace meta {

ErrorOr<cpp2::ErrorCode, std::unordered_set<GraphSpaceID>> CreateBackupProcessor::spaceNameToId(
    const std::vector<std::string>* backupSpaces) {
    folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
    std::unordered_set<GraphSpaceID> spaces;

    if (backupSpaces != nullptr) {
        DCHECK(!backupSpaces->empty());
        std::vector<std::string> values;
        std::vector<std::string> keys;

        values.reserve(backupSpaces->size());
        keys.reserve(backupSpaces->size());

        std::transform(
            backupSpaces->begin(), backupSpaces->end(), std::back_inserter(keys), [](auto& name) {
                return MetaServiceUtils::indexSpaceKey(name);
            });

        auto ret = kvstore_->multiGet(kDefaultSpaceId, kDefaultPartId, std::move(keys), &values);
        if (ret.first != kvstore::ResultCode::SUCCEEDED) {
            LOG(ERROR) << "Failed to get space id, error: " << ret.first;
            if (ret.first == kvstore::ResultCode::ERR_KEY_NOT_FOUND) {
                return cpp2::ErrorCode::E_BACKUP_SPACE_NOT_FOUND;
            }
            return MetaCommon::to(ret.first);
        }

        std::transform(std::make_move_iterator(values.begin()),
                       std::make_move_iterator(values.end()),
                       std::inserter(spaces, spaces.end()),
                       [](const std::string& rawID) {
                           return *reinterpret_cast<const GraphSpaceID*>(rawID.c_str());
                       });

    } else {
        auto prefix = MetaServiceUtils::spacePrefix();
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return MetaCommon::to(ret);
        }

        while (iter->valid()) {
            auto spaceId = MetaServiceUtils::spaceId(iter->key());
            auto spaceName = MetaServiceUtils::spaceName(iter->val());
            VLOG(3) << "List spaces " << spaceId << ", name " << spaceName;
            spaces.emplace(spaceId);
            iter->next();
        }
    }

    if (spaces.empty()) {
        LOG(ERROR) << "Failed to create a full backup because there is currently no space.";
        return cpp2::ErrorCode::E_BACKUP_SPACE_NOT_FOUND;
    }

    return spaces;
}

void CreateBackupProcessor::process(const cpp2::CreateBackupReq& req) {
    auto* backupSpaces = req.get_spaces();
    auto* store = static_cast<kvstore::NebulaStore*>(kvstore_);
    if (!store->isLeader(kDefaultSpaceId, kDefaultPartId)) {
        handleErrorCode(cpp2::ErrorCode::E_LEADER_CHANGED);
        onFinished();
        return;
    }

    auto result = MetaServiceUtils::isIndexRebuilding(kvstore_);
    if (!nebula::ok(result)) {
        LOG(ERROR) << "Index is rebuilding, not allowed to create backup.";
        handleErrorCode(MetaCommon::to(nebula::error(result)));
        onFinished();
        return;
    }

    if (nebula::value(result)) {
        handleErrorCode(cpp2::ErrorCode::E_BACKUP_BUILDING_INDEX);
        onFinished();
        return;
    }

    folly::SharedMutex::WriteHolder wHolder(LockUtils::snapshotLock());

    auto hosts = ActiveHostsMan::getActiveHosts(kvstore_);
    if (hosts.empty()) {
        LOG(ERROR) << "There has some offline hosts";
        handleErrorCode(cpp2::ErrorCode::E_NO_HOSTS);
        onFinished();
        return;
    }

    auto spaceIdRet = spaceNameToId(backupSpaces);
    if (!nebula::ok(spaceIdRet)) {
        handleErrorCode(nebula::error(spaceIdRet));
        onFinished();
        return;
    }

    auto spaces = nebula::value(spaceIdRet);

    // The entire process follows mostly snapshot logic.
    std::vector<kvstore::KV> data;
    auto backupName = folly::format("BACKUP_{}", MetaServiceUtils::genTimestampStr()).str();

    data.emplace_back(MetaServiceUtils::snapshotKey(backupName),
                      MetaServiceUtils::snapshotVal(cpp2::SnapshotStatus::INVALID,
                                                    NetworkUtils::toHostsStr(hosts)));
    Snapshot::instance(kvstore_, client_)->setSpaces(spaces);

    // step 1 : Blocking all writes action for storage engines.
    auto ret = Snapshot::instance(kvstore_, client_)->blockingWrites(SignType::BLOCK_ON);
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Send blocking sign to storage engine error";
        handleErrorCode(ret);
        ret = Snapshot::instance(kvstore_, client_)->blockingWrites(SignType::BLOCK_OFF);
        if (ret != cpp2::ErrorCode::SUCCEEDED) {
            LOG(ERROR) << "Cancel write blocking error";
        }
        onFinished();
        return;
    }

    // step 3 : Create checkpoint for all storage engines.
    auto sret = Snapshot::instance(kvstore_, client_)->createSnapshot(backupName);
    if (sret.isLeftType()) {
        LOG(ERROR) << "Checkpoint create error on storage engine";
        handleErrorCode(sret.left());
        ret = Snapshot::instance(kvstore_, client_)->blockingWrites(SignType::BLOCK_OFF);
        if (ret != cpp2::ErrorCode::SUCCEEDED) {
            LOG(ERROR) << "Cancel write blocking error";
        }
        onFinished();
        return;
    }

    // step 4 created backup for meta(export sst).
    auto backupFiles = MetaServiceUtils::backupSpaces(kvstore_, spaces, backupName, backupSpaces);
    if (!nebula::ok(backupFiles)) {
        LOG(ERROR) << "Failed backup meta";
        handleErrorCode(MetaCommon::to(nebula::error(backupFiles)));
        onFinished();
        return;
    }

    // step 5 : checkpoint created done, so release the write blocking.
    ret = Snapshot::instance(kvstore_, client_)->blockingWrites(SignType::BLOCK_OFF);
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Cancel write blocking error";
        handleErrorCode(ret);
        onFinished();
        return;
    }

    // step 6 : update snapshot status from INVALID to VALID.
    data.emplace_back(MetaServiceUtils::snapshotKey(backupName),
                      MetaServiceUtils::snapshotVal(cpp2::SnapshotStatus::VALID,
                                                    NetworkUtils::toHostsStr(hosts)));

    auto putRet = doSyncPut(std::move(data));
    if (putRet != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "All checkpoint creations are done, "
                      "but update checkpoint status error. "
                      "backup : "
                   << backupName;
        handleErrorCode(MetaCommon::to(putRet));
        onFinished();
        return;
    }

    std::unordered_map<GraphSpaceID, cpp2::SpaceBackupInfo> backupInfo;

    // set backup info
    auto snapshotInfo = std::move(sret.right());
    for (auto id : spaces) {
        LOG(INFO) << "backup space " << id;
        cpp2::SpaceBackupInfo spaceInfo;
        auto spaceKey = MetaServiceUtils::spaceKey(id);
        auto spaceRet = doGet(spaceKey);
        if (!spaceRet.ok()) {
            handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
            onFinished();
            return;
        }
        auto properties = MetaServiceUtils::parseSpace(spaceRet.value());
        // todo we should save partition info.
        auto it = snapshotInfo.find(id);
        DCHECK(it != snapshotInfo.end());
        spaceInfo.set_cp_dirs(it->second);
        spaceInfo.set_space(std::move(properties));
        backupInfo.emplace(id, std::move(spaceInfo));
    }
    cpp2::BackupMeta backup;
    LOG(INFO) << "sst files count was:" << nebula::value(backupFiles).size();
    backup.set_meta_files(std::move(nebula::value(backupFiles)));
    backup.set_backup_info(std::move(backupInfo));
    backup.set_backup_name(std::move(backupName));
    backup.set_full(true);
    if (backupSpaces == nullptr) {
        backup.set_include_system_space(true);
    } else {
        backup.set_include_system_space(false);
    }
    backup.set_create_time(time::WallClock::fastNowInMilliSec());

    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_meta(std::move(backup));
    LOG(INFO) << "backup done";

    onFinished();
}
}   // namespace meta
}   // namespace nebula
