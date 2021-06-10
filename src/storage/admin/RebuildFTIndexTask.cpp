/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/admin/RebuildFTIndexTask.h"
#include "common/base/Logging.h"

<<<<<<< HEAD
<<<<<<< HEAD
DECLARE_uint32(raft_heartbeat_interval_secs);

=======
>>>>>>> rebuild fulltext index via listener
=======
DECLARE_uint32(raft_heartbeat_interval_secs);

>>>>>>> rebase master; rebuild done logic
namespace nebula {
namespace storage {

ErrorOr<nebula::cpp2::ErrorCode, std::vector<AdminSubTask>>
RebuildFTIndexTask::genSubTasks() {
    std::vector<AdminSubTask> tasks;
    VLOG(3) << "Begin rebuild fulltext indexes, space : " << *ctx_.parameters_.space_id_ref();
    auto parts = *ctx_.parameters_.parts_ref();
    auto* store = dynamic_cast<kvstore::NebulaStore*>(env_->kvstore_);
    auto listenerRet = store->spaceListener(*ctx_.parameters_.space_id_ref());
    if (!ok(listenerRet)) {
        return error(listenerRet);
    }
    auto space = nebula::value(listenerRet);
    for (const auto& part : parts) {
<<<<<<< HEAD
        nebula::kvstore::Listener *listener = nullptr;
=======
        nebula::kvstore::Listener *listener;
>>>>>>> rebuild fulltext index via listener
        for (auto& lMap : space->listeners_) {
            if (part != lMap.first) {
                continue;
            }
            for (auto& l : lMap.second) {
                if (l.first != meta::cpp2::ListenerType::ELASTICSEARCH) {
                    continue;
                }
                listener = l.second.get();
                break;
            }
        }
<<<<<<< HEAD
        if (listener == nullptr) {
            return nebula::cpp2::ErrorCode::E_LISTENER_NOT_FOUND;
        }
        if (!listener->isRunning()) {
            LOG(ERROR) << "listener not ready, may be starting or waiting snapshot";
            // TODO : add ErrorCode for listener not ready.
=======
        if (!listener) {
>>>>>>> rebuild fulltext index via listener
            return nebula::cpp2::ErrorCode::E_LISTENER_NOT_FOUND;
        }
        VLOG(3) << folly::sformat("Processing fulltext rebuild subtask, space={}, part={}",
                                  *ctx_.parameters_.space_id_ref(), part);
        std::function<nebula::cpp2::ErrorCode()> task =
            std::bind(&RebuildFTIndexTask::taskByPart, this, listener);
        tasks.emplace_back(std::move(task));
    }
    return tasks;
}

nebula::cpp2::ErrorCode
RebuildFTIndexTask::taskByPart(nebula::kvstore::Listener* listener) {
<<<<<<< HEAD
<<<<<<< HEAD
    auto part = listener->partitionId();
    listener->resetListener();
    while (true) {
        sleep(FLAGS_raft_heartbeat_interval_secs);
        if (listener->pursueLeaderDone()) {
            return nebula::cpp2::ErrorCode::SUCCEEDED;
        }
        VLOG(1) << folly::sformat(
            "Processing fulltext rebuild subtask, part={}, rebuild_log={}",
            part, listener->getApplyId());
=======
    auto endLogId = listener->getApplyId();
=======
>>>>>>> rebase master; rebuild done logic
    auto part = listener->partitionId();
    listener->resetListener();
    while (true) {
        sleep(FLAGS_raft_heartbeat_interval_secs);
        if (listener->pursueLeaderDone()) {
            return nebula::cpp2::ErrorCode::SUCCEEDED;
        }
        VLOG(3) << folly::sformat(
<<<<<<< HEAD
            "Processing fulltext rebuild subtask, part={}, rebuild_log={}, end_log={}",
            part, listener->getApplyId(), endLogId);
        sleep(5);
>>>>>>> rebuild fulltext index via listener
=======
            "Processing fulltext rebuild subtask, part={}, rebuild_log={}",
            part, listener->getApplyId());
>>>>>>> rebase master; rebuild done logic
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace storage
}  // namespace nebula
