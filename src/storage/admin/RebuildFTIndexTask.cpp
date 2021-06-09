/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/admin/RebuildFTIndexTask.h"
#include "common/base/Logging.h"

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
        nebula::kvstore::Listener *listener;
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
        if (!listener) {
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
    auto endLogId = listener->getApplyId();
    listener->resetListener();
    while (true) {
        if (listener->rebuildDone(endLogId)) {
            return nebula::cpp2::ErrorCode::SUCCEEDED;
        }
        VLOG(3) << folly::sformat(
            "Processing fulltext rebuild subtask, rebuild_log={}, end_log={}",
            listener->getApplyId(), endLogId);
        sleep(5);
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace storage
}  // namespace nebula
