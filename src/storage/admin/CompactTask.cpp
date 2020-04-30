/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/admin/CompactTask.h"
#include "base/Logging.h"

namespace nebula {
namespace storage {

using ResultCode = nebula::kvstore::ResultCode;

ErrorOr<ResultCode, std::vector<AdminSubTask>>
CompactTask::genSubTasks() {
    std::vector<AdminSubTask> ret;
    if (!ctx_.store_) {
        return ret;
    }

    auto errOrSpace = ctx_.store_->space(ctx_.spaceId_);
    if (!ok(errOrSpace)) {
        return error(errOrSpace);
    }

    auto space = nebula::value(errOrSpace);

    using FuncObj = std::function<ResultCode()>;
    for (auto& engine : space->engines_) {
        FuncObj obj = std::bind(&CompactTask::subTask, this, engine.get());
        ret.emplace_back(obj);
    }
    return ret;
}

ResultCode CompactTask::subTask(nebula::kvstore::KVEngine* engine) {
    return engine->compact();
}

}  // namespace storage
}  // namespace nebula
