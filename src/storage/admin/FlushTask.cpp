/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/admin/FlushTask.h"

namespace nebula {
namespace storage {

bool FlushTask::check() {
    return env_->kvstore_ != nullptr;
}

ErrorOr<nebula::cpp2::ErrorCode, std::vector<AdminSubTask>>
FlushTask::genSubTasks() {
    std::vector<AdminSubTask> ret;
    auto* store = dynamic_cast<kvstore::NebulaStore*>(env_->kvstore_);
    auto errOrSpace = store->space(*ctx_.parameters_.space_id_ref());
    if (!ok(errOrSpace)) {
        LOG(ERROR) << "Space not found";
        return error(errOrSpace);
    }

    auto space = nebula::value(errOrSpace);
    ret.emplace_back([space = space]() {
        for (auto& engine : space->engines_) {
            auto code = engine->flush();
            if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
                return code;
            }
        }
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    });
    return ret;
}

}  // namespace storage
}  // namespace nebula
