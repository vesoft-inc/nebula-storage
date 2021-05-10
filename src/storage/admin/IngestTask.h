/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_ADMIN_INGESTTASK_H_
#define STORAGE_ADMIN_INGESTTASK_H_

#include "kvstore/KVEngine.h"
#include "kvstore/NebulaStore.h"
#include "storage/admin/AdminTask.h"

namespace nebula {
namespace storage {

class IngestTask : public AdminTask {
public:
    IngestTask(StorageEnv* env, TaskContext&& ctx) : AdminTask(env, std::move(ctx)) {}

    bool check() override;

    ErrorOr<nebula::cpp2::ErrorCode, std::vector<AdminSubTask>> genSubTasks() override;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_INGESTTASK_H_
