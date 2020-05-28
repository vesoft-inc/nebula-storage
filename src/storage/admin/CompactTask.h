/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_ADMIN_COMPACTTASK_H_
#define STORAGE_ADMIN_COMPACTTASK_H_

#include "thrift/ThriftTypes.h"
#include "kvstore/KVEngine.h"
#include "kvstore/NebulaStore.h"
#include "storage/admin/AdminTask.h"

namespace nebula {
namespace storage {

class CompactTask : public AdminTask {
public:
    CompactTask(StorageEnv* env, TaskContext&& ctx)
    : AdminTask(env, std::move(ctx)) {}

    ErrorOr<cpp2::ErrorCode, std::vector<AdminSubTask>> genSubTasks() override;
    kvstore::ResultCode subTask(nebula::kvstore::KVEngine* engine);
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_COMPACTTASK_H_
