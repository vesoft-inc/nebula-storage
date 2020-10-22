/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_ADMIN_STATISTASK_H_
#define STORAGE_ADMIN_STATISTASK_H_

#include "common/thrift/ThriftTypes.h"
#include "kvstore/KVEngine.h"
#include "kvstore/NebulaStore.h"
#include "storage/admin/AdminTask.h"

namespace nebula {
namespace storage {

class StatisTask : public AdminTask {
public:
    StatisTask(StorageEnv* env, TaskContext&& ctx)
        : AdminTask(env, std::move(ctx)) {}

    ~StatisTask() {
        LOG(INFO) << "Release Statis Task";
    }

    ErrorOr<cpp2::ErrorCode, std::vector<AdminSubTask>> genSubTasks() override;

    void finish(cpp2::ErrorCode rc) override;

protected:
    void cancel() override {
        canceled_ = true;
    }

    kvstore::ResultCode genSubTask(GraphSpaceID space,
                                   PartitionID part,
                                   std::vector<TagID> tags,
                                   std::vector<EdgeType> edges);

private:
    cpp2::ErrorCode getSchemas(GraphSpaceID spaceId);

protected:
    std::atomic<bool>                           canceled_{false};
    GraphSpaceID                                spaceId_;
    std::vector<TagID>                          tags_;
    std::vector<EdgeType>                       edges_;
    std::vector<nebula::meta::cpp2::StatisItem> statistics_;
    size_t                                      subTaskSize_{0};
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_ADMIN_STATISTASK_H_
