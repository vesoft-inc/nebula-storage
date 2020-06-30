/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_ADMIN_REBUILDINDEXTASK_H_
#define STORAGE_ADMIN_REBUILDINDEXTASK_H_

#include "kvstore/LogEncoder.h"
#include "common/meta/IndexManager.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "storage/admin/AdminTask.h"

namespace nebula {
namespace storage {

class RebuildIndexTask : public AdminTask {
public:
    explicit RebuildIndexTask(StorageEnv* env, TaskContext&& ctx)
        : AdminTask(env, std::move(ctx)) {}

    ~RebuildIndexTask() {
        LOG(INFO) << "Release Rebuild Task";
        if (env_->rebuildIndexGuard_ != nullptr) {
            env_->rebuildIndexGuard_->assign(std::make_tuple(space_, 0, 0),
                                             IndexState::FINISHED);
        }
    }

    ErrorOr<cpp2::ErrorCode, std::vector<AdminSubTask>> genSubTasks() override;

protected:
    virtual StatusOr<std::shared_ptr<nebula::meta::cpp2::IndexItem>>
    getIndex(GraphSpaceID space, IndexID indexID) = 0;

    virtual kvstore::ResultCode
    buildIndexGlobal(GraphSpaceID space,
                     PartitionID part,
                     meta::cpp2::SchemaID SchemaID,
                     IndexID indexID,
                     const std::vector<meta::cpp2::ColumnDef>& cols) = 0;

    void cancel() override {
        canceled_ = true;
    }

    kvstore::ResultCode buildIndexOnOperations(GraphSpaceID space,
                                               IndexID indexID,
                                               PartitionID part);

    kvstore::ResultCode processModifyOperation(GraphSpaceID space,
                                               PartitionID part,
                                               std::vector<kvstore::KV>&& data);

    kvstore::ResultCode processRemoveOperation(GraphSpaceID space,
                                               PartitionID part,
                                               std::string&& key);

    kvstore::ResultCode cleanupOperationLogs(GraphSpaceID space,
                                             PartitionID part,
                                             std::vector<std::string>&& keys);

    kvstore::ResultCode genSubTask(GraphSpaceID space,
                                   PartitionID part,
                                   meta::cpp2::SchemaID schemaID,
                                   IndexID indexID,
                                   const std::shared_ptr<meta::cpp2::IndexItem> item,
                                   bool isOffline);

protected:
    std::atomic<bool>   canceled_{false};
    GraphSpaceID        space_;
    std::mutex          buildLock_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_REBUILDINDEXTASK_H_
