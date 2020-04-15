/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_ADMIN_REBUILDEDGEINDEXTASK_H_
#define STORAGE_ADMIN_REBUILDEDGEINDEXTASK_H_

#include "storage/admin/RebuildIndexTask.h"

namespace nebula {
namespace storage {

class RebuildEdgeIndexTask : public RebuildIndexTask {
public:
    explicit RebuildEdgeIndexTask(StorageEnv* env,
                                  TaskContext&& ctx)
    : RebuildIndexTask(env, std::move(ctx)) {}

private:
    StatusOr<std::shared_ptr<nebula::meta::cpp2::IndexItem>>
    getIndex(GraphSpaceID space, IndexID indexID) override;

    kvstore::ResultCode buildIndexGlobal(GraphSpaceID space,
                                         PartitionID part,
                                         meta::cpp2::SchemaID SchemaID,
                                         IndexID indexID,
                                         const std::vector<meta::cpp2::ColumnDef>& cols) override;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_REBUILDEDGEINDEXTASK_H_
