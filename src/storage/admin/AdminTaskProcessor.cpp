/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/admin/AdminTaskProcessor.h"
#include "storage/admin/AdminTaskManager.h"
#include "common/interface/gen-cpp2/common_types.h"

namespace nebula {
namespace storage {

void AdminTaskProcessor::process(const cpp2::AddAdminTaskRequest& req) {
    bool runDirectly = true;
    auto rc = cpp2::ErrorCode::SUCCEEDED;
    auto taskManager = AdminTaskManager::instance();

    auto* store = dynamic_cast<kvstore::KVStore*>(env_->kvstore_);
    auto cb = [&](cpp2::ErrorCode ret) {
        if (ret != cpp2::ErrorCode::SUCCEEDED) {
            cpp2::PartitionResult thriftRet;
            thriftRet.set_code(ret);
            codes_.emplace_back(std::move(thriftRet));
        }
        onFinished();
    };
    TaskContext ctx(req, store, cb);
    auto task = AdminTaskFactory::createAdminTask(std::move(ctx));
    if (task) {
        runDirectly = false;
        taskManager->addAsyncTask(task);
    } else {
        rc = cpp2::ErrorCode::E_INVALID_TASK_PARA;
    }

    if (runDirectly) {
        if (rc != cpp2::ErrorCode::SUCCEEDED) {
            cpp2::PartitionResult thriftRet;
            thriftRet.set_code(rc);
            codes_.emplace_back(std::move(thriftRet));
        }
        onFinished();
    }
}

}  // namespace storage
}  // namespace nebula

