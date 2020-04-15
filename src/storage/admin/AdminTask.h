/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_ADMIN_ADMINTASK_H_
#define STORAGE_ADMIN_ADMINTASK_H_

#include "storage/admin/TaskUtils.h"
#include "storage/BaseProcessor.h"
#include "interface/gen-cpp2/storage_types.h"
#include "kvstore/Common.h"
#include "kvstore/NebulaStore.h"
#include "thrift/ThriftTypes.h"
#include <folly/AtomicHashMap.h>
#include <folly/executors/task_queue/UnboundedBlockingQueue.h>

namespace nebula {
namespace storage {

class AdminSubTask {
public:
    AdminSubTask() = default;

    AdminSubTask(std::function<cpp2::ErrorCode()> f, int32_t subTaskID)
    : run_(f), subTaskID_(subTaskID) {}

    AdminSubTask(std::function<kvstore::ResultCode()> f, int32_t subTaskID) {
        run_ = [f = f]() {
            return toStorageErr(f());
        };
        subTaskID_ = subTaskID;
    }

    cpp2::ErrorCode invoke() {
        return run_();
    }

    int32_t getSubTaskID() {
        return subTaskID_;
    }

private:
    std::function<cpp2::ErrorCode()> run_;
    int32_t                          subTaskID_;
};

enum class TaskPriority : int8_t {
    LO,
    MID,
    HI
};

struct TaskContext {
    using CallBack = std::function<void(cpp2::ErrorCode)>;

    TaskContext() = default;
    TaskContext(const cpp2::AddAdminTaskRequest& req,
                CallBack cb)
            : cmd_(req.get_cmd())
            , jobId_(req.get_job_id())
            , taskId_(req.get_task_id())
            , spaceId_(req.get_para().get_space_id())
            , parts_(req.get_para().parts)
            , parameters_(req.get_para())
            , onFinish_(cb) {}

    nebula::meta::cpp2::AdminCmd    cmd_;
    int32_t                         jobId_{-1};
    int32_t                         taskId_{-1};
    GraphSpaceID                    spaceId_{-1};
    std::vector<PartitionID>        parts_;
    nebula::storage::cpp2::TaskPara parameters_;
    TaskPriority                    pri_{TaskPriority::MID};
    CallBack                        onFinish_;
    size_t                          concurrentReq_{INT_MAX};
};

class AdminTask {
    using TCallBack = std::function<void(cpp2::ErrorCode)>;
    using SubTaskQueue = folly::UnboundedBlockingQueue<AdminSubTask>;

public:
    AdminTask() = default;

    explicit AdminTask(StorageEnv* env, TaskContext&& ctx)
    : env_(env), ctx_(ctx) {}

    virtual ErrorOr<cpp2::ErrorCode, std::vector<AdminSubTask>> genSubTasks() = 0;

    virtual ~AdminTask() {}

    virtual void setCallback(TCallBack cb) {
        ctx_.onFinish_ = cb;
    }

    virtual int8_t getPriority() {
        return static_cast<int8_t>(ctx_.pri_);
    }

    virtual void finish() {
        finish(rc_);
    }

    virtual void finish(cpp2::ErrorCode rc) {
        FLOG_INFO("task(%d, %d) finished, rc=[%d]", ctx_.jobId_, ctx_.taskId_,
                  static_cast<int>(rc));
        ctx_.onFinish_(rc);
    }

    virtual int getJobId() {
        return ctx_.jobId_;;
    }

    virtual int getTaskId() {
        return ctx_.taskId_;
    }

    virtual void setConcurrentReq(int concurrenctReq) {
        if (concurrenctReq > 0) {
            ctx_.concurrentReq_ = concurrenctReq;
        }
    }

    virtual size_t getConcurrentReq() {
        return ctx_.concurrentReq_;
    }

    virtual cpp2::ErrorCode status() const {
        return rc_;
    }

    virtual void subTaskFinish(cpp2::ErrorCode rc) {
        static cpp2::ErrorCode suc{cpp2::ErrorCode::SUCCEEDED};
        rc_.compare_exchange_strong(suc, rc);
    }

    virtual void cancel() {
        FLOG_INFO("task(%d, %d) cancelled", ctx_.jobId_, ctx_.taskId_);
        static cpp2::ErrorCode suc{cpp2::ErrorCode::SUCCEEDED};
        rc_.compare_exchange_strong(suc, cpp2::ErrorCode::E_USER_CANCEL);
    }

    kvstore::ResultCode saveJobStatus(GraphSpaceID spaceId,
                                      PartitionID partId,
                                      std::vector<kvstore::KV> data);

public:
    std::atomic<size_t>                                     unFinishedSubTask_;
    SubTaskQueue                                            subtasks_;
    folly::ConcurrentHashMap<int32_t, cpp2::ErrorCode>*         subTaskStatus_;

protected:
    StorageEnv*                     env_;
    TaskContext                     ctx_;
    std::atomic<cpp2::ErrorCode>    rc_{cpp2::ErrorCode::SUCCEEDED};
};

class AdminTaskFactory {
public:
    static std::shared_ptr<AdminTask> createAdminTask(StorageEnv* env,
                                                      TaskContext&& ctx);
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_ADMIN_ADMINTASK_H_
