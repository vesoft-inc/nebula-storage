/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_JOB_BALANCETASK_H_
#define META_JOB_BALANCETASK_H_

#include "meta/processors/Common.h"
#include "meta/processors/admin/AdminClient.h"

namespace nebula {
namespace meta {

class BalanceTask {
    friend class BalancePlan;
    friend class BalanceJobExecutor;
    FRIEND_TEST(BalanceDataTest, BalanceTaskTest);
    FRIEND_TEST(BalanceDataTest, DispatchTasksTest);
    FRIEND_TEST(BalanceDataTest, BalancePlanTest);

public:
    BalanceTask() = default;

    BalanceTask(JobID jobId,
                TaskID taskId,
                GraphSpaceID spaceId,
                PartitionID partId,
                const HostAddr& src,
                const HostAddr& dst,
                kvstore::KVStore* kv,
                AdminClient* client)
        : jobId_(jobId)
        , taskId_(taskId)
        , spaceId_(spaceId)
        , partId_(partId)
        , src_(src)
        , dst_(dst)
        , taskIdStr_(buildTaskId())
        , kv_(kv)
        , client_(client) {}

    const std::string& taskIdStr() const {
        return taskIdStr_;
    }

    void invoke();

    BalanceTaskResult result() const {
        return ret_;
    }

private:
    std::string buildTaskId() {
        return folly::stringPrintf("[%d:%d, %d:%d, %s:%d->%s:%d]",
                                   jobId_,
                                   taskId_,
                                   spaceId_,
                                   partId_,
                                   src_.host.c_str(),
                                   src_.port,
                                   dst_.host.c_str(),
                                   dst_.port);
    }

    bool saveTaskStatus();

public:
    JobID        jobId_;
    TaskID       taskId_;
    GraphSpaceID spaceId_;
    PartitionID  partId_;
    HostAddr     src_;
    HostAddr     dst_;
    std::string  taskIdStr_;
    kvstore::KVStore* kv_ = nullptr;
    AdminClient* client_ = nullptr;
    BalanceTaskStatus status_ = BalanceTaskStatus::START;
    BalanceTaskResult ret_ = BalanceTaskResult::IN_PROGRESS;
    Timestamp startTime_ = 0;
    Timestamp endTime_ = 0;
    std::function<void()> onFinished_;
    std::function<void()> onError_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_JOB_BALANCETASK_H_
