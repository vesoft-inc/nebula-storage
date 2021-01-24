/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_JOB_BALANCEPLAN_H_
#define META_JOB_BALANCEPLAN_H_

#include "kvstore/KVStore.h"
#include "meta/processors/Common.h"
#include "meta/processors/jobMan/BalanceTask.h"

namespace nebula {
namespace meta {

class BalancePlan {
    friend class BalanceJobExecutor;
    FRIEND_TEST(BalanceDataTest, BalanceTaskTest);
    FRIEND_TEST(BalanceDataTest, DispatchTasksTest);
    FRIEND_TEST(BalanceDataTest, BalancePlanTest);
    FRIEND_TEST(BalanceDataTest, NormalTest);
    FRIEND_TEST(BalanceDataTest, SpecifyHostTest);
    FRIEND_TEST(BalanceDataTest, SpecifyMultiHostTest);
    FRIEND_TEST(BalanceDataTest, MockReplaceMachineTest);
    FRIEND_TEST(BalanceDataTest, SingleReplicaTest);
    FRIEND_TEST(BalanceDataTest, TryToRecoveryTest);
    FRIEND_TEST(BalanceDataTest, RecoveryTest);
    FRIEND_TEST(BalanceDataTest, StopPlanTest);
    FRIEND_TEST(GetBalancePlanTest, BalancePlanJob);

public:
    BalancePlan(JobID id, GraphSpaceID space, kvstore::KVStore* kv, AdminClient* client)
        : id_(id)
        , space_(space)
        , kv_(kv)
        , client_(client) {}

    BalancePlan(const BalancePlan& plan)
        : id_(plan.id_)
        , space_(plan.space_)
        , kv_(plan.kv_)
        , client_(plan.client_)
        , tasks_(plan.tasks_)
        , finishedTaskNum_(plan.finishedTaskNum_)
        , status_(plan.status_) {}

    void addTask(BalanceTask task) {
        tasks_.emplace_back(std::move(task));
    }

    void invoke();

    cpp2::JobStatus status() {
        return status_;
    }

    nebula::cpp2::ErrorCode saveJobStatus();

    JobID id() const {
        return id_;
    }

    const std::vector<BalanceTask>& tasks() const {
        return tasks_;
    }

    int32_t taskSize() const {
        return tasks_.size();
    }

private:
    nebula::cpp2::ErrorCode recovery();

    void dispatchTasks();

private:
    JobID id_ = 0;
    GraphSpaceID space_ = 0;
    kvstore::KVStore* kv_ = nullptr;
    AdminClient* client_ = nullptr;
    std::vector<BalanceTask> tasks_;
    size_t finishedTaskNum_ = 0;
    std::function<void()> onFinished_;
    cpp2::JobStatus status_ = cpp2::JobStatus::QUEUE;

    // List of task index in tasks_;
    using Bucket = std::vector<int32_t>;
    std::vector<Bucket> buckets_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_JOB_BALANCEPLAN_H_
