/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/common/MetaCommon.h"
#include "meta/ActiveHostsMan.h"
#include "meta/MetaServiceUtils.h"
#include "meta/processors/jobMan/BalancePlan.h"
#include "meta/processors/jobMan/JobDescription.h"
#include <thrift/lib/cpp/util/EnumUtils.h>

DEFINE_uint32(task_concurrency, 10, "The tasks number could be invoked simultaneously");

namespace nebula {
namespace meta {

void BalancePlan::dispatchTasks() {
    // Key -> spaceID + partID,  Val -> List of task index in tasks_;
    std::unordered_map<std::pair<GraphSpaceID, PartitionID>, std::vector<int32_t>> partTasks;
    int32_t index = 0;
    for (auto& task : tasks_) {
        partTasks[std::make_pair(task.spaceId_, task.partId_)].emplace_back(index++);
    }
    buckets_.resize(std::min(partTasks.size(), (size_t)FLAGS_task_concurrency));
    for (auto it = partTasks.begin(); it != partTasks.end(); it++) {
        size_t minNum = tasks_.size();
        int32_t i = 0, minIndex = 0;
        for (auto& bucket : buckets_) {
            if (bucket.size() < minNum) {
                minNum = bucket.size();
                minIndex = i;
            }
            i++;
        }
        for (auto taskIndex : it->second) {
            buckets_[minIndex].emplace_back(taskIndex);
        }
    }
}

void BalancePlan::invoke() {
    status_ = cpp2::JobStatus::RUNNING;
    // Sort the tasks by its id to ensure the order after recovery.
    std::sort(tasks_.begin(), tasks_.end(), [](auto& l, auto& r) {
        return l.taskIdStr() < r.taskIdStr();
    });
    dispatchTasks();
    for (size_t i = 0; i < buckets_.size(); i++) {
        for (size_t j = 0; j < buckets_[i].size(); j++) {
            auto taskIndex = buckets_[i][j];
            tasks_[taskIndex].onFinished_ = [this, i, j]() {
                bool finished = false;
                {
                    finishedTaskNum_++;
                    VLOG(1) << "Balance " << id_ << " has completed "
                            << finishedTaskNum_ << " task";
                    if (finishedTaskNum_ == tasks_.size()) {
                        finished = true;
                        if (status_ == cpp2::JobStatus::RUNNING) {
                            status_ = cpp2::JobStatus::FINISHED;
                            LOG(INFO) << "Balance " << id_ << " succeeded!";
                        }
                    }
                }
                if (finished) {
                    CHECK_EQ(j, this->buckets_[i].size() - 1);
                    saveJobStatus();
                    onFinished_();
                } else if (j + 1 < this->buckets_[i].size()) {
                    auto& task = this->tasks_[this->buckets_[i][j + 1]];
                    task.invoke();
                }
            };  // onFinished

            tasks_[taskIndex].onError_ = [this, i, j, taskIndex]() {
                bool finished = false;
                {
                    finishedTaskNum_++;
                    VLOG(1) << "Balance " << id_ << " has completed "
                            << finishedTaskNum_ << " task";
                    status_ = cpp2::JobStatus::FAILED;
                    if (finishedTaskNum_ == tasks_.size()) {
                        finished = true;
                        LOG(INFO) << "Balance " << id_ << " failed!";
                    }
                }

                if (finished) {
                    CHECK_EQ(j, this->buckets_[i].size() - 1);
                    saveJobStatus();
                    onFinished_();
                } else if (j + 1 < this->buckets_[i].size()) {
                    auto& task = this->tasks_[this->buckets_[i][j + 1]];
                    if (tasks_[taskIndex].spaceId_ == task.spaceId_ &&
                        tasks_[taskIndex].partId_ == task.partId_) {
                        LOG(INFO) << "Skip the task for the same partId " << task.partId_;
                        task.ret_ = BalanceTaskResult::FAILED;
                    }
                    task.invoke();
                }
            };  // onError
        }  // for (auto j = 0; j < buckets_[i].size(); j++)
    }  // for (auto i = 0; i < buckets_.size(); i++)

    saveJobStatus();
    for (auto& bucket : buckets_) {
        if (!bucket.empty()) {
            tasks_[bucket[0]].invoke();
        }
    }
}

nebula::cpp2::ErrorCode BalancePlan::saveJobStatus() {
    CHECK_NOTNULL(kv_);
    std::vector<kvstore::KV> data;
    data.emplace_back(MetaServiceUtils::balancePlanKey(space_),
                      MetaServiceUtils::balancePlanVal(status_, tasks_));

    auto jobDescRet = JobDescription::loadJobDescription(id_, kv_);
    if (!nebula::ok(jobDescRet)) {
        LOG(ERROR) << "Can't load the job " << id_;
        return nebula::cpp2::ErrorCode::E_JOB_NOT_FOUND;
    }

    auto jobDesc = nebula::value(jobDescRet);
    jobDesc.setStatus(status_);
    data.emplace_back(jobDesc.jobKey(), jobDesc.jobVal());

    folly::Baton<true, std::atomic> baton;
    auto ret = nebula::cpp2::ErrorCode::SUCCEEDED;
    kv_->asyncMultiPut(kDefaultSpaceId,
                       kDefaultPartId,
                       std::move(data),
                       [&baton, &ret] (nebula::cpp2::ErrorCode code) {
        if (nebula::cpp2::ErrorCode::SUCCEEDED != code) {
            ret = code;
            LOG(ERROR) << "Can't write the kvstore, ret = "
                       << apache::thrift::util::enumNameSafe(code);
        }
        baton.post();
    });
    baton.wait();
    return ret;
}

nebula::cpp2::ErrorCode BalancePlan::recovery() {
    CHECK_NOTNULL(kv_);
    const auto& prefix = MetaServiceUtils::balanceTaskPrefix(id_);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kv_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Can't access kvstore, ret = "
                   << apache::thrift::util::enumNameSafe(ret);
        return ret;
    }

    while (iter->valid()) {
        BalanceTask task;
        task.kv_ = kv_;
        task.client_ = client_;
        {
            auto tup = MetaServiceUtils::parseBalanceTaskKey(iter->key());
            task.jobId_ = std::get<0>(tup);
            task.taskId_ = std::get<1>(tup);
            task.spaceId_ = std::get<2>(tup);
            task.partId_ = std::get<3>(tup);
            task.src_ = std::get<4>(tup);
            task.dst_ = std::get<5>(tup);
            task.taskIdStr_ = task.buildTaskId();
        }
        {
            auto tup = MetaServiceUtils::parseBalanceTaskVal(iter->val());
            task.status_ = std::get<0>(tup);
            task.ret_ = std::get<1>(tup);
            task.startTime_ = std::get<2>(tup);
            task.endTime_ = std::get<3>(tup);
            if (task.ret_ != BalanceTaskResult::SUCCEEDED) {
                // Resume the failed task, skip the in-progress and invalid tasks
                if (task.ret_ == BalanceTaskResult::FAILED) {
                    task.ret_ = BalanceTaskResult::IN_PROGRESS;
                }
                task.status_ = BalanceTaskStatus::START;
                auto activeHostRet = ActiveHostsMan::isLived(kv_, task.dst_);
                if (!nebula::ok(activeHostRet)) {
                    auto retCode = nebula::error(activeHostRet);
                    LOG(ERROR) << "Get active hosts failed, error: "
                               << static_cast<int32_t>(retCode);
                    return retCode;
                } else {
                    auto isLive = nebula::value(activeHostRet);
                    if (!isLive) {
                        LOG(ERROR) << "The destination is not lived";
                        task.ret_ = BalanceTaskResult::INVALID;
                    }
                }
            }
        }
        tasks_.emplace_back(std::move(task));
        iter->next();
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace meta
}  // namespace nebula
