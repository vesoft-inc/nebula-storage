/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "storage/admin/AdminTask.h"
#include "storage/admin/AdminTaskManager.h"

DEFINE_uint32(max_task_concurrency, 10, "The tasks number could be invoked simultaneously");
DEFINE_uint32(max_concurrent_subtasks, 10, "The sub tasks could be invoked simultaneously");

namespace nebula {
namespace storage {

bool AdminTaskManager::init() {
    LOG(INFO) << "max concurrenct subtasks: " << FLAGS_max_concurrent_subtasks;
    pool_ = std::make_unique<ThreadPool>(FLAGS_max_concurrent_subtasks);
    bgThread_ = std::make_unique<thread::GenericWorker>();
    if (!bgThread_->start()) {
        return false;
    }

    bgThread_->addTask(&AdminTaskManager::schedule, this);
    shutdown_ = false;
    LOG(INFO) << "exit AdminTaskManager::init()";
    return true;
}

void AdminTaskManager::addAsyncTask(std::shared_ptr<AdminTask> task) {
    TaskHandle handle = std::make_pair(task->getJobId(), task->getTaskId());
    tasks_.insert(handle, task);
    taskQueue_.add(handle);
    LOG(INFO) << folly::stringPrintf("enqueue task(%d, %d), con req=%zu",
                                     task->getJobId(), task->getTaskId(),
                                     task->getConcurrentReq());
}

cpp2::ErrorCode AdminTaskManager::cancelJob(int jobId) {
    auto ret = cpp2::ErrorCode::E_KEY_NOT_FOUND;
    auto it = tasks_.begin();
    while (it != tasks_.end()) {
        auto handle = it->first;
        if (handle.first == jobId) {
            it->second->cancel();
            FLOG_INFO("task(%d, %d) cancelled", jobId, handle.second);
            ret = cpp2::ErrorCode::SUCCEEDED;
        }
        ++it;
    }
    return ret;
}

cpp2::ErrorCode AdminTaskManager::cancelTask(int jobId, int taskId) {
    if (taskId < 0) {
        return cancelJob(jobId);
    }
    auto ret = cpp2::ErrorCode::SUCCEEDED;
    TaskHandle handle = std::make_pair(jobId, taskId);
    auto it = tasks_.find(handle);
    if (it == tasks_.cend()) {
        ret = cpp2::ErrorCode::E_KEY_NOT_FOUND;
    } else {
        it->second->cancel();
    }
    return ret;
}

void AdminTaskManager::shutdown() {
    LOG(INFO) << "enter AdminTaskManager::shutdown()";
    shutdown_ = true;
    bgThread_->stop();
    bgThread_->wait();

    for (auto it = tasks_.begin(); it != tasks_.end(); ++it) {
        it->second->cancel();  // cancelled_ = true;
    }

    pool_->join();

    LOG(INFO) << "exit AdminTaskManager::shutdown()";
}

// schedule
void AdminTaskManager::schedule() {
    std::chrono::milliseconds interval{20};    // 20ms
    while (!shutdown_) {
        LOG(INFO) << "waiting for incoming task";
        folly::Optional<TaskHandle> optTaskHandle{folly::none};
        while (!optTaskHandle && !shutdown_) {
            optTaskHandle = taskQueue_.try_take_for(interval);
        }

        if (shutdown_) {
            LOG(INFO) << "detect AdminTaskManager::shutdown()";
            return;
        }

        auto handle = *optTaskHandle;
        LOG(INFO) << folly::stringPrintf("dequeue task(%d, %d)",
                                         handle.first, handle.second);
        auto it = tasks_.find(handle);
        if (it == tasks_.end()) {
            LOG(ERROR) << folly::stringPrintf(
                        "trying to exec non-exist task(%d, %d)",
                        handle.first, handle.second);
            continue;
        }

        auto task = it->second;
        auto errOrSubTasks = task->genSubTasks();
        if (!nebula::ok(errOrSubTasks)) {
            LOG(ERROR) << "genSubTasks() failed=" << static_cast<int>(nebula::error(errOrSubTasks));
            task->finish(nebula::error(errOrSubTasks));
            tasks_.erase(handle);
            return;
        }

        auto subTasks = nebula::value(errOrSubTasks);
        auto size = subTasks.size();
        task->subTaskStatus_ = new folly::ConcurrentHashMap<int32_t, cpp2::ErrorCode>(size);
        for (auto& subtask : subTasks) {
            task->subtasks_.add(subtask);
        }

        auto subTaskConcurrency = std::min(task->getConcurrentReq(),
                                           static_cast<size_t>(FLAGS_max_concurrent_subtasks));
        subTaskConcurrency = std::min(subTaskConcurrency, size);
        task->unFinishedSubTask_ = size;

        FLOG_INFO("run task(%d, %d), %zu subtasks in %zu thread",
                  handle.first, handle.second,
                  task->unFinishedSubTask_.load(),
                  subTaskConcurrency);
        for (size_t i = 0; i < subTaskConcurrency; ++i) {
            pool_->add(std::bind(&AdminTaskManager::runSubTask, this, handle));
        }
    }  // end while (!shutdown_)
    LOG(INFO) << "AdminTaskManager::pickTaskThread(~)";
}

void AdminTaskManager::runSubTask(TaskHandle handle) {
    auto it = tasks_.find(handle);
    if (it == tasks_.cend()) {
        FLOG_INFO("task(%d, %d) runSubTask() exit", handle.first, handle.second);
        return;
    }
    auto task = it->second;
    std::chrono::milliseconds take_dura{10};
    if (auto subTask = task->subtasks_.try_take_for(take_dura)) {
        if (task->status() == cpp2::ErrorCode::SUCCEEDED) {
            auto rc = subTask->invoke();
            task->subTaskFinish(rc);
        }

        if (0 == --task->unFinishedSubTask_) {
            FLOG_INFO("task(%d, %d) finished", task->getJobId(), task->getTaskId());
            task->finish();
            tasks_.erase(handle);
        } else {
            LOG(INFO) << "Run Task!";
            pool_->add(std::bind(&AdminTaskManager::runSubTask, this, handle));
        }
    } else {
        FLOG_INFO("task(%d, %d) runSubTask() exit", handle.first, handle.second);
    }
}

bool AdminTaskManager::isFinished(int jobID, int taskID) {
    auto tasks = tasks_.find(std::make_pair(jobID, taskID))->second;
    for (size_t i = 0; i < tasks->subTaskStatus_->size(); i++) {
        if (tasks->subTaskStatus_->find(i)->second == cpp2::ErrorCode::SUCCEEDED) {
            return true;
        }
    }
    return false;
}

}  // namespace storage
}  // namespace nebula
