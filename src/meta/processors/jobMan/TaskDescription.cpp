/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/jobMan/TaskDescription.h"
#include "common/time/WallClock.h"
#include "meta/MetaServiceUtils.h"
#include <folly/String.h>

namespace nebula {
namespace meta {

TaskDescription::TaskDescription(JobID iJob, TaskID iTask, const HostAddr& dst)
                : TaskDescription(iJob, iTask, dst.host, dst.port) {}

TaskDescription::TaskDescription(JobID iJob, TaskID iTask, std::string addr, int32_t port)
                : iJob_(iJob)
                , iTask_(iTask)
                , dest_(addr, port)
                , status_(cpp2::JobStatus::RUNNING)
                , startTime_(std::time(nullptr))
                , stopTime_(0) {}

TaskDescription::TaskDescription(const folly::StringPiece& key,
                                 const folly::StringPiece& val) {
    auto [jobId, taskId] = MetaServiceUtils::parseTaskKey(key);
    iJob_ = jobId;
    iTask_ = taskId;

    auto [dest, status, startTime, endTime] = MetaServiceUtils::parseTaskValue(val);
    dest_ = dest;
    status_ = status;
    startTime_ = startTime;
    stopTime_ = endTime;
}

std::string TaskDescription::taskKey() {
    return MetaServiceUtils::taskKey(iJob_, iTask_);
}

std::string TaskDescription::taskVal() {
    return MetaServiceUtils::taskVal(dest_, status_, startTime_, stopTime_);
}

/*
 * =====================================================================================
 * | Job Id(TaskId) | Command(Dest) | Status   | Start Time        | Stop Time         |
 * =====================================================================================
 * | 27-0           | 192.168.8.5   | finished | 12/09/19 11:09:40 | 12/09/19 11:09:40 |
 * -------------------------------------------------------------------------------------
 * */
cpp2::TaskDesc TaskDescription::toTaskDesc() {
    cpp2::TaskDesc ret;
    ret.set_job_id(iJob_);
    ret.set_task_id(iTask_);
    ret.set_host(dest_);
    ret.set_status(status_);
    ret.set_start_time(startTime_);
    ret.set_stop_time(stopTime_);
    return ret;
}

bool TaskDescription::setStatus(cpp2::JobStatus newStatus) {
    if (JobStatus::laterThan(status_, newStatus)) {
        return false;
    }
    status_ = newStatus;
    if (newStatus == cpp2::JobStatus::RUNNING) {
        startTime_ = nebula::time::WallClock::fastNowInSec();
    }

    if (JobStatus::laterThan(newStatus, cpp2::JobStatus::RUNNING)) {
        stopTime_ = nebula::time::WallClock::fastNowInSec();
    }
    return true;
}

}  // namespace meta
}  // namespace nebula

