/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <stdexcept>
#include <thrift/lib/cpp/util/EnumUtils.h>
#include <boost/stacktrace.hpp>
#include "meta/common/MetaCommon.h"
#include "meta/processors/Common.h"
#include <folly/String.h>
#include "meta/MetaServiceUtils.h"
#include "meta/processors/jobMan/JobDescription.h"
#include "kvstore/KVIterator.h"

namespace nebula {
namespace meta {

int32_t JobDescription::minDataVer_ = 1;
int32_t JobDescription::currDataVer_ = 1;

JobDescription::JobDescription(JobID id,
                               cpp2::AdminCmd cmd,
                               std::vector<std::string> paras,
                               cpp2::JobStatus status,
                               Timestamp startTime,
                               Timestamp stopTime)
                               : id_(id),
                                 cmd_(cmd),
                                 paras_(std::move(paras)),
                                 status_(status),
                                 startTime_(startTime),
                                 stopTime_(stopTime) {}

ErrorOr<nebula::cpp2::ErrorCode, JobDescription>
JobDescription::makeJobDescription(folly::StringPiece rawkey,
                                   folly::StringPiece rawval) {
    try {
        if (!MetaServiceUtils::isJobKey(rawkey)) {
            return nebula::cpp2::ErrorCode::E_INVALID_JOB;
        }
        auto key = MetaServiceUtils::parseJobKey(rawkey);

        if (!isSupportedValue(rawval)) {
            LOG(ERROR) << "not supported data ver of job " << key;
            return nebula::cpp2::ErrorCode::E_INVALID_JOB;
        }
        auto tup = MetaServiceUtils::parseJobValue(rawval);
        auto cmd = std::get<0>(tup);
        auto paras = std::get<1>(tup);
        for (const auto& p : paras) {
            LOG(INFO) << "p = " << p;
        }
        auto status = std::get<2>(tup);
        auto startTime = std::get<3>(tup);
        auto stopTime = std::get<4>(tup);
        return JobDescription(key, cmd, paras, status, startTime, stopTime);
    } catch(std::exception& ex) {
        LOG(ERROR) << ex.what();
    }
    return nebula::cpp2::ErrorCode::E_INVALID_JOB;
}

std::string JobDescription::jobKey() const {
    return MetaServiceUtils::jobKey(id_);
}

std::string JobDescription::jobVal() const {
    return MetaServiceUtils::jobVal(cmd_, paras_, status_, startTime_,
                                    stopTime_, currDataVer_);
}

cpp2::JobDesc JobDescription::toJobDesc() {
    cpp2::JobDesc ret;
    ret.set_id(id_);
    ret.set_cmd(cmd_);
    ret.set_paras(paras_);
    ret.set_status(status_);
    ret.set_start_time(startTime_);
    ret.set_stop_time(stopTime_);
    return ret;
}

bool JobDescription::setStatus(cpp2::JobStatus newStatus) {
    if (JobStatus::laterThan(status_, newStatus)) {
        return false;
    }
    status_ = newStatus;
    if (newStatus == Status::RUNNING) {
        startTime_ = std::time(nullptr);
    }
    if (JobStatus::laterThan(newStatus, Status::RUNNING)) {
        stopTime_ = std::time(nullptr);
    }
    return true;
}

ErrorOr<nebula::cpp2::ErrorCode, JobDescription>
JobDescription::loadJobDescription(JobID iJob, nebula::kvstore::KVStore* kv) {
    auto key = MetaServiceUtils::jobKey(iJob);
    std::string val;
    auto retCode = kv->get(kDefaultSpaceId, kDefaultPartId, key, &val);
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Loading Job Description Failed"
                   << apache::thrift::util::enumNameSafe(retCode);
        return retCode;
    }
    return makeJobDescription(key, val);
}

bool JobDescription::isSupportedValue(const folly::StringPiece& val) {
    if (val.size() < sizeof(int32_t)) {
        return false;
    }

    auto v = *reinterpret_cast<const int32_t*>(val.data());
    int32_t ver = INT_MAX - v;
    return ver >= minDataVer_ && ver <= currDataVer_;
}

}  // namespace meta
}  // namespace nebula

