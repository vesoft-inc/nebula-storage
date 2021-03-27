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
    auto taskManager = AdminTaskManager::instance();

    auto toMetaErrCode = [](storage::cpp2::ErrorCode storageCode) {
        switch (storageCode) {
            case storage::cpp2::ErrorCode::SUCCEEDED:
                return meta::cpp2::ErrorCode::SUCCEEDED;
            case storage::cpp2::ErrorCode::E_UNKNOWN:
                return meta::cpp2::ErrorCode::E_UNKNOWN;
            default:
                LOG(ERROR) << "unsupported conversion of code "
                           << cpp2::_ErrorCode_VALUES_TO_NAMES.at(storageCode);
                return meta::cpp2::ErrorCode::E_UNKNOWN;
        }
    };

    auto cb = [env = env_, toMetaErrCode = std::move(toMetaErrCode),
               jobId = req.get_job_id(), taskId = req.get_task_id()](
                  nebula::storage::cpp2::ErrorCode errCode,
                  nebula::meta::cpp2::StatisItem& result) {
        meta::cpp2::StatisItem* pStatis = nullptr;
        if (errCode == cpp2::ErrorCode::SUCCEEDED &&
            result.status == nebula::meta::cpp2::JobStatus::FINISHED) {
            pStatis = &result;
        }

        auto metaCode = toMetaErrCode(errCode);
        LOG(INFO) << folly::sformat("reportTaskFinish(), job={}, task={}, rc={}",
                                    jobId,
                                    taskId,
                                    meta::cpp2::_ErrorCode_VALUES_TO_NAMES.at(metaCode));
        auto maxRetry = 5;
        auto retry = 0;
        while (retry++ < maxRetry) {
            auto rc = meta::cpp2::ErrorCode::SUCCEEDED;
            auto fut = env->metaClient_->reportTaskFinish(jobId, taskId, metaCode, pStatis);
            fut.wait();
            if (!fut.hasValue()) {
                LOG(INFO) << folly::sformat(
                    "reportTaskFinish() got rpc error:, job={}, task={}",
                    jobId,
                    taskId);
                continue;
            }
            if (!fut.value().ok()) {
                LOG(INFO) << folly::sformat(
                    "reportTaskFinish() has bad status:, job={}, task={}, rc={}",
                    jobId,
                    taskId,
                    fut.value().status().toString());
                break;
            }
            rc = fut.value().value();
            LOG(INFO) << folly::sformat("reportTaskFinish(), job={}, task={}, rc={}",
                                        jobId,
                                        taskId,
                                        meta::cpp2::_ErrorCode_VALUES_TO_NAMES.at(rc));
            if (rc == meta::cpp2::ErrorCode::E_LEADER_CHANGED ||
                rc == meta::cpp2::ErrorCode::E_STORE_FAILURE) {
                continue;
            } else {
                break;
            }
        }
    };

    TaskContext ctx(req, std::move(cb));
    auto task = AdminTaskFactory::createAdminTask(env_, std::move(ctx));
    if (task) {
        taskManager->addAsyncTask(task);
    } else {
        cpp2::PartitionResult thriftRet;
        thriftRet.set_code(cpp2::ErrorCode::E_INVALID_TASK_PARA);
        codes_.emplace_back(std::move(thriftRet));
    }
    onFinished();
}

void AdminTaskProcessor::onProcessFinished(nebula::meta::cpp2::StatisItem& result) {
    resp_.set_statis(std::move(result));
}

}  // namespace storage
}  // namespace nebula

