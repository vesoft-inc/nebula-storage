/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/fs/FileUtils.h"
#include "storage/admin/DownloadTask.h"

namespace nebula {
namespace storage {

bool DownloadTask::check() {
    return env_->kvstore_ != nullptr;
}

ErrorOr<nebula::cpp2::ErrorCode, std::vector<AdminSubTask>>
DownloadTask::genSubTasks() {
    auto space = *ctx_.parameters_.space_id_ref();
    auto parts = *ctx_.parameters_.parts_ref();
    auto paras = ctx_.parameters_.task_specfic_paras_ref();
    if (!paras.has_value() || paras->size() != 3) {
        LOG(ERROR) << "Download Task should be three parameters";
        return nebula::cpp2::ErrorCode::E_INVALID_PARM;
    }

    hdfsHost_ = (*paras)[0];
    hdfsPort_ = folly::to<int32_t>((*paras)[1]);
    hdfsPath_ = (*paras)[2];

    std::vector<AdminSubTask> tasks;
    for (const auto& part : parts) {
        TaskFunction task = std::bind(&DownloadTask::invoke, this, space, part);
        tasks.emplace_back(std::move(task));
    }
    return tasks;
}

nebula::cpp2::ErrorCode
DownloadTask::invoke(GraphSpaceID space, PartitionID part) {
    LOG(INFO) << "Space: " << space << " Part: " << part;
    auto hdfsPartPath = folly::stringPrintf("%s/%d", hdfsPath_.c_str(), part);
    auto partResult = env_->kvstore_->part(space, part);
    if (!ok(partResult)) {
        LOG(ERROR) << "Can't found space: " << space << ", part: " << part;
        return nebula::cpp2::ErrorCode::E_PART_NOT_FOUND;
    }

    auto localPath = folly::stringPrintf("%s/download/",
                                         value(partResult)->engine()->getDataRoot());

    if (fs::FileUtils::fileType(localPath.c_str()) == fs::FileType::NOTEXIST) {
        fs::FileUtils::makeDir(localPath);
    }
    auto result = helper_->copyToLocal(hdfsHost_, hdfsPort_,
                                       hdfsPartPath, localPath);

    if (result.ok()) {
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    } else {
        return nebula::cpp2::ErrorCode::E_TASK_EXECUTION_FAILED;
    }
}

}  // namespace storage
}  // namespace nebula
