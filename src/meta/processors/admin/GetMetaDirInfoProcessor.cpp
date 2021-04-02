/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/admin/GetMetaDirInfoProcessor.h"
#include "common/fs/FileUtils.h"

namespace nebula {
namespace meta {

void GetMetaDirInfoProcessor::process(const cpp2::GetMetaDirInfoReq& req) {
    UNUSED(req);
    char cwd[PATH_MAX];
    if (getcwd(cwd, sizeof(cwd)) == nullptr) {
        resp_.set_code(cpp2::ErrorCode::E_GET_META_DIR_FAILURE);
        LOG(ERROR) << "Failed to get current dir ";
        onFinished();
        return;
    }

    auto data_root = kvstore_->getDataRoot();

    std::vector<std::string> realpaths;
    bool failed = false;
    std::transform(std::make_move_iterator(data_root.begin()),
                   std::make_move_iterator(data_root.end()),
                   std::back_inserter(realpaths),
                   [&failed](auto f) {
                       if (f[0] == '/') {
                           return f;
                       } else {
                           auto result = nebula::fs::FileUtils::realPath(f.c_str());
                           if (!result.ok()) {
                               failed = true;
                               LOG(ERROR) << "Failed to get the absolute path of file: " << f;
                               return f;
                           }
                           return std::string(result.value());
                       }
                   });
    if (failed) {
        resp_.set_code(cpp2::ErrorCode::E_GET_META_DIR_FAILURE);
        onFinished();
        return;
    }
    resp_.set_data_dir(realpaths);
    resp_.set_root_dir(cwd);

    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    onFinished();
}
}   // namespace meta
}   // namespace nebula
