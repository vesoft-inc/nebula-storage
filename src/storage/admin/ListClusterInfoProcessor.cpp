/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/admin/ListClusterInfoProcessor.h"

namespace nebula {
namespace storage {

void ListClusterInfoProcessor::process(const cpp2::ListClusterInfoReq& req) {
    UNUSED(req);
    CHECK_NOTNULL(env_);

    auto data_root = env_->kvstore_->getDataRoot();

    std::vector<std::string> realpaths;
    bool failed = false;
    std::transform(std::make_move_iterator(data_root.begin()),
                   std::make_move_iterator(data_root.end()),
                   std::back_inserter(realpaths),
                   [&failed](auto f) {
                       if (f[0] == '/') {
                           return f;
                       }
                       char* p = realpath(f.c_str(), nullptr);
                       if (p == nullptr) {
                           LOG(ERROR) << "Failed to get the absolute path of file: " << f;
                           failed = true;
                           return f;
                       }
                       return std::string(p);
                   });
    if (failed) {
        cpp2::PartitionResult thriftRet;
        thriftRet.set_code(cpp2::ErrorCode::E_FAILED_GET_ABS_PATH);
        codes_.emplace_back(std::move(thriftRet));
        onFinished();
        return;
    }
    resp_.set_data_dir(std::move(realpaths));

    char cwd[PATH_MAX];
    if (getcwd(cwd, sizeof(cwd)) == nullptr) {
        LOG(ERROR) << "Failed to get current dir";
        cpp2::PartitionResult thriftRet;
        thriftRet.set_code(cpp2::ErrorCode::E_FAILED_GET_ABS_PATH);
        codes_.emplace_back(std::move(thriftRet));
        onFinished();
        return;
    }
    resp_.set_root_dir(cwd);

    onFinished();
}

}   // namespace storage
}   // namespace nebula
