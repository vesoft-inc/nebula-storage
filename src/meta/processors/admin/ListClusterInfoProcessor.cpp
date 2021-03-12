/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/admin/ListClusterInfoProcessor.h"

namespace nebula {
namespace meta {

void ListClusterInfoProcessor::process(const cpp2::ListClusterInfoReq& req) {
    auto* store = dynamic_cast<kvstore::NebulaStore*>(kvstore_);
    if (store == nullptr) {
        onFinished();
        return;
    }

    if (!store->isLeader(kDefaultSpaceId, kDefaultPartId)) {
        handleErrorCode(cpp2::ErrorCode::E_LEADER_CHANGED);
        onFinished();
        return;
    }

    if (req.get_spaces() != nullptr) {
        onFinished();
    }

    const auto& prefix = MetaServiceUtils::hostPrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        handleErrorCode(cpp2::ErrorCode::E_LIST_CLUSTER_FAILURE);
        onFinished();
        return;
    }

    std::vector<nebula::cpp2::NodeInfo> storages;
    while (iter->valid()) {
        auto host = MetaServiceUtils::parseHostKey(iter->key());
        auto status = client_->listClusterInfo(host).get();
        if (!status.ok()) {
            handleErrorCode(cpp2::ErrorCode::E_LIST_CLUSTER_FAILURE);
            onFinished();
            return;
        }

        storages.emplace_back(apache::thrift::FragileConstructor(),
                              std::move(host),
                              status.value().first,
                              status.value().second);
        iter->next();
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
                           char* p = realpath(f.c_str(), nullptr);
                           if (p == nullptr) {
                               failed = true;
                               LOG(ERROR) << "Failed to get the absolute path of file: " << f;
                               return f;
                           }
                           return std::string(p);
                       }
                   });
    if (failed) {
        handleErrorCode(cpp2::ErrorCode::E_LIST_CLUSTER_GET_ABS_PATH_FAILURE);
        onFinished();
        return;
    }
    char cwd[PATH_MAX];
    if (getcwd(cwd, sizeof(cwd)) == nullptr) {
        handleErrorCode(cpp2::ErrorCode::E_LIST_CLUSTER_GET_ABS_PATH_FAILURE);
        LOG(ERROR) << "Failed to get current dir ";
        onFinished();
        return;
    }

    nebula::cpp2::NodeInfo meta;
    meta.set_data_dir(realpaths);
    meta.set_root_dir(cwd);
    meta.set_host(store->address());

    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_meta(std::move(meta));
    resp_.set_storage_cluster(std::move(storages));
    onFinished();
}
}   // namespace meta
}   // namespace nebula
