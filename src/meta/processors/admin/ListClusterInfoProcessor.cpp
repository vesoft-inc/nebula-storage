/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/admin/ListClusterInfoProcessor.h"

namespace nebula {
namespace meta {

void ListClusterInfoProcessor::process(const cpp2::ListClusterInfoReq& req) {
    UNUSED(req);
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

    auto* pm = store->partManager();
    auto* mpm = dynamic_cast<nebula::kvstore::MemPartManager*>(pm);
    if (mpm == nullptr) {
        resp_.set_code(cpp2::ErrorCode::E_LIST_CLUSTER_FAILURE);
        onFinished();
        return;
    }
    auto& map = mpm->partsMap();
    resp_.set_meta_servers(
        std::move(map[nebula::meta::kDefaultSpaceId][nebula::meta::kDefaultPartId].hosts_));

    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_storage_servers(std::move(storages));
    onFinished();
}
}   // namespace meta
}   // namespace nebula
