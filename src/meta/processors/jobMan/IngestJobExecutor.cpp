/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/MetaServiceUtils.h"
#include "meta/processors/jobMan/IngestJobExecutor.h"

namespace nebula {
namespace meta {

IngestJobExecutor::IngestJobExecutor(JobID jobId,
                                     kvstore::KVStore* kvstore,
                                     AdminClient* adminClient,
                                     const std::vector<std::string>& paras)
    : SimpleConcurrentJobExecutor(jobId, kvstore, adminClient, paras) {}

bool IngestJobExecutor::check() {
    return paras_.size() == 1;
}

nebula::cpp2::ErrorCode IngestJobExecutor::prepare() {
    std::string spaceName = paras_[0];
    auto errOrSpaceId = getSpaceIdFromName(spaceName);
    if (!nebula::ok(errOrSpaceId)) {
        LOG(ERROR) << "Can't find the space: " << spaceName;
        return nebula::error(errOrSpaceId);
    }

    space_ = nebula::value(errOrSpaceId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto prefix = MetaServiceUtils::partPrefix(space_);
    auto code = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Fetch Parts Failed";
        return code;
    }

    while (iter->valid()) {
        for (auto &host : MetaServiceUtils::parsePartVal(iter->val())) {
            if (storageHosts_.count(host.host) == 0) {
                storageHosts_.insert(host.host);
            }
        }
        iter->next();
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

folly::Future<Status> IngestJobExecutor::executeInternal(HostAddr&& address,
                                                         std::vector<PartitionID>&& parts) {
    return adminClient_->addTask(cpp2::AdminCmd::INGEST, jobId_, taskId_++, space_,
                                 {std::move(address)}, taskParas_, std::move(parts), concurrency_);
}

}  // namespace meta
}  // namespace nebula
