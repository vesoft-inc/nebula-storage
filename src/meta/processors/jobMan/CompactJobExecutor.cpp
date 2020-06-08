/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/jobMan/CompactJobExecutor.h"

namespace nebula {
namespace meta {

CompactJobExecutor::CompactJobExecutor(JobID jobId,
                                       kvstore::KVStore* kvstore,
                                       AdminClient* adminClient,
                                       std::vector<std::string> paras)
    : SimpleConcurrentJobExecutor(jobId, kvstore, adminClient, paras) {}

folly::Future<Status> CompactJobExecutor::executeInternal(const HostAddr& address,
                                                          std::vector<PartitionID> parts) {
    return adminClient_->addTask(cpp2::AdminCmd::COMPACT, jobId_, taskId_++, space_,
                                 {address}, {}, parts, concurrency_);
}

}  // namespace meta
}  // namespace nebula
