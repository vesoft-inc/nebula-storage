/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/jobMan/RebuildTagJobExecutor.h"

namespace nebula {
namespace meta {

std::vector<folly::Future<Status>>
RebuildTagJobExecutor::executeInternal(HostAddr&& host,
                                       std::vector<PartitionID>&& parts) {
    std::vector<folly::Future<Status>> futures;
    for (auto indexId : indexIds_) {
        std::vector<std::string> taskParameters;
        taskParameters.emplace_back(folly::to<std::string>(indexId));
        auto future = adminClient_->addTask(cpp2::AdminCmd::REBUILD_TAG_INDEX, jobId_, taskId_++,
                                            space_, {std::move(host)}, std::move(taskParameters),
                                            std::move(parts), concurrency_);
        futures.emplace_back(std::move(future));
    }
    return futures;
}

}  // namespace meta
}  // namespace nebula
