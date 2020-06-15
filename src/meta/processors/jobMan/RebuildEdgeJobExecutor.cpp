/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/jobMan/RebuildEdgeJobExecutor.h"

namespace nebula {
namespace meta {

folly::Future<Status>
RebuildEdgeJobExecutor::executeInternal(HostAddr address,
                                        std::vector<PartitionID> parts) {
    // TODO will move to admin client's addTask
    return adminClient_->rebuildEdgeIndex(std::move(address), space_, indexId_,
                                          std::move(parts), isOffline_);
}

}  // namespace meta
}  // namespace nebula
