 /* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_BALANCEJOBEXECUTOR_H_
#define META_BALANCEJOBEXECUTOR_H_

#include "meta/processors/admin/BalanceTask.h"
#include "meta/processors/admin/BalancePlan.h"
#include "meta/processors/jobMan/SimpleConcurrentJobExecutor.h"

namespace nebula {
namespace meta {

using HostParts = std::unordered_map<HostAddr, std::vector<PartitionID>>;
using ZoneParts = std::pair<std::string, std::vector<PartitionID>>;

/*
 * BalanceJobExecutor is use to balance data between hosts.
 */
class BalanceJobExecutor : public MetaJobExecutor {
public:
    BalanceJobExecutor(JobID jobId,
                       kvstore::KVStore* kvstore,
                       AdminClient* adminClient,
                       const std::vector<std::string>& params);

    bool check() override;

    cpp2::ErrorCode prepare() override;

    cpp2::ErrorCode stop() override;

protected:
    folly::Future<Status>
    executeInternal(HostAddr&& address, std::vector<PartitionID>&& parts) override;

private:
    cpp2::ErrorCode getSpaceInfo(GraphSpaceID spaceId, std::pair<int32_t, bool>& spaceInfo);

    ErrorOr<cpp2::ErrorCode, std::vector<BalanceTask>>
    genBalanceTasks(GraphSpaceID spaceId,
                    int32_t spaceReplica,
                    bool dependentOnGroup,
                    std::vector<HostAddr>&& hostDel);

    int32_t getHostParts(GraphSpaceID spaceId,
                         bool dependentOnGroup,
                         HostParts& hostParts);

private:
    mutable std::mutex lock_;
    std::unique_ptr<BalancePlan> plan_;
    std::unordered_map<HostAddr, ZoneParts> zoneParts_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_BALANCEJOBEXECUTOR_H_
