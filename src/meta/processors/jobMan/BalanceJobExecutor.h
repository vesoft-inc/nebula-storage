 /* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_BALANCEJOBEXECUTOR_H_
#define META_BALANCEJOBEXECUTOR_H_

#include "meta/processors/jobMan/BalanceTask.h"
#include "meta/processors/jobMan/BalancePlan.h"
#include "meta/processors/jobMan/SimpleConcurrentJobExecutor.h"
#include <folly/executors/CPUThreadPoolExecutor.h>

namespace nebula {
namespace meta {

using SpaceInfo = std::pair<int32_t, bool>;
using HostParts = std::unordered_map<HostAddr, std::vector<PartitionID>>;
using ZoneParts = std::pair<std::string, std::vector<PartitionID>>;
using ZoneNameAndParts = std::pair<std::string, std::vector<PartitionID>>;

/*
 * BalanceJobExecutor is use to balance data between hosts.
 */
class BalanceJobExecutor : public MetaJobExecutor {
    FRIEND_TEST(BalanceDataTest, BalancePartsTest);
    FRIEND_TEST(BalanceDataTest, SimpleTestWithZone);
    FRIEND_TEST(BalanceDataTest, ExpansionZoneTest);
    FRIEND_TEST(BalanceDataTest, ExpansionHostIntoZoneTest);
    FRIEND_TEST(BalanceDataTest, ShrinkZoneTest);
    FRIEND_TEST(BalanceDataTest, ShrinkHostFromZoneTest);
    FRIEND_TEST(BalanceDataTest, BalanceWithComplexZoneTest);
    FRIEND_TEST(BalanceDataTest, NormalTest);
    FRIEND_TEST(BalanceDataTest, SpecifyHostTest);
    FRIEND_TEST(BalanceDataTest, SpecifyMultiHostTest);
    FRIEND_TEST(BalanceDataTest, MockReplaceMachineTest);
    FRIEND_TEST(BalanceDataTest, SingleReplicaTest);
    FRIEND_TEST(BalanceDataTest, TryToRecoveryTest);
    FRIEND_TEST(BalanceDataTest, RecoveryTest);
    FRIEND_TEST(BalanceDataTest, StopPlanTest);

public:
    BalanceJobExecutor(JobID jobId,
                       kvstore::KVStore* kvstore,
                       AdminClient* adminClient,
                       const std::vector<std::string>& params);

    nebula::cpp2::ErrorCode execute() override;

    bool check() override;

    nebula::cpp2::ErrorCode prepare() override;

    nebula::cpp2::ErrorCode stop() override;

    nebula::cpp2::ErrorCode recovery() override;

protected:
    folly::Future<Status>
    executeInternal(HostAddr&&, std::vector<PartitionID>&&) override {
        return Status::OK();
    }

private:
    ErrorOr<nebula::cpp2::ErrorCode, std::vector<BalanceTask>>
    genTasks(int32_t spaceReplica,
             bool dependentOnGroup,
             std::vector<HostAddr> lostHosts);

    ErrorOr<nebula::cpp2::ErrorCode, std::vector<BalanceTask>>
    recoveryInternal();

    ErrorOr<nebula::cpp2::ErrorCode, std::pair<HostParts, std::vector<HostAddr>>>
    fetchHostParts(bool dependentOnGroup,
                   const HostParts& hostParts,
                   std::vector<HostAddr>& lostHosts);

    bool balanceParts(JobID id,
                      HostParts& newHostParts,
                      int32_t totalParts,
                      std::vector<BalanceTask>& tasks);

    bool transferLostHost(std::vector<BalanceTask>& tasks,
                          HostParts& newHostParts,
                          const HostAddr& source,
                          PartitionID partId,
                          bool dependentOnGroup);

    void calDiff(const HostParts& hostParts,
                 const std::vector<HostAddr>& activeHosts,
                 std::vector<HostAddr>& newlyAdded,
                 std::vector<HostAddr>& lost);

    Status checkReplica(const HostParts& hostParts,
                        const std::vector<HostAddr>& activeHosts,
                        int32_t replica,
                        PartitionID partId);

    std::vector<std::pair<HostAddr, int32_t>>
    sortedHostsByParts(const HostParts& hostParts);

    StatusOr<HostAddr> hostWithMinimalParts(const HostParts& hostParts,
                                            PartitionID partId);

    StatusOr<HostAddr> hostWithMinimalPartsForZone(const HostAddr& source,
                                                   const HostParts& hostParts,
                                                   PartitionID partId);

    bool checkZoneLegal(const HostAddr& source, const HostAddr& target, PartitionID part);

    bool assembleZoneParts(const std::string& groupName, HostParts& hostParts);

    bool getHostParts(bool dependentOnGroup,
                      HostParts& hostParts,
                      int32_t& totalParts);

private:
    TaskID                                         taskId_{0};
    bool                                           recovery_{false};
    std::shared_ptr<BalancePlan>                   plan_;
    std::vector<HostAddr>                          lostHosts_;
    std::unique_ptr<folly::Executor>               executor_;
    std::unordered_map<HostAddr, ZoneNameAndParts> zoneParts_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_BALANCEJOBEXECUTOR_H_
