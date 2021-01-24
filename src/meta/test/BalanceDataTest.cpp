/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common/fs/TempDir.h"
#include "meta/test/TestUtils.h"
#include "meta/test/MockAdminClient.h"
#include "meta/processors/jobMan/BalanceTask.h"
#include "meta/processors/jobMan/JobManager.h"
#include "meta/processors/jobMan/BalanceJobExecutor.h"
#include "meta/processors/partsMan/CreateSpaceProcessor.h"

DECLARE_uint32(task_concurrency);
DECLARE_int32(heartbeat_interval_secs);
DECLARE_uint32(expired_time_factor);

namespace nebula {
namespace meta {

using ::testing::_;
using ::testing::AtLeast;
using ::testing::ByMove;
using ::testing::Return;
using ::testing::DefaultValue;
using ::testing::NiceMock;
using ::testing::NaggyMock;
using ::testing::StrictMock;
using ::testing::SetArgPointee;

using HostParts = std::unordered_map<HostAddr, std::vector<PartitionID>>;

void showHostLoading(kvstore::KVStore* kv, GraphSpaceID spaceId) {
    auto prefix = MetaServiceUtils::partPrefix(spaceId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
    HostParts hostPart;
    while (iter->valid()) {
        auto key = iter->key();
        PartitionID partId;
        memcpy(&partId, key.data() + prefix.size(), sizeof(PartitionID));
        auto hs = MetaServiceUtils::parsePartVal(iter->val());
        for (auto h : hs) {
            hostPart[h].emplace_back(partId);
        }
        iter->next();
    }

    for (auto it = hostPart.begin(); it != hostPart.end(); it++) {
        std::stringstream ss;
        for (auto part : it->second) {
            ss << part << " ";
        }
        LOG(INFO) << "Host: " << it->first << " parts: " << ss.str();
    }
}

HostParts assignHostParts(kvstore::KVStore* kv, GraphSpaceID spaceId) {
    auto prefix = MetaServiceUtils::partPrefix(spaceId);
    std::unique_ptr<kvstore::KVIterator> iter;
    kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    HostParts hostPart;
    while (iter->valid()) {
        auto key = iter->key();
        PartitionID partId;
        memcpy(&partId, key.data() + prefix.size(), sizeof(PartitionID));
        auto hs = MetaServiceUtils::parsePartVal(iter->val());
        for (auto h : hs) {
            hostPart[h].emplace_back(partId);
        }
        iter->next();
    }
    return hostPart;
}

nebula::cpp2::ErrorCode saveJobDescription(kvstore::KVStore* kv,
                                           const JobDescription& description) {
    std::vector<kvstore::KV> data{std::make_pair(description.jobKey(), description.jobVal())};
    folly::Baton<true, std::atomic> baton;
    auto result = nebula::cpp2::ErrorCode::SUCCEEDED;
    kv->asyncMultiPut(kDefaultSpaceId, kDefaultPartId, std::move(data),
                      [&] (nebula::cpp2::ErrorCode code) {
                        result = code;
                        baton.post();
                      });
    baton.wait();
    return result;
}

TEST(BalanceDataTest, BalanceTaskTest) {
    fs::TempDir rootPath("/tmp/SimpleTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    HostAddr src("0", 0);
    HostAddr dst("1", 1);
    FLAGS_heartbeat_interval_secs = 1;
    TestUtils::registerHB(kv, {src, dst});

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    {
        StrictMock<MockAdminClient> client;
        EXPECT_CALL(client, checkPeers(0, 0)).Times(2);
        EXPECT_CALL(client, transLeader(0, 0, src, _)).Times(1);
        EXPECT_CALL(client, addPart(0, 0, dst, true)).Times(1);
        EXPECT_CALL(client, addLearner(0, 0, dst)).Times(1);
        EXPECT_CALL(client, waitingForCatchUpData(0, 0, dst)).Times(1);
        EXPECT_CALL(client, memberChange(0, 0, dst, true)).Times(1);
        EXPECT_CALL(client, memberChange(0, 0, src, false)).Times(1);
        EXPECT_CALL(client, updateMeta(0, 0, src, dst)).Times(1);
        EXPECT_CALL(client, removePart(0, 0, src)).Times(1);

        folly::Baton<true, std::atomic> b;
        BalanceTask task(0, 0, 0, 0, src, dst, kv, &client);
        task.onFinished_ = [&]() {
            LOG(INFO) << "Task finished!";
            EXPECT_EQ(BalanceTaskResult::SUCCEEDED, task.ret_);
            EXPECT_EQ(BalanceTaskStatus::END, task.status_);
            b.post();
        };
        task.onError_ = []() {
            LOG(FATAL) << "We should not reach here!";
        };
        task.invoke();
        b.wait();
    }
    {
        NiceMock<MockAdminClient> client;
        EXPECT_CALL(client, transLeader(_, _, _, _))
            .Times(1)
            .WillOnce(Return(ByMove(folly::Future<Status>(Status::Error("Transfer failed")))));

        folly::Baton<true, std::atomic> b;
        BalanceTask task(0, 0, 0, 0, src, dst, kv, &client);
        task.onFinished_ = []() {
            LOG(FATAL) << "We should not reach here!";
        };
        task.onError_ = [&]() {
            LOG(INFO) << "Error happens!";
            EXPECT_EQ(BalanceTaskResult::FAILED, task.ret_);
            EXPECT_EQ(BalanceTaskStatus::CHANGE_LEADER, task.status_);
            b.post();
        };
        task.invoke();
        b.wait();
    }
    LOG(INFO) << "Test finished!";
}

TEST(BalanceDataTest, DispatchTasksTest) {
    GraphSpaceID space = 1;
    {
        FLAGS_task_concurrency = 10;
        BalancePlan plan(101, space, nullptr, nullptr);
        for (int i = 0; i < 20; i++) {
            BalanceTask task(0, 0, space, 0, HostAddr(std::to_string(i), 0),
                             HostAddr(std::to_string(i), 1), nullptr, nullptr);
            plan.addTask(std::move(task));
        }
        plan.dispatchTasks();
        // All tasks is about space 0, part 0.
        // So they will be dispatched into the same bucket.
        ASSERT_EQ(1, plan.buckets_.size());
        ASSERT_EQ(20, plan.buckets_[0].size());
    }
    {
        FLAGS_task_concurrency = 10;
        BalancePlan plan(101, space, nullptr, nullptr);
        for (int i = 0; i < 5; i++) {
            BalanceTask task(0, 0, space, i, HostAddr(std::to_string(i), 0),
                             HostAddr(std::to_string(i), 1), nullptr, nullptr);
            plan.addTask(std::move(task));
        }
        plan.dispatchTasks();
        ASSERT_EQ(5, plan.buckets_.size());
        for (auto& bucket : plan.buckets_) {
            ASSERT_EQ(1, bucket.size());
        }
    }
    {
        FLAGS_task_concurrency = 20;
        BalancePlan plan(1, space, nullptr, nullptr);
        for (int i = 0; i < 5; i++) {
            BalanceTask task(0, 0, space, i, HostAddr(std::to_string(i), 0),
                             HostAddr(std::to_string(i), 1), nullptr, nullptr);
            plan.addTask(std::move(task));
        }

        for (int i = 0; i < 10; i++) {
            BalanceTask task(0, 0, space, i, HostAddr(std::to_string(i), 2),
                             HostAddr(std::to_string(i), 3), nullptr, nullptr);
            plan.addTask(std::move(task));
        }
        plan.dispatchTasks();
        ASSERT_EQ(10, plan.buckets_.size());
        int32_t total = 0;
        for (auto i = 0; i < 10; i++) {
            ASSERT_LE(1, plan.buckets_[i].size());
            ASSERT_GE(2, plan.buckets_[i].size());
            total += plan.buckets_[i].size();
        }
        ASSERT_EQ(15, total);
    }
}

TEST(BalanceDataTest, BalancePlanTest) {
    fs::TempDir rootPath("/tmp/BalancePlanTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    std::vector<HostAddr> hosts;
    for (int i = 0; i < 10; i++) {
        hosts.emplace_back(std::to_string(i), 0);
        hosts.emplace_back(std::to_string(i), 1);
    }

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    {
        LOG(INFO) << "Test with all tasks succeeded, only one bucket!";
        GraphSpaceID spaceId = 1;
        NiceMock<MockAdminClient> client;

        JobID jobId = 101;
        std::vector<std::string> paras{"false", "test_space"};
        JobDescription description(jobId, cpp2::AdminCmd::DATA_BALANCE, paras);
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
                  saveJobDescription(kv, std::move(description)));

        BalancePlan plan(jobId, spaceId, kv, &client);
        TestUtils::registerHB(kv, hosts);
        for (int i = 0; i < 10; i++) {
            BalanceTask task(jobId, i, spaceId, 0, HostAddr(std::to_string(i), 0),
                             HostAddr(std::to_string(i), 1), kv, &client);
            plan.addTask(std::move(task));
        }

        folly::Baton<true, std::atomic> b;
        plan.onFinished_ = [&] () {
            ASSERT_EQ(cpp2::JobStatus::FINISHED, plan.status_);
            ASSERT_EQ(10, plan.finishedTaskNum_);
            b.post();
        };
        plan.invoke();
        b.wait();

        // All tasks is about space 0, part 0.
        // So they will be dispatched into the same bucket.
        ASSERT_EQ(1, plan.buckets_.size());
        ASSERT_EQ(10, plan.buckets_[0].size());
    }

    {
        LOG(INFO) << "Test with all tasks succeeded, 10 buckets!";
        NiceMock<MockAdminClient> client;
        JobID jobId = 102;
        std::vector<std::string> paras{"false", "test_space"};
        JobDescription description(jobId, cpp2::AdminCmd::DATA_BALANCE, paras);
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
                  saveJobDescription(kv, std::move(description)));

        BalancePlan plan(jobId, 1, kv, &client);
        TestUtils::registerHB(kv, hosts);

        for (int i = 0; i < 10; i++) {
            BalanceTask task(jobId, i, 1, i, HostAddr(std::to_string(i), 0),
                             HostAddr(std::to_string(i), 1), kv, &client);
            plan.addTask(std::move(task));
        }
        folly::Baton<true, std::atomic> b;
        plan.onFinished_ = [&plan, &b] () {
            ASSERT_EQ(cpp2::JobStatus::FINISHED, plan.status_);
            ASSERT_EQ(10, plan.finishedTaskNum_);
            b.post();
        };
        plan.invoke();
        b.wait();
        // All tasks is about different parts.
        // So they will be dispatched into different buckets.
        ASSERT_EQ(10, plan.buckets_.size());
        for (auto i = 0; i < 10; i++) {
            ASSERT_EQ(1, plan.buckets_[1].size());
        }
    }
    {
        LOG(INFO) << "Test with one task failed, 10 buckets";
        JobID jobId = 103;
        std::vector<std::string> paras{"false", "test_space"};
        JobDescription description(jobId, cpp2::AdminCmd::DATA_BALANCE, paras);
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
                  saveJobDescription(kv, std::move(description)));

        BalancePlan plan(jobId, 1, kv, nullptr);
        NiceMock<MockAdminClient> client1, client2;
        for (int i = 0; i < 9; i++) {
            BalanceTask task(jobId, i, 1, i, HostAddr(std::to_string(i), 0),
                             HostAddr(std::to_string(i), 1), kv, &client1);
            plan.addTask(std::move(task));
        }

        EXPECT_CALL(client2, transLeader(_, _, _, _))
            .Times(1)
            .WillOnce(Return(ByMove(folly::Future<Status>(Status::Error("Transfer failed")))));
        BalanceTask task(jobId, 1, 1, 9, HostAddr("9", 0), HostAddr("9", 1), kv, &client2);
        plan.addTask(std::move(task));
        TestUtils::registerHB(kv, hosts);
        folly::Baton<true, std::atomic> b;
        plan.onFinished_ = [&plan, &b] () {
            ASSERT_EQ(cpp2::JobStatus::FAILED, plan.status_);
            ASSERT_EQ(10, plan.finishedTaskNum_);
            b.post();
        };
        plan.invoke();
        b.wait();
    }
}

TEST(BalanceDataTest, SimpleTestWithZone) {
    fs::TempDir rootPath("/tmp/SimpleTestWithZone.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    FLAGS_heartbeat_interval_secs = 1;
    {
        std::vector<HostAddr> hosts;
        for (int i = 0; i < 4; i++) {
            hosts.emplace_back(std::to_string(i), i);
        }
        TestUtils::createSomeHosts(kv, hosts);
        TestUtils::registerHB(kv, hosts);

        // create zone and group
        ZoneInfo zoneInfo = {
            {"zone_0", {{"0", 0}}},
            {"zone_1", {{"1", 1}}},
            {"zone_2", {{"2", 2}}},
            {"zone_3", {{"3", 3}}}
        };
        GroupInfo groupInfo = {
            {"group_0", {"zone_0", "zone_1", "zone_2", "zone_3"}}
        };
        TestUtils::assembleGroupAndZone(kv, zoneInfo, groupInfo);
    }
    {
        cpp2::SpaceDesc properties;
        properties.set_space_name("default_space");
        properties.set_partition_num(4);
        properties.set_replica_factor(3);
        properties.set_group_name("group_0");
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(properties));
        auto* processor = CreateSpaceProcessor::instance(kv);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(1, resp.get_id().get_space_id());
    }
    sleep(1);
    {
        HostParts hostParts;
        hostParts.emplace(HostAddr("0", 0), std::vector<PartitionID>{1, 2, 3, 4});
        hostParts.emplace(HostAddr("1", 0), std::vector<PartitionID>{1, 2, 3, 4});
        hostParts.emplace(HostAddr("2", 0), std::vector<PartitionID>{1, 2, 3, 4});
        hostParts.emplace(HostAddr("3", 0), std::vector<PartitionID>{});
        int32_t totalParts = 12;
        std::vector<BalanceTask> tasks;
        NiceMock<MockAdminClient> client;
        BalanceJobExecutor executor(0, kv, &client, {});
        executor.balanceParts(0, hostParts, totalParts, tasks);
        for (auto it = hostParts.begin(); it != hostParts.end(); it++) {
            EXPECT_EQ(3, it->second.size());
        }
        EXPECT_EQ(3, tasks.size());
    }
}

TEST(BalanceDataTest, BalancePartsTest) {
    fs::TempDir rootPath("/tmp/BalancePartsTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    NiceMock<MockAdminClient> client;

    auto dump = [](const HostParts& hostParts,
                   const std::vector<BalanceTask>& tasks) {
        for (auto it = hostParts.begin(); it != hostParts.end(); it++) {
            std::stringstream ss;
            ss << it->first << ": ";
            for (auto partId : it->second) {
                ss << partId << ", ";
            }
            VLOG(1) << ss.str();
        }
        for (const auto& task : tasks) {
            VLOG(1) << task.taskIdStr();
        }
    };
    {
        HostParts hostParts;
        hostParts.emplace(HostAddr("0", 0), std::vector<PartitionID>{1, 2, 3, 4});
        hostParts.emplace(HostAddr("1", 0), std::vector<PartitionID>{1, 2, 3, 4});
        hostParts.emplace(HostAddr("2", 0), std::vector<PartitionID>{1, 2, 3, 4});
        hostParts.emplace(HostAddr("3", 0), std::vector<PartitionID>{});
        int32_t totalParts = 12;
        std::vector<BalanceTask> tasks;
        VLOG(1) << "=== original map ====";
        dump(hostParts, tasks);
        BalanceJobExecutor executor(1, kv, &client, {});
        executor.balanceParts(0, hostParts, totalParts, tasks);
        VLOG(1) << "=== new map ====";
        dump(hostParts, tasks);
        for (auto it = hostParts.begin(); it != hostParts.end(); it++) {
            EXPECT_EQ(3, it->second.size());
        }
        EXPECT_EQ(3, tasks.size());
    }
}

TEST(BalanceDataTest, ExpansionZoneTest) {
    fs::TempDir rootPath("/tmp/ExpansionZoneTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    FLAGS_heartbeat_interval_secs = 1;
    {
        std::vector<HostAddr> hosts;
        for (int i = 0; i < 3; i++) {
            hosts.emplace_back(std::to_string(i), i);
        }
        TestUtils::createSomeHosts(kv, hosts);
        TestUtils::registerHB(kv, hosts);

        // create zone and group
        ZoneInfo zoneInfo = {
            {"zone_0", {{"0", 0}}},
            {"zone_1", {{"1", 1}}},
            {"zone_2", {{"2", 2}}}
        };
        GroupInfo groupInfo = {
            {"default_group", {"zone_0", "zone_1", "zone_2"}}
        };
        TestUtils::assembleGroupAndZone(kv, zoneInfo, groupInfo);
    }
    {
        cpp2::SpaceDesc properties;
        properties.set_space_name("default_space");
        properties.set_partition_num(4);
        properties.set_replica_factor(3);
        properties.set_group_name("default_group");
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(properties));
        auto* processor = CreateSpaceProcessor::instance(kv);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(1, resp.get_id().get_space_id());
    }

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });

    JobID jobId = 101;
    NiceMock<MockAdminClient> client;
    std::vector<std::string> paras{"false", "test_space"};
    JobDescription description(jobId, cpp2::AdminCmd::DATA_BALANCE, paras);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, saveJobDescription(kv, std::move(description)));

    BalanceJobExecutor executor(jobId, kv, &client, {});
    executor.space_ = 1;
    auto ret = executor.execute();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_BALANCED, ret);
    {
        std::vector<HostAddr> hosts;
        for (int i = 0; i < 4; i++) {
            hosts.emplace_back(std::to_string(i), i);
        }
        TestUtils::createSomeHosts(kv, hosts);
        TestUtils::registerHB(kv, hosts);
        ZoneInfo zoneInfo = {
            {"zone_0", {{"0", 0}}},
            {"zone_1", {{"1", 1}}},
            {"zone_2", {{"2", 2}}},
            {"zone_3", {{"3", 3}}}
        };
        GroupInfo groupInfo = {
            {"default_group", {"zone_0", "zone_1", "zone_2", "zone_3"}}
        };
        TestUtils::assembleGroupAndZone(kv, zoneInfo, groupInfo);
    }
    {
        HostParts hostParts;
        hostParts.emplace(HostAddr("0", 0), std::vector<PartitionID>{1, 2, 3, 4});
        hostParts.emplace(HostAddr("1", 0), std::vector<PartitionID>{1, 2, 3, 4});
        hostParts.emplace(HostAddr("2", 0), std::vector<PartitionID>{1, 2, 3, 4});
        hostParts.emplace(HostAddr("3", 0), std::vector<PartitionID>{});
        int32_t totalParts = 12;
        std::vector<BalanceTask> tasks;
        executor.balanceParts(0, hostParts, totalParts, tasks);
        for (auto it = hostParts.begin(); it != hostParts.end(); it++) {
            EXPECT_EQ(3, it->second.size());
        }
        EXPECT_EQ(3, tasks.size());
    }
}

TEST(BalanceDataTest, ExpansionHostIntoZoneTest) {
    fs::TempDir rootPath("/tmp/ExpansionHostIntoZoneTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    FLAGS_heartbeat_interval_secs = 1;
    {
        std::vector<HostAddr> hosts;
        for (int i = 0; i < 3; i++) {
            hosts.emplace_back(std::to_string(i), i);
        }
        TestUtils::createSomeHosts(kv, hosts);
        TestUtils::registerHB(kv, hosts);

        // create zone and group
        ZoneInfo zoneInfo = {
            {"zone_0", {{"0", 0}}},
            {"zone_1", {{"1", 1}}},
            {"zone_2", {{"2", 2}}}
        };
        GroupInfo groupInfo = {
            {"default_group", {"zone_0", "zone_1", "zone_2"}}
        };
        TestUtils::assembleGroupAndZone(kv, zoneInfo, groupInfo);
    }
    {
        cpp2::SpaceDesc properties;
        properties.set_space_name("default_space");
        properties.set_partition_num(4);
        properties.set_replica_factor(3);
        properties.set_group_name("default_group");
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(properties));
        auto* processor = CreateSpaceProcessor::instance(kv);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(1, resp.get_id().get_space_id());
    }

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });

    JobID jobId = 101;
    NiceMock<MockAdminClient> client;
    std::vector<std::string> paras{"false", "test_space"};
    JobDescription description(jobId, cpp2::AdminCmd::DATA_BALANCE, paras);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, saveJobDescription(kv, std::move(description)));

    BalanceJobExecutor executor(jobId, kv, &client, {});
    executor.space_ = 1;
    auto ret = executor.execute();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_BALANCED, ret);
    {
        std::vector<HostAddr> hosts;
        for (int i = 0; i < 6; i++) {
            hosts.emplace_back(std::to_string(i), i);
        }
        TestUtils::createSomeHosts(kv, hosts);
        TestUtils::registerHB(kv, hosts);
        ZoneInfo zoneInfo = {
            {"zone_0", {{"0", 0}, {"3", 3}}},
            {"zone_1", {{"1", 1}, {"4", 4}}},
            {"zone_2", {{"2", 2}, {"5", 5}}}
        };
        GroupInfo groupInfo = {
            {"default_group", {"zone_0", "zone_1", "zone_2"}}
        };
        TestUtils::assembleGroupAndZone(kv, zoneInfo, groupInfo);
    }
    {
        HostParts hostParts;
        hostParts.emplace(HostAddr("0", 0), std::vector<PartitionID>{1, 2, 3, 4});
        hostParts.emplace(HostAddr("1", 1), std::vector<PartitionID>{1, 2, 3, 4});
        hostParts.emplace(HostAddr("2", 2), std::vector<PartitionID>{1, 2, 3, 4});
        hostParts.emplace(HostAddr("3", 3), std::vector<PartitionID>{});
        hostParts.emplace(HostAddr("4", 4), std::vector<PartitionID>{});
        hostParts.emplace(HostAddr("5", 5), std::vector<PartitionID>{});

        int32_t totalParts = 12;
        std::vector<BalanceTask> tasks;
        executor.balanceParts(0, hostParts, totalParts, tasks);
        for (auto it = hostParts.begin(); it != hostParts.end(); it++) {
            EXPECT_EQ(2, it->second.size());
        }
        EXPECT_EQ(6, tasks.size());
    }
}

TEST(BalanceDataTest, ShrinkZoneTest) {
    fs::TempDir rootPath("/tmp/ShrinkZoneTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    FLAGS_heartbeat_interval_secs = 1;
    {
        std::vector<HostAddr> hosts;
        for (int i = 0; i < 4; i++) {
            hosts.emplace_back(std::to_string(i), i);
        }

        TestUtils::createSomeHosts(kv, hosts);
        TestUtils::registerHB(kv, hosts);
        // create zone and group
        ZoneInfo zoneInfo = {
            {"zone_0", {{"0", 0}}},
            {"zone_1", {{"1", 1}}},
            {"zone_2", {{"2", 2}}},
            {"zone_3", {{"3", 3}}}
        };
        GroupInfo groupInfo = {
            {"default_group", {"zone_0", "zone_1", "zone_2", "zone_3"}}
        };
        TestUtils::assembleGroupAndZone(kv, zoneInfo, groupInfo);
    }
    {
        cpp2::SpaceDesc properties;
        properties.set_space_name("default_space");
        properties.set_partition_num(4);
        properties.set_replica_factor(3);
        properties.set_group_name("default_group");
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(properties));
        auto* processor = CreateSpaceProcessor::instance(kv);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(1, resp.get_id().get_space_id());
    }

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });

    JobID jobId = 101;
    NiceMock<MockAdminClient> client;
    std::vector<std::string> paras{"false", "test_space"};
    JobDescription description(jobId, cpp2::AdminCmd::DATA_BALANCE, paras);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, saveJobDescription(kv, std::move(description)));

    BalanceJobExecutor executor(jobId, kv, &client, {});
    executor.space_ = 1;
    auto ret = executor.execute();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_BALANCED, ret);
    {
        ZoneInfo zoneInfo = {
            {"zone_0", {{"0", 0}}},
            {"zone_1", {{"1", 1}}},
            {"zone_2", {{"2", 2}}},
        };
        GroupInfo groupInfo = {
            {"default_group", {"zone_0", "zone_1", "zone_2"}}
        };
        TestUtils::assembleGroupAndZone(kv, zoneInfo, groupInfo);
    }

    executor.lostHosts_ = {{"3", 3}};
    ret = executor.execute();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
}

TEST(BalanceDataTest, ShrinkHostFromZoneTest) {
    fs::TempDir rootPath("/tmp/ShrinkHostFromZoneTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    FLAGS_heartbeat_interval_secs = 1;
    {
        std::vector<HostAddr> hosts;
        for (int i = 0; i < 6; i++) {
            hosts.emplace_back(std::to_string(i), i);
        }
        TestUtils::createSomeHosts(kv, hosts);
        TestUtils::registerHB(kv, hosts);

        // create zone and group
        ZoneInfo zoneInfo = {
            {"zone_0", {{"0", 0}, {"3", 3}}},
            {"zone_1", {{"1", 1}, {"4", 4}}},
            {"zone_2", {{"2", 2}, {"5", 5}}}
        };
        GroupInfo groupInfo = {
            {"default_group", {"zone_0", "zone_1", "zone_2"}}
        };
        TestUtils::assembleGroupAndZone(kv, zoneInfo, groupInfo);
    }
    {
        cpp2::SpaceDesc properties;
        properties.set_space_name("default_space");
        properties.set_partition_num(4);
        properties.set_replica_factor(3);
        properties.set_group_name("default_group");
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(properties));
        auto* processor = CreateSpaceProcessor::instance(kv);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(1, resp.get_id().get_space_id());
    }

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });

    JobID jobId = 101;
    NiceMock<MockAdminClient> client;
    std::vector<std::string> paras{"false", "test_space"};
    JobDescription description(jobId, cpp2::AdminCmd::DATA_BALANCE, paras);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, saveJobDescription(kv, std::move(description)));

    BalanceJobExecutor executor(jobId, kv, &client, {});
    executor.space_ = 1;
    auto ret = executor.execute();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_BALANCED, ret);
    showHostLoading(kv, 1);

    {
        ZoneInfo zoneInfo = {
            {"zone_0", {{"0", 0}}},
            {"zone_1", {{"1", 1}, {"4", 4}}},
            {"zone_2", {{"2", 2}, {"5", 5}}}
        };
        GroupInfo groupInfo = {
            {"default_group", {"zone_0", "zone_1", "zone_2"}}
        };
        TestUtils::assembleGroupAndZone(kv, zoneInfo, groupInfo);
    }
    executor.lostHosts_ = {{"3", 3}};
    ret = executor.execute();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
}

TEST(BalanceDataTest, BalanceWithComplexZoneTest) {
    fs::TempDir rootPath("/tmp/BalanceWithComplexZoneTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    FLAGS_heartbeat_interval_secs = 1;
    std::vector<HostAddr> hosts;
    for (int i = 0; i < 18; i++) {
        hosts.emplace_back(std::to_string(i), i);
    }
    TestUtils::createSomeHosts(kv, hosts);
    TestUtils::registerHB(kv, hosts);

    {
        ZoneInfo zoneInfo = {
            {"zone_0", {HostAddr("0", 0), HostAddr("1", 1)}},
            {"zone_1", {HostAddr("2", 2), HostAddr("3", 3)}},
            {"zone_2", {HostAddr("4", 4), HostAddr("5", 5)}},
            {"zone_3", {HostAddr("6", 6), HostAddr("7", 7)}},
            {"zone_4", {HostAddr("8", 8), HostAddr("9", 9)}},
            {"zone_5", {HostAddr("10", 10), HostAddr("11", 11)}},
            {"zone_6", {HostAddr("12", 12), HostAddr("13", 13)}},
            {"zone_7", {HostAddr("14", 14), HostAddr("15", 15)}},
            {"zone_8", {HostAddr("16", 16), HostAddr("17", 17)}},
        };
        {
            GroupInfo groupInfo = {
                {"group_0", {"zone_0", "zone_1", "zone_2", "zone_3", "zone_4"}}
            };
            TestUtils::assembleGroupAndZone(kv, zoneInfo, groupInfo);
        }
        {
            GroupInfo groupInfo = {
                {"group_1", {"zone_0", "zone_1", "zone_2", "zone_3", "zone_4",
                             "zone_5", "zone_6", "zone_7", "zone_8"}}
            };
            TestUtils::assembleGroupAndZone(kv, zoneInfo, groupInfo);
        }
    }
    {
        {
            cpp2::SpaceDesc properties;
            properties.set_space_name("default_space");
            properties.set_partition_num(18);
            properties.set_replica_factor(3);
            cpp2::CreateSpaceReq req;
            req.set_properties(std::move(properties));
            auto* processor = CreateSpaceProcessor::instance(kv);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
            ASSERT_EQ(1, resp.get_id().get_space_id());
            LOG(INFO) << "Show host about space " << resp.get_id().get_space_id();
            showHostLoading(kv, resp.get_id().get_space_id());
        }
        {
            cpp2::SpaceDesc properties;
            properties.set_space_name("space_on_group_0");
            properties.set_partition_num(64);
            properties.set_replica_factor(3);
            properties.set_group_name("group_0");
            cpp2::CreateSpaceReq req;
            req.set_properties(std::move(properties));
            auto* processor = CreateSpaceProcessor::instance(kv);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
            ASSERT_EQ(2, resp.get_id().get_space_id());
            LOG(INFO) << "Show host about space " << resp.get_id().get_space_id();
            showHostLoading(kv, resp.get_id().get_space_id());
        }
        {
            cpp2::SpaceDesc properties;
            properties.set_space_name("space_on_group_1");
            properties.set_partition_num(81);
            properties.set_replica_factor(3);
            properties.set_group_name("group_1");
            cpp2::CreateSpaceReq req;
            req.set_properties(std::move(properties));
            auto* processor = CreateSpaceProcessor::instance(kv);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
            ASSERT_EQ(3, resp.get_id().get_space_id());
            LOG(INFO) << "Show host about space " << resp.get_id().get_space_id();
            showHostLoading(kv, resp.get_id().get_space_id());
        }
    }
    sleep(1);

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    NiceMock<MockAdminClient> client;
    BalanceJobExecutor executor(3, kv, &client, {});

    {
        int32_t totalParts = 18 * 3;
        std::vector<BalanceTask> tasks;
        auto hostParts = assignHostParts(kv, 1);
        executor.space_ = 1;
        executor.balanceParts(0, hostParts, totalParts, tasks);
    }
    {
        int32_t totalParts = 64 * 3;
        std::vector<BalanceTask> tasks;
        auto hostParts = assignHostParts(kv, 2);
        executor.space_ = 2;
        executor.balanceParts(0, hostParts, totalParts, tasks);
    }
    {
        auto dump = [](const HostParts& hostParts,
                       const std::vector<BalanceTask>& tasks) {
            for (auto it = hostParts.begin(); it != hostParts.end(); it++) {
                std::stringstream ss;
                ss << it->first << ": ";
                for (auto partId : it->second) {
                    ss << partId << ", ";
                }
                LOG(INFO) << ss.str() << " size " << it->second.size();
            }
            for (const auto& task : tasks) {
                LOG(INFO) << task.taskIdStr();
            }
        };

        HostParts hostParts;
        std::vector<PartitionID> parts;
        for (int32_t i = 0; i< 81; i++) {
            parts.emplace_back(i);
        }

        for (int32_t i = 0; i< 18; i++) {
            if (i == 10 || i == 12 || i == 14) {
                hostParts.emplace(HostAddr(std::to_string(i), i), parts);
            } else {
                hostParts.emplace(HostAddr(std::to_string(i), i), std::vector<PartitionID>{});
            }
        }

        LOG(INFO) << "=== original map ====";
        int32_t totalParts = 243;
        std::vector<BalanceTask> tasks;
        dump(hostParts, tasks);
        executor.space_ = 3;
        executor.balanceParts(0, hostParts, totalParts, tasks);

        LOG(INFO) << "=== new map ====";
        dump(hostParts, tasks);
        for (auto it = hostParts.begin(); it != hostParts.end(); it++) {
            EXPECT_GE(it->second.size(), 5);
            EXPECT_LE(it->second.size(), 24);

            LOG(INFO) << "Host " << it->first << " Part Size " << it->second.size();
        }
        showHostLoading(kv, 3);
    }
}

int32_t verifyBalancePlan(kvstore::KVStore* kv,
                          GraphSpaceID spaceId,
                          cpp2::JobStatus balanceStatus) {
    const auto& prefix = MetaServiceUtils::balancePlanPrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    EXPECT_EQ(retcode, nebula::cpp2::ErrorCode::SUCCEEDED);
    int32_t num = 0;
    while (iter->valid()) {
        auto id = MetaServiceUtils::parseBalancePlanKey(iter->key());
        auto planItem = MetaServiceUtils::parseBalancePlanVal(iter->val());
        EXPECT_EQ(spaceId, id);
        EXPECT_EQ(balanceStatus, planItem.get_status());
        num++;
        iter->next();
    }
    return num;
}

void verifyBalanceTask(kvstore::KVStore* kv,
                       JobID jobId,
                       BalanceTaskStatus status,
                       BalanceTaskResult result,
                       std::unordered_map<HostAddr, int32_t>& partCount,
                       int32_t exceptNumber) {
    const auto& prefix = MetaServiceUtils::balanceTaskPrefix(jobId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto code = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    ASSERT_EQ(code, nebula::cpp2::ErrorCode::SUCCEEDED);
    int32_t num = 0;
    while (iter->valid()) {
        auto keyTuple = MetaServiceUtils::parseBalanceTaskKey(iter->key());
        ASSERT_EQ(jobId, std::get<0>(keyTuple));
        ASSERT_EQ(1, std::get<2>(keyTuple));
        partCount[std::get<4>(keyTuple)]--;
        partCount[std::get<5>(keyTuple)]++;
        auto valueTuple = MetaServiceUtils::parseBalanceTaskVal(iter->val());
        ASSERT_EQ(status, std::get<0>(valueTuple));
        ASSERT_EQ(result, std::get<1>(valueTuple));
        ASSERT_LT(0, std::get<2>(valueTuple));
        ASSERT_LT(0, std::get<3>(valueTuple));
        num++;
        iter->next();
    }
    ASSERT_EQ(exceptNumber, num);
}

TEST(BalanceDataTest, NormalTest) {
    fs::TempDir rootPath("/tmp/NormalTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    FLAGS_heartbeat_interval_secs = 1;
    FLAGS_expired_time_factor = 1;
    TestUtils::createSomeHosts(kv);
    GraphSpaceID spaceId = 1;
    TestUtils::assembleSpace(kv, spaceId, 8, 3, 4);
    std::unordered_map<HostAddr, int32_t> partCount;

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });

    JobID jobId = 101;
    NiceMock<MockAdminClient> client;
    std::vector<std::string> paras{"false", "test_space"};
    JobDescription description(jobId, cpp2::AdminCmd::DATA_BALANCE, paras);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, saveJobDescription(kv, description));

    BalanceJobExecutor executor(jobId, kv, &client, {});
    executor.space_ = spaceId;
    auto code = executor.execute();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_BALANCED, code);

    sleep(1);
    LOG(INFO) << "Now, we lost host " << HostAddr("3", 3);
    TestUtils::registerHB(kv, {{"0", 0}, {"1", 1}, {"2", 2}});
    code = executor.execute();
    sleep(3);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
    description.setStatus(cpp2::JobStatus::FINISHED);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, saveJobDescription(kv, std::move(description)));

    LOG(INFO) << "Rebalance finished!";
    ASSERT_EQ(1, verifyBalancePlan(kv, spaceId, cpp2::JobStatus::FINISHED));
    verifyBalanceTask(kv, executor.plan_->id_,
                      BalanceTaskStatus::END,
                      BalanceTaskResult::SUCCEEDED,
                      partCount, 6);
}

TEST(BalanceDataTest, SpecifyHostTest) {
    fs::TempDir rootPath("/tmp/SpecifyHostTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    FLAGS_heartbeat_interval_secs = 1;
    FLAGS_expired_time_factor = 1;
    TestUtils::createSomeHosts(kv, {{"0", 0}, {"1", 1}, {"2", 2}, {"3", 3}});
    GraphSpaceID spaceId = 1;
    TestUtils::assembleSpace(kv, spaceId, 8, 3, 4);
    std::unordered_map<HostAddr, int32_t> partCount;

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });

    JobID jobId = 101;
    NiceMock<MockAdminClient> client;
    std::vector<std::string> paras{"3:3", "false", "test_space"};
    JobDescription description(jobId, cpp2::AdminCmd::DATA_BALANCE, paras);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, saveJobDescription(kv, description));

    BalanceJobExecutor executor(jobId, kv, &client, {});
    executor.space_ = spaceId;

    sleep(1);
    LOG(INFO) << "Now, we remove host {3, 3}";
    TestUtils::registerHB(kv, {{"0", 0}, {"1", 1}, {"2", 2}, {"3", 3}});
    executor.paras_ = std::move(paras);
    ASSERT_TRUE(executor.check());
    auto code = executor.prepare();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
    code = executor.execute();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);

    sleep(1);
    LOG(INFO) << "Rebalance finished!";
    ASSERT_EQ(1, verifyBalancePlan(kv, spaceId, cpp2::JobStatus::FINISHED));
    verifyBalanceTask(kv, jobId,
                      BalanceTaskStatus::END,
                      BalanceTaskResult::SUCCEEDED,
                      partCount, 6);
}

TEST(BalanceDataTest, SpecifyMultiHostTest) {
    fs::TempDir rootPath("/tmp/SpecifyMultiHostTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    FLAGS_heartbeat_interval_secs = 1;
    FLAGS_expired_time_factor = 1;
    TestUtils::createSomeHosts(kv, {{"0", 0}, {"1", 1}, {"2", 2},
                                    {"3", 3}, {"4", 4}, {"5", 5}});
    GraphSpaceID spaceId = 1;
    TestUtils::assembleSpace(kv, spaceId, 12, 3, 6);
    std::unordered_map<HostAddr, int32_t> partCount;
    for (int32_t i = 0; i < 6; i++) {
        partCount[HostAddr(std::to_string(i), i)] = 6;
    }

    NiceMock<MockAdminClient> client;
    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    {
        JobID jobId = 101;
        std::vector<std::string> paras{"2:2,3:3", "false", "test_space"};
        JobDescription description(jobId, cpp2::AdminCmd::DATA_BALANCE, paras);
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, saveJobDescription(kv, description));

        sleep(1);
        LOG(INFO) << "Now, we want to remove host {2, 2}/{3, 3}";
        // If {"2", 2} and {"3", 3} are both dead, minority hosts for some part are alive,
        // it would lead to a fail
        TestUtils::registerHB(kv, {{"0", 0}, {"1", 1}, {"4", 4}, {"5", 5}});

        BalanceJobExecutor executor(jobId, kv, &client, {});
        executor.paras_ = paras;
        ASSERT_TRUE(executor.check());
        auto code = executor.prepare();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
        code = executor.execute();
        EXPECT_EQ(nebula::cpp2::ErrorCode::E_NO_VALID_HOST, code);
    }
    {
        JobID jobId = 102;
        std::vector<std::string> paras{"2:2,3:3", "false", "test_space"};
        JobDescription description(jobId, cpp2::AdminCmd::DATA_BALANCE, paras);
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, saveJobDescription(kv, description));

        // If {"2", 2} is dead, {"3", 3} stiil alive, each part has majority hosts alive
        TestUtils::registerHB(kv, {{"0", 0}, {"1", 1}, {"3", 3}, {"4", 4}, {"5", 5}});

        BalanceJobExecutor executor(jobId, kv, &client, {});
        executor.paras_ = paras;
        ASSERT_TRUE(executor.check());
        auto code = executor.prepare();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
        code = executor.execute();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);

        sleep(1);
        LOG(INFO) << "Rebalance finished!";
        ASSERT_EQ(1, verifyBalancePlan(kv, spaceId, cpp2::JobStatus::FINISHED));

        verifyBalanceTask(kv, jobId,
                          BalanceTaskStatus::END,
                          BalanceTaskResult::SUCCEEDED,
                          partCount, 13);
        ASSERT_EQ(9, partCount[HostAddr("0", 0)]);
        ASSERT_EQ(9, partCount[HostAddr("1", 1)]);
        ASSERT_EQ(0, partCount[HostAddr("2", 2)]);
        ASSERT_EQ(0, partCount[HostAddr("3", 3)]);
        ASSERT_EQ(9, partCount[HostAddr("4", 4)]);
        ASSERT_EQ(9, partCount[HostAddr("5", 5)]);
    }
}

TEST(BalanceDataTest, MockReplaceMachineTest) {
    fs::TempDir rootPath("/tmp/MockReplaceMachineTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    FLAGS_heartbeat_interval_secs = 1;
    FLAGS_expired_time_factor = 1;
    TestUtils::createSomeHosts(kv, {{"0", 0}, {"1", 1}, {"2", 2}});
    GraphSpaceID spaceId = 1;
    TestUtils::assembleSpace(kv, spaceId, 12, 3, 3);

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });

    JobID jobId = 101;
    NiceMock<MockAdminClient> client;
    std::vector<std::string> paras{"false", "test_space"};
    JobDescription description(jobId, cpp2::AdminCmd::DATA_BALANCE, paras);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, saveJobDescription(kv, description));
    BalanceJobExecutor executor(jobId, kv, &client, {});

    // add a new machine
    TestUtils::createSomeHosts(kv, {{"0", 0}, {"1", 1}, {"2", 2}, {"3", 3}});
    LOG(INFO) << "Now, we want to replace host {2, 2} with {3, 3}";
    // Because for all parts majority hosts still alive, we could balance
    sleep(1);
    TestUtils::registerHB(kv, {{"0", 0}, {"1", 1}, {"3", 3}});

    executor.paras_ = paras;
    ASSERT_TRUE(executor.check());
    auto code = executor.prepare();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
    code = executor.execute();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);

    sleep(1);
    LOG(INFO) << "Rebalance finished!";
    ASSERT_EQ(1, verifyBalancePlan(kv, spaceId, cpp2::JobStatus::FINISHED));

    std::unordered_map<HostAddr, int32_t> partCount;
    verifyBalanceTask(kv, jobId,
                      BalanceTaskStatus::END,
                      BalanceTaskResult::SUCCEEDED,
                      partCount, 12);
}

TEST(BalanceDataTest, SingleReplicaTest) {
    fs::TempDir rootPath("/tmp/SingleReplicaTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    FLAGS_heartbeat_interval_secs = 1;
    FLAGS_expired_time_factor = 1;
    TestUtils::createSomeHosts(kv, {{"0", 0}, {"1", 1}, {"2", 2},
                                    {"3", 3}, {"4", 4}, {"5", 5}});
    GraphSpaceID spaceId = 1;
    TestUtils::assembleSpace(kv, spaceId, 12, 1, 6);
    std::unordered_map<HostAddr, int32_t> partCount;
    for (int32_t i = 0; i < 6; i++) {
        partCount[HostAddr(std::to_string(i), i)] = 2;
    }

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });

    JobID jobId = 101;
    NiceMock<MockAdminClient> client;
    std::vector<std::string> paras{"2:2,3:3", "false", "test_space"};
    JobDescription description(jobId, cpp2::AdminCmd::DATA_BALANCE, paras);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, saveJobDescription(kv, description));
    BalanceJobExecutor executor(jobId, kv, &client, {});

    sleep(1);
    LOG(INFO) << "Now, we want to remove host {2, 2} and {3, 3}";
    TestUtils::registerHB(kv, {{"0", 0}, {"1", 1}, {"2", 2}, {"3", 3}, {"4", 4}, {"5", 5}});

    executor.paras_ = paras;
    ASSERT_TRUE(executor.check());
    auto code = executor.prepare();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
    code = executor.execute();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);

    sleep(1);
    LOG(INFO) << "Rebalance finished!";
    ASSERT_EQ(1, verifyBalancePlan(kv, spaceId, cpp2::JobStatus::FINISHED));

    verifyBalanceTask(kv, jobId,
                      BalanceTaskStatus::END,
                      BalanceTaskResult::SUCCEEDED,
                      partCount, 4);
    ASSERT_EQ(3, partCount[HostAddr("0", 0)]);
    ASSERT_EQ(3, partCount[HostAddr("1", 1)]);
    ASSERT_EQ(0, partCount[HostAddr("2", 2)]);
    ASSERT_EQ(0, partCount[HostAddr("3", 3)]);
    ASSERT_EQ(3, partCount[HostAddr("4", 4)]);
    ASSERT_EQ(3, partCount[HostAddr("5", 5)]);
}

TEST(BalanceDataTest, TryToRecoveryTest) {
    fs::TempDir rootPath("/tmp/TryToRecoveryTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    FLAGS_heartbeat_interval_secs = 1;
    FLAGS_expired_time_factor = 1;
    TestUtils::createSomeHosts(kv);
    GraphSpaceID spaceId = 1;
    TestUtils::assembleSpace(kv, spaceId, 8, 3, 4);

    sleep(1);
    LOG(INFO) << "Now, we lost host " << HostAddr("3", 3);
    TestUtils::registerHB(kv, {{"0", 0}, {"1", 1}, {"2", 2}});

    JobID jobId = 101;
    NiceMock<MockAdminClient> client;
    BalanceJobExecutor executor(jobId, kv, &client, {});
    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });

    {
        std::vector<std::string> paras{"false", "test_space"};
        JobDescription description(jobId, cpp2::AdminCmd::DATA_BALANCE, paras);
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, saveJobDescription(kv, description));

        // first 6 call is the failed case, since we can't recover the plan, so only 6 call
        EXPECT_CALL(client, waitingForCatchUpData(_, _, _))
            .Times(6)
            .WillOnce(Return(ByMove(folly::Future<Status>(Status::Error("catch up failed")))))
            .WillOnce(Return(ByMove(folly::Future<Status>(Status::Error("catch up failed")))))
            .WillOnce(Return(ByMove(folly::Future<Status>(Status::Error("catch up failed")))))
            .WillOnce(Return(ByMove(folly::Future<Status>(Status::Error("catch up failed")))))
            .WillOnce(Return(ByMove(folly::Future<Status>(Status::Error("catch up failed")))))
            .WillOnce(Return(ByMove(folly::Future<Status>(Status::Error("catch up failed")))));

        executor.paras_ = paras;
        ASSERT_TRUE(executor.check());
        auto code = executor.prepare();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
        code = executor.execute();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
        sleep(1);

        ASSERT_EQ(1, verifyBalancePlan(kv, spaceId, cpp2::JobStatus::FAILED));
        std::unordered_map<HostAddr, int32_t> partCount;
        verifyBalanceTask(kv, jobId,
                        BalanceTaskStatus::CATCH_UP_DATA,
                        BalanceTaskResult::FAILED,
                        partCount, 6);
    }

    LOG(INFO) << "Now let's try to recovery it. Since the the host will expired in 1 second, "
              << "all host would be regarded as offline, so all task will be invalid";
    {
        std::vector<std::string> paras{"true", "test_space"};

        executor.paras_ = paras;
        ASSERT_TRUE(executor.check());
        auto code = executor.prepare();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
        code = executor.execute();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);

        sleep(1);
        ASSERT_EQ(1, verifyBalancePlan(kv, spaceId, cpp2::JobStatus::FINISHED));
        std::unordered_map<HostAddr, int32_t> partCount;
        verifyBalanceTask(kv, jobId,
                        BalanceTaskStatus::START,
                        BalanceTaskResult::INVALID,
                        partCount, 6);
    }
}

TEST(BalanceDataTest, RecoveryTest) {
    FLAGS_task_concurrency = 1;
    fs::TempDir rootPath("/tmp/RecoveryTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    FLAGS_heartbeat_interval_secs = 1;
    FLAGS_expired_time_factor = 1;
    TestUtils::createSomeHosts(kv);
    GraphSpaceID spaceId = 1;
    TestUtils::assembleSpace(kv, spaceId, 8, 3, 4);

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });

    NiceMock<MockAdminClient> client;
    // first 6 call is the failed case, the later call will return default value
    // In gtest release 1.8.0 we can only write as follows:
    EXPECT_CALL(client, waitingForCatchUpData(_, _, _))
        .Times(AtLeast(12))
        .WillOnce(Return(ByMove(folly::Future<Status>(Status::Error("catch up failed")))))
        .WillOnce(Return(ByMove(folly::Future<Status>(Status::Error("catch up failed")))))
        .WillOnce(Return(ByMove(folly::Future<Status>(Status::Error("catch up failed")))))
        .WillOnce(Return(ByMove(folly::Future<Status>(Status::Error("catch up failed")))))
        .WillOnce(Return(ByMove(folly::Future<Status>(Status::Error("catch up failed")))))
        .WillOnce(Return(ByMove(folly::Future<Status>(Status::Error("catch up failed")))));

    sleep(1);

    JobID jobId = 101;
    BalanceJobExecutor executor(jobId, kv, &client, {});
    {
        LOG(INFO) << "Now, we lost host " << HostAddr("3", 3);
        TestUtils::registerHB(kv, {{"0", 0}, {"1", 1}, {"2", 2}});
        std::vector<std::string> paras{"false", "test_space"};
        JobDescription description(jobId, cpp2::AdminCmd::DATA_BALANCE, paras);
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, saveJobDescription(kv, description));

        executor.paras_ = paras;
        ASSERT_TRUE(executor.check());
        auto code = executor.prepare();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
        code = executor.execute();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
        sleep(1);

        ASSERT_EQ(1, verifyBalancePlan(kv, spaceId, cpp2::JobStatus::FAILED));
        std::unordered_map<HostAddr, int32_t> partCount;
        verifyBalanceTask(kv, jobId,
                          BalanceTaskStatus::CATCH_UP_DATA,
                          BalanceTaskResult::FAILED,
                          partCount, 6);
    }
    {
        // register hb again to prevent from regarding src as offline
        TestUtils::registerHB(kv, {{"0", 0}, {"1", 1}, {"2", 2}});
        LOG(INFO) << "Now let's try to recovery it.";
        std::vector<std::string> paras{"true", "test_space"};
        JobDescription description(jobId, cpp2::AdminCmd::DATA_BALANCE, paras);
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, saveJobDescription(kv, description));

        executor.paras_ = paras;
        ASSERT_TRUE(executor.check());
        auto code = executor.prepare();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
        code = executor.execute();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
        sleep(1);

        ASSERT_EQ(1, verifyBalancePlan(kv, spaceId, cpp2::JobStatus::FINISHED));
        std::unordered_map<HostAddr, int32_t> partCount;
        verifyBalanceTask(kv, jobId,
                          BalanceTaskStatus::END,
                          BalanceTaskResult::SUCCEEDED,
                          partCount, 6);
    }
}

}  // namespace meta
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    testing::InitGoogleMock(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
