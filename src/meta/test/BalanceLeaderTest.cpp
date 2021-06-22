/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <folly/synchronization/Baton.h>
#include "meta/test/TestUtils.h"
#include "meta/test/MockAdminClient.h"
#include "meta/processors/partsMan/CreateSpaceProcessor.h"
#include "meta/processors/admin/LeaderBalanceProcessor.h"

DECLARE_uint32(task_concurrency);
DECLARE_int32(heartbeat_interval_secs);
DECLARE_double(leader_balance_deviation);

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

void verifyLeaderBalancePlan(HostLeaderMap& hostLeaderMap,
                             const LeaderBalancePlan& plan,
                             size_t minLoad, size_t maxLoad) {
    for (const auto& task : plan) {
        auto space = std::get<0>(task);
        auto part = std::get<1>(task);

        auto& fromParts = hostLeaderMap[std::get<2>(task)][space];
        auto it = std::find(fromParts.begin(), fromParts.end(), part);
        ASSERT_TRUE(it != fromParts.end());
        fromParts.erase(it);

        auto& toParts = hostLeaderMap[std::get<3>(task)][space];
        toParts.emplace_back(part);
    }

    for (const auto& hostEntry : hostLeaderMap) {
        for (const auto& entry : hostEntry.second) {
            EXPECT_GE(entry.second.size(), minLoad);
            EXPECT_LE(entry.second.size(), maxLoad);
        }
    }
}

TEST(BalanceLeaderTest, SimpleLeaderBalancePlanTest) {
    fs::TempDir rootPath("/tmp/SimpleLeaderBalancePlanTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    std::vector<HostAddr> hosts = {{"0", 0}, {"1", 1}, {"2", 2}};
    TestUtils::createSomeHosts(kv, hosts);
    // 9 partition in space 1, 3 replica, 3 hosts
    TestUtils::assembleSpace(kv, 1, 9, 3, 3);

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    NiceMock<MockAdminClient> client;

    auto* processor = LeaderBalanceProcessor::instance(kv);
    processor->space_ = 1;
    {
        HostLeaderMap hostLeaderMap;
        hostLeaderMap[HostAddr("0", 0)][1] = {1, 2, 3, 4, 5};
        hostLeaderMap[HostAddr("1", 1)][1] = {6, 7, 8};
        hostLeaderMap[HostAddr("2", 2)][1] = {9};
        auto tempMap = hostLeaderMap;

        LeaderBalancePlan plan;
        auto leaderBalanceResult = processor->buildLeaderBalancePlan(&hostLeaderMap, 3,
                                                                     false, plan, false);
        ASSERT_TRUE(nebula::ok(leaderBalanceResult) && nebula::value(leaderBalanceResult));
        verifyLeaderBalancePlan(hostLeaderMap, plan, 3, 3);

        // check two plan build are same
        LeaderBalancePlan tempPlan;
        auto tempLeaderBalanceResult = processor->buildLeaderBalancePlan(&tempMap, 3, false,
                                                                         tempPlan, false);
        ASSERT_TRUE(nebula::ok(tempLeaderBalanceResult) && nebula::value(tempLeaderBalanceResult));
        verifyLeaderBalancePlan(tempMap, tempPlan, 3, 3);

        EXPECT_EQ(plan.size(), tempPlan.size());
        for (size_t i = 0; i < plan.size(); i++) {
            EXPECT_EQ(plan[i], tempPlan[i]);
        }
    }
    {
        HostLeaderMap hostLeaderMap;
        hostLeaderMap[HostAddr("0", 0)][1] = {1, 2, 3, 4};
        hostLeaderMap[HostAddr("1", 1)][1] = {5, 6, 7, 8};
        hostLeaderMap[HostAddr("2", 2)][1] = {9};

        LeaderBalancePlan plan;
        auto leaderBalanceResult = processor->buildLeaderBalancePlan(&hostLeaderMap, 3,
                                                                     false, plan, false);
        ASSERT_TRUE(nebula::ok(leaderBalanceResult) && nebula::value(leaderBalanceResult));
        verifyLeaderBalancePlan(hostLeaderMap, plan, 3, 3);
    }
    {
        HostLeaderMap hostLeaderMap;
        hostLeaderMap[HostAddr("0", 0)][1] = {};
        hostLeaderMap[HostAddr("1", 1)][1] = {};
        hostLeaderMap[HostAddr("2", 2)][1] = {1, 2, 3, 4, 5, 6, 7, 8, 9};

        LeaderBalancePlan plan;
        auto leaderBalanceResult = processor->buildLeaderBalancePlan(&hostLeaderMap, 3,
                                                                     false, plan, false);
        ASSERT_TRUE(nebula::ok(leaderBalanceResult) && nebula::value(leaderBalanceResult));
        verifyLeaderBalancePlan(hostLeaderMap, plan, 3, 3);
    }
    {
        HostLeaderMap hostLeaderMap;
        hostLeaderMap[HostAddr("0", 0)][1] = {1, 2, 3};
        hostLeaderMap[HostAddr("1", 1)][1] = {4, 5, 6};
        hostLeaderMap[HostAddr("2", 2)][1] = {7, 8, 9};

        LeaderBalancePlan plan;
        auto leaderBalanceResult = processor->buildLeaderBalancePlan(&hostLeaderMap, 3,
                                                                     false, plan, false);
        ASSERT_TRUE(nebula::ok(leaderBalanceResult) && nebula::value(leaderBalanceResult));
        verifyLeaderBalancePlan(hostLeaderMap, plan, 3, 3);
    }
}

TEST(BalanceLeaderTest, IntersectHostsLeaderBalancePlanTest) {
    fs::TempDir rootPath("/tmp/IntersectHostsLeaderBalancePlanTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    std::vector<HostAddr> hosts = {{"0", 0}, {"1", 1}, {"2", 2}, {"3", 3}, {"4", 4}, {"5", 5}};
    TestUtils::createSomeHosts(kv, hosts);
    // 7 partition in space 1, 3 replica, 6 hosts, so not all hosts have intersection parts
    TestUtils::assembleSpace(kv, 1, 7, 3, 6);

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    NiceMock<MockAdminClient> client;
    auto* processor = LeaderBalanceProcessor::instance(kv);
    processor->space_ = 1;

    {
        HostLeaderMap hostLeaderMap;
        hostLeaderMap[HostAddr("0", 0)][1] = {4, 5, 6};
        hostLeaderMap[HostAddr("1", 1)][1] = {};
        hostLeaderMap[HostAddr("2", 2)][1] = {};
        hostLeaderMap[HostAddr("3", 3)][1] = {1, 2, 3, 7};
        hostLeaderMap[HostAddr("4", 4)][1] = {};
        hostLeaderMap[HostAddr("5", 5)][1] = {};
        showHostLoading(kv, 1);

        LeaderBalancePlan plan;
        auto leaderBalanceResult = processor->buildLeaderBalancePlan(&hostLeaderMap, 3,
                                                                     false, plan, false);
        ASSERT_TRUE(nebula::ok(leaderBalanceResult) && nebula::value(leaderBalanceResult));
        verifyLeaderBalancePlan(hostLeaderMap, plan, 1, 2);
    }
    {
        HostLeaderMap hostLeaderMap;
        hostLeaderMap[HostAddr("0", 0)][1] = {};
        hostLeaderMap[HostAddr("1", 1)][1] = {5, 6, 7};
        hostLeaderMap[HostAddr("2", 2)][1] = {};
        hostLeaderMap[HostAddr("3", 3)][1] = {1, 2};
        hostLeaderMap[HostAddr("4", 4)][1] = {};
        hostLeaderMap[HostAddr("5", 5)][1] = {3, 4};

        LeaderBalancePlan plan;
        auto leaderBalanceResult = processor->buildLeaderBalancePlan(&hostLeaderMap, 3,
                                                                     false, plan, false);
        ASSERT_TRUE(nebula::ok(leaderBalanceResult) && nebula::value(leaderBalanceResult));
        verifyLeaderBalancePlan(hostLeaderMap, plan, 1, 2);
    }
    {
        HostLeaderMap hostLeaderMap;
        hostLeaderMap[HostAddr("0", 0)][1] = {};
        hostLeaderMap[HostAddr("1", 1)][1] = {1, 5};
        hostLeaderMap[HostAddr("2", 2)][1] = {2, 6};
        hostLeaderMap[HostAddr("3", 3)][1] = {3, 7};
        hostLeaderMap[HostAddr("4", 4)][1] = {4};
        hostLeaderMap[HostAddr("5", 5)][1] = {};

        LeaderBalancePlan plan;
        auto leaderBalanceResult = processor->buildLeaderBalancePlan(&hostLeaderMap, 3,
                                                                     false, plan, false);
        ASSERT_TRUE(nebula::ok(leaderBalanceResult) && nebula::value(leaderBalanceResult));
        verifyLeaderBalancePlan(hostLeaderMap, plan, 1, 2);
    }
    {
        HostLeaderMap hostLeaderMap;
        hostLeaderMap[HostAddr("0", 0)][1] = {5, 6};
        hostLeaderMap[HostAddr("1", 1)][1] = {1, 7};
        hostLeaderMap[HostAddr("2", 2)][1] = {};
        hostLeaderMap[HostAddr("3", 3)][1] = {};
        hostLeaderMap[HostAddr("4", 4)][1] = {2, 3, 4};
        hostLeaderMap[HostAddr("5", 5)][1] = {};

        LeaderBalancePlan plan;
        auto leaderBalanceResult = processor->buildLeaderBalancePlan(&hostLeaderMap, 3,
                                                                     false, plan, false);
        ASSERT_TRUE(nebula::ok(leaderBalanceResult) && nebula::value(leaderBalanceResult));
        verifyLeaderBalancePlan(hostLeaderMap, plan, 1, 2);
    }
    {
        HostLeaderMap hostLeaderMap;
        hostLeaderMap[HostAddr("0", 0)][1] = {6};
        hostLeaderMap[HostAddr("1", 1)][1] = {1, 7};
        hostLeaderMap[HostAddr("2", 2)][1] = {2};
        hostLeaderMap[HostAddr("3", 3)][1] = {3};
        hostLeaderMap[HostAddr("4", 4)][1] = {4};
        hostLeaderMap[HostAddr("5", 5)][1] = {5};

        LeaderBalancePlan plan;
        auto leaderBalanceResult = processor->buildLeaderBalancePlan(&hostLeaderMap, 3,
                                                                     false, plan, false);
        ASSERT_TRUE(nebula::ok(leaderBalanceResult) && nebula::value(leaderBalanceResult));
        verifyLeaderBalancePlan(hostLeaderMap, plan, 1, 2);
    }
}

TEST(BalanceLeaderTest, ManyHostsLeaderBalancePlanTest) {
    fs::TempDir rootPath("/tmp/SimpleLeaderBalancePlanTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    FLAGS_heartbeat_interval_secs = 600;

    int partCount = 99999;
    int replica = 3;
    int hostCount = 100;
    std::vector<HostAddr> hosts;
    for (int i = 0; i < hostCount; i++) {
        hosts.emplace_back(std::to_string(i), i);
    }
    TestUtils::createSomeHosts(kv, hosts);
    TestUtils::assembleSpace(kv, 1, partCount, replica, hostCount);

    float avgLoad = static_cast<float>(partCount) / hostCount;
    int32_t minLoad = std::floor(avgLoad * (1 - FLAGS_leader_balance_deviation));
    int32_t maxLoad = std::ceil(avgLoad * (1 + FLAGS_leader_balance_deviation));

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    NiceMock<MockAdminClient> client;
    auto* processor = LeaderBalanceProcessor::instance(kv);
    processor->space_ = 1;

    // chcek several times if they are balanced
    for (int count = 0; count < 1; count++) {
        HostLeaderMap hostLeaderMap;
        // all part will random choose a leader
        for (int partId = 1; partId <= partCount; partId++) {
            std::vector<HostAddr> peers;
            size_t idx = partId;
            for (int32_t i = 0; i < replica; i++, idx++) {
                peers.emplace_back(hosts[idx % hostCount]);
            }
            ASSERT_EQ(peers.size(), replica);
            auto leader = peers[folly::Random::rand32(peers.size())];
            hostLeaderMap[leader][1].emplace_back(partId);
        }

        LeaderBalancePlan plan;
        auto leaderBalanceResult = processor->buildLeaderBalancePlan(&hostLeaderMap, 3,
                                                                     false, plan);
        ASSERT_TRUE(nebula::ok(leaderBalanceResult) && nebula::value(leaderBalanceResult));
        verifyLeaderBalancePlan(hostLeaderMap, plan, minLoad, maxLoad);
    }
}

TEST(BalanceLeaderTest, LeaderBalanceTest) {
    fs::TempDir rootPath("/tmp/LeaderBalanceTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    FLAGS_heartbeat_interval_secs = 10;
    std::vector<HostAddr> hosts = {{"0", 0}, {"1", 1}, {"2", 2}};
    TestUtils::createSomeHosts(kv, hosts);
    GraphSpaceID spaceId = 1;
    TestUtils::assembleSpace(kv, spaceId, 9, 3, 3);

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    NiceMock<MockAdminClient> client;
    HostLeaderMap dist;
    dist[HostAddr("0", 0)][1] = {1, 2, 3, 4, 5};
    dist[HostAddr("1", 1)][1] = {6, 7, 8};
    dist[HostAddr("2", 2)][1] = {9};
    EXPECT_CALL(client, getLeaderDist(_))
        .WillOnce(
            DoAll(SetArgPointee<0>(dist), Return(ByMove(folly::Future<Status>(Status::OK())))));

    auto* processor = LeaderBalanceProcessor::instance(kv);
    processor->space_ = 1;
    processor->client_.reset(&client);
    auto ret = processor->leaderBalance();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
}

TEST(BalanceLeaderTest, LeaderBalanceWithZoneTest) {
    fs::TempDir rootPath("/tmp/LeaderBalanceWithZone.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    FLAGS_heartbeat_interval_secs = 10;
    std::vector<HostAddr> hosts;
    for (int i = 0; i < 9; i++) {
        hosts.emplace_back(std::to_string(i), i);
    }
    TestUtils::createSomeHosts(kv, hosts);
    TestUtils::registerHB(kv, hosts);

    // create zone and group
    {
        ZoneInfo zoneInfo = {
            {"zone_0", {HostAddr("0", 0), HostAddr("1", 1)}},
            {"zone_1", {HostAddr("2", 2), HostAddr("3", 3)}},
            {"zone_2", {HostAddr("4", 4), HostAddr("5", 5)}}
        };
        GroupInfo groupInfo = {
            {"default_group", {"zone_0", "zone_1", "zone_2"}}
        };

        TestUtils::assembleGroupAndZone(kv, zoneInfo, groupInfo);
        TestUtils::createSpace(kv, "default_space", 8, 3, "default_group");
    }

    sleep(1);
    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    NiceMock<MockAdminClient> client;
    auto* processor = LeaderBalanceProcessor::instance(kv);
    processor->space_ = 1;

    {
        HostLeaderMap hostLeaderMap;
        hostLeaderMap[HostAddr("0", 0)][1] = {1, 3, 5, 7};
        hostLeaderMap[HostAddr("1", 1)][1] = {2, 4, 6, 8};
        hostLeaderMap[HostAddr("2", 2)][1] = {};
        hostLeaderMap[HostAddr("3", 3)][1] = {};
        hostLeaderMap[HostAddr("4", 4)][1] = {};
        hostLeaderMap[HostAddr("5", 5)][1] = {};

        LeaderBalancePlan plan;
        auto leaderBalanceResult = processor->buildLeaderBalancePlan(&hostLeaderMap, 3,
                                                                     true, plan, true);
        ASSERT_TRUE(nebula::ok(leaderBalanceResult));
        ASSERT_TRUE(nebula::value(leaderBalanceResult));
        verifyLeaderBalancePlan(hostLeaderMap, plan, 1, 2);
    }
    {
        HostLeaderMap hostLeaderMap;
        hostLeaderMap[HostAddr("0", 0)][1] = {1, 3};
        hostLeaderMap[HostAddr("1", 1)][1] = {2, 4};
        hostLeaderMap[HostAddr("2", 2)][1] = {5, 7};
        hostLeaderMap[HostAddr("3", 3)][1] = {6, 8};
        hostLeaderMap[HostAddr("4", 4)][1] = {};
        hostLeaderMap[HostAddr("5", 5)][1] = {};

        LeaderBalancePlan plan;
        auto leaderBalanceResult = processor->buildLeaderBalancePlan(&hostLeaderMap, 3,
                                                                     true, plan, true);
        ASSERT_TRUE(nebula::ok(leaderBalanceResult) && nebula::value(leaderBalanceResult));
        verifyLeaderBalancePlan(hostLeaderMap, plan, 1, 2);
    }
}

TEST(BalanceLeaderTest, LeaderBalanceWithLargerZoneTest) {
    fs::TempDir rootPath("/tmp/LeaderBalanceWithLargerZoneTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    FLAGS_heartbeat_interval_secs = 10;
    std::vector<HostAddr> hosts;
    for (int i = 0; i < 15; i++) {
        hosts.emplace_back(std::to_string(i), i);
    }
    TestUtils::createSomeHosts(kv, hosts);
    TestUtils::registerHB(kv, hosts);

    // create zone and group
    {
        ZoneInfo zoneInfo = {
            {"zone_0", {HostAddr("0", 0), HostAddr("1", 1)}},
            {"zone_1", {HostAddr("2", 2), HostAddr("3", 3)}},
            {"zone_2", {HostAddr("4", 4), HostAddr("5", 5)}},
            {"zone_3", {HostAddr("6", 6), HostAddr("7", 7)}},
            {"zone_4", {HostAddr("8", 8), HostAddr("9", 9)}},
        };
        GroupInfo groupInfo = {
            {"default_group", {"zone_0", "zone_1", "zone_2", "zone_3", "zone_4"}}
        };

        TestUtils::assembleGroupAndZone(kv, zoneInfo, groupInfo);
        TestUtils::createSpace(kv, "default_space", 8, 3, "default_group");
    }

    sleep(1);
    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    NiceMock<MockAdminClient> client;
    auto* processor = LeaderBalanceProcessor::instance(kv);
    processor->space_ = 1;
    {
        HostLeaderMap hostLeaderMap;
        hostLeaderMap[HostAddr("0", 0)][1] = {1, 5, 8};
        hostLeaderMap[HostAddr("1", 1)][1] = {3, 6, 7};
        hostLeaderMap[HostAddr("2", 2)][1] = {2};
        hostLeaderMap[HostAddr("3", 3)][1] = {4};
        hostLeaderMap[HostAddr("4", 4)][1] = {};
        hostLeaderMap[HostAddr("5", 5)][1] = {};
        hostLeaderMap[HostAddr("6", 6)][1] = {};
        hostLeaderMap[HostAddr("7", 7)][1] = {};
        hostLeaderMap[HostAddr("8", 8)][1] = {};

        LeaderBalancePlan plan;
        auto leaderBalanceResult = processor->buildLeaderBalancePlan(&hostLeaderMap, 3,
                                                                     true, plan, true);
        ASSERT_TRUE(nebula::ok(leaderBalanceResult) && nebula::value(leaderBalanceResult));
        verifyLeaderBalancePlan(hostLeaderMap, plan, 0, 1);
    }
}

TEST(BalanceLeaderTest, LeaderBalanceWithComplexZoneTest) {
    fs::TempDir rootPath("/tmp/LeaderBalanceWithComplexZoneTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    FLAGS_heartbeat_interval_secs = 10;
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
        auto spaceId = TestUtils::createSpace(kv, "default_space", 9, 3);
        showHostLoading(kv, spaceId);
        spaceId = TestUtils::createSpace(kv, "space_on_group_0", 64, 3, "group_0");
        showHostLoading(kv, spaceId);
        spaceId = TestUtils::createSpace(kv, "space_on_group_1", 81, 3, "group_1");
        showHostLoading(kv, spaceId);
    }

    showHostLoading(kv, 3);
    sleep(1);

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });

    NiceMock<MockAdminClient> client;
    auto* processor = LeaderBalanceProcessor::instance(kv);
    processor->space_ = 3;
    {
        HostLeaderMap hostLeaderMap;
        hostLeaderMap[HostAddr("0", 0)][3] = {};
        hostLeaderMap[HostAddr("1", 1)][3] = {};
        hostLeaderMap[HostAddr("2", 2)][3] = {};
        hostLeaderMap[HostAddr("3", 3)][3] = {};
        hostLeaderMap[HostAddr("4", 4)][3] = {62, 68, 74, 80};
        hostLeaderMap[HostAddr("5", 5)][3] = {};
        hostLeaderMap[HostAddr("6", 6)][3] = {};
        hostLeaderMap[HostAddr("7", 7)][3] = {};
        hostLeaderMap[HostAddr("8", 8)][3] = {};
        hostLeaderMap[HostAddr("8", 8)][3] = {};
        hostLeaderMap[HostAddr("9", 9)][3] = {59, 65, 71, 77};
        hostLeaderMap[HostAddr("10", 10)][3] = {61, 67, 73, 79};
        hostLeaderMap[HostAddr("11", 11)][3] = {29, 34, 37, 42, 45, 50, 53, 58, 64, 70, 76};
        hostLeaderMap[HostAddr("12", 12)][3] = {1, 3, 6, 8, 11, 14, 16, 19, 22,
                                                24, 27, 30, 46, 48, 51, 54};
        hostLeaderMap[HostAddr("13", 13)][3] = {10, 15, 18, 31, 52, 69, 81};
        hostLeaderMap[HostAddr("14", 14)][3] = {5, 13, 21, 32, 35, 40, 43, 56, 60, 66, 72, 78};
        hostLeaderMap[HostAddr("15", 15)][3] = {2, 4, 7, 9, 12, 17, 20, 23, 25, 28, 33,
                                                39, 41, 44, 47, 49, 55, 57, 63, 75};
        hostLeaderMap[HostAddr("16", 16)][3] = {26};
        hostLeaderMap[HostAddr("17", 17)][3] = {36, 38};

        LeaderBalancePlan plan;
        auto leaderBalanceResult = processor->buildLeaderBalancePlan(&hostLeaderMap, 3,
                                                                     true, plan, true);
        ASSERT_TRUE(nebula::ok(leaderBalanceResult) && nebula::value(leaderBalanceResult));
        verifyLeaderBalancePlan(hostLeaderMap, plan, 1, 9);
    }
}

}  // namespace meta
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
