/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include <gtest/gtest.h>
#include <folly/synchronization/Baton.h>
#include "meta/ActiveHostsMan.h"
#include "meta/test/TestUtils.h"

DECLARE_int32(heartbeat_interval_secs);
DECLARE_uint32(expired_time_factor);

namespace nebula {
namespace meta {

TEST(ActiveHostsManTest, EncodeDecodeHostInfoV2) {
    auto now = time::WallClock::fastNowInMilliSec();
    auto role = cpp2::HostRole::STORAGE;
    std::string strGitInfoSHA = gitInfoSha();
    {
        HostInfo hostInfo(now, role, strGitInfoSHA);
        auto encodeHostInfo = HostInfo::encodeV2(hostInfo);

        auto decodeHostInfo = HostInfo::decode(encodeHostInfo);
        ASSERT_EQ(hostInfo.lastHBTimeInMilliSec_, decodeHostInfo.lastHBTimeInMilliSec_);
        ASSERT_EQ(hostInfo.role_, decodeHostInfo.role_);
        ASSERT_EQ(hostInfo.gitInfoSha_, decodeHostInfo.gitInfoSha_);
    }
    {
        HostInfo hostInfo(now);
        auto encodeHostInfo = HostInfo::encodeV2(hostInfo);

        auto decodeHostInfo = HostInfo::decode(encodeHostInfo);
        ASSERT_EQ(hostInfo.lastHBTimeInMilliSec_, decodeHostInfo.lastHBTimeInMilliSec_);
        ASSERT_EQ(hostInfo.role_, decodeHostInfo.role_);
        ASSERT_EQ(hostInfo.gitInfoSha_, decodeHostInfo.gitInfoSha_);
    }
    {
        HostInfo hostInfo(now, role, strGitInfoSHA);
        auto encodeHostInfo = HostInfo::encodeV2(hostInfo);

        auto decodeHostInfo = HostInfo::decode(encodeHostInfo);
        ASSERT_EQ(hostInfo.lastHBTimeInMilliSec_, decodeHostInfo.lastHBTimeInMilliSec_);
    }
}

TEST(ActiveHostsManTest, NormalTest) {
    fs::TempDir rootPath("/tmp/ActiveHostsManTest.XXXXXX");
    FLAGS_heartbeat_interval_secs = 1;
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
    auto now = time::WallClock::fastNowInMilliSec();
    HostInfo info1(now, cpp2::HostRole::STORAGE, gitInfoSha());
    ActiveHostsMan::updateHostInfo(kv.get(), HostAddr("0", 0), info1);
    ActiveHostsMan::updateHostInfo(kv.get(), HostAddr("0", 1), info1);
    ActiveHostsMan::updateHostInfo(kv.get(), HostAddr("0", 2), info1);
    ASSERT_EQ(3, ActiveHostsMan::getActiveHosts(kv.get()).size());

    HostInfo info2(now + 2000, cpp2::HostRole::STORAGE, gitInfoSha());
    ActiveHostsMan::updateHostInfo(kv.get(), HostAddr("0", 0), info2);
    ASSERT_EQ(3, ActiveHostsMan::getActiveHosts(kv.get()).size());
    {
        const auto& prefix = MetaServiceUtils::hostPrefix();
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        CHECK_EQ(kvstore::ResultCode::SUCCEEDED, ret);
        int i = 0;
        while (iter->valid()) {
            auto host = MetaServiceUtils::parseHostKey(iter->key());
            HostInfo info = HostInfo::decode(iter->val());
            ASSERT_EQ(HostAddr("0", i), HostAddr(host.host, host.port));
            if (i != 0) {
                ASSERT_EQ(info1, info);
            } else {
                ASSERT_EQ(info2, info);
            }
            iter->next();
            i++;
        }
        ASSERT_EQ(3, i);
    }

    sleep(FLAGS_heartbeat_interval_secs * FLAGS_expired_time_factor + 1);
    ASSERT_EQ(1, ActiveHostsMan::getActiveHosts(kv.get()).size());
}

TEST(ActiveHostsManTest, LeaderTest) {
    fs::TempDir rootPath("/tmp/ActiveHostsManTest.XXXXXX");
    FLAGS_heartbeat_interval_secs = 1;
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
    auto now = time::WallClock::fastNowInMilliSec();

    HostInfo hInfo1(now, cpp2::HostRole::STORAGE, gitInfoSha());
    HostInfo hInfo2(now + 2000, cpp2::HostRole::STORAGE, gitInfoSha());
    ActiveHostsMan::updateHostInfo(kv.get(), HostAddr("0", 0), hInfo1);
    ActiveHostsMan::updateHostInfo(kv.get(), HostAddr("0", 1), hInfo1);
    ActiveHostsMan::updateHostInfo(kv.get(), HostAddr("0", 2), hInfo1);
    ASSERT_EQ(3, ActiveHostsMan::getActiveHosts(kv.get()).size());

    auto makePartInfo = [](int partId) {
        cpp2::LeaderInfo part;
        part.set_part_id(partId);
        part.set_term(partId * 1024);
        return part;
    };

    auto cmpPartInfo = [](auto& a, auto& b) {
        if (a.get_part_id() == b.get_part_id()) {
            return a.get_term() < b.get_term();
        }
        return a.get_part_id() < b.get_part_id();
    };

    auto part1 = makePartInfo(1);
    auto part2 = makePartInfo(2);
    auto part3 = makePartInfo(3);

    std::vector<cpp2::LeaderInfo> parts{part1, part2, part3};
    std::sort(parts.begin(), parts.end(), cmpPartInfo);

    HostAddr host("0", 0);

    std::unordered_map<GraphSpaceID, std::vector<cpp2::LeaderInfo>> leaderIds;
    leaderIds.emplace(1, std::vector<cpp2::LeaderInfo>{part1, part2});
    leaderIds.emplace(2, std::vector<cpp2::LeaderInfo>{part3});
    ActiveHostsMan::updateHostInfo(kv.get(), host, hInfo2, &leaderIds);

    EXPECT_EQ(3, ActiveHostsMan::getActiveHosts(kv.get()).size());
    using Key = std::pair<GraphSpaceID, PartitionID>;
    using Value = std::pair<HostAddr, int64_t>;
    auto converter = [](auto& result) {
        return std::make_pair(std::get<0>(result), std::get<1>(result));
    };

    std::map<Key, Value> results;
    {
        const auto& prefix = MetaServiceUtils::leaderPrefix();
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        CHECK_EQ(kvstore::ResultCode::SUCCEEDED, ret);
        int i = 0;
        while (iter->valid()) {
            auto spaceAndPart = MetaServiceUtils::parseLeaderKeyV3(iter->key());
            auto hostAndTerm = MetaServiceUtils::parseLeaderValV3(iter->val());
            results[spaceAndPart] = converter(hostAndTerm);
            iter->next();
            i++;
        }
        EXPECT_EQ(results.size(), 3);
        EXPECT_EQ(i, 3);
    }

    sleep(FLAGS_heartbeat_interval_secs * FLAGS_expired_time_factor + 1);
    ASSERT_EQ(1, ActiveHostsMan::getActiveHosts(kv.get()).size());
}

TEST(LastUpdateTimeManTest, NormalTest) {
    fs::TempDir rootPath("/tmp/LastUpdateTimeManTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));

    ASSERT_EQ(0, LastUpdateTimeMan::get(kv.get()));
    int64_t now = time::WallClock::fastNowInMilliSec();

    LastUpdateTimeMan::update(kv.get(), now);
    ASSERT_EQ(now, LastUpdateTimeMan::get(kv.get()));
    LastUpdateTimeMan::update(kv.get(), now + 100);
    ASSERT_EQ(now + 100, LastUpdateTimeMan::get(kv.get()));

    LastUpdateTimeMan::update(kv.get(), now - 100);
    {
        auto key = MetaServiceUtils::lastUpdateTimeKey();
        std::string val;
        auto ret = kv->get(kDefaultSpaceId, kDefaultPartId, key, &val);
        ASSERT_EQ(ret, kvstore::ResultCode::SUCCEEDED);
        int64_t lastUpdateTime = *reinterpret_cast<const int64_t*>(val.data());
        ASSERT_EQ(now - 100, lastUpdateTime);
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
