/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "meta/processors/admin/RestoreProcessor.h"
#include "meta/test/TestUtils.h"

namespace nebula {
namespace meta {

TEST(RestoreProcessorTest, RestoreTest) {
    fs::TempDir rootPath("/tmp/RestoreOriginTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
    auto now = time::WallClock::fastNowInMilliSec();

    HostAddr host1("127.0.0.1", 3360);
    HostAddr host2("127.0.0.2", 3360);
    HostAddr host3("127.0.0.3", 3360);

    std::vector<HostAddr> hosts;
    hosts.emplace_back(host1);
    hosts.emplace_back(host2);
    hosts.emplace_back(host3);

    for (auto h : hosts) {
        ActiveHostsMan::updateHostInfo(
            kv.get(), h, HostInfo(now, meta::cpp2::HostRole::STORAGE, ""));
    }

    meta::TestUtils::registerHB(kv.get(), hosts);

    // mock admin client
    bool ret = false;
    cpp2::SpaceDesc properties;
    GraphSpaceID id = 1;
    properties.set_space_name("test_space");
    int partNum = 10;
    properties.set_partition_num(partNum);
    properties.set_replica_factor(3);

    auto spaceVal = MetaServiceUtils::spaceVal(properties);
    std::vector<nebula::kvstore::KV> data;
    data.emplace_back(MetaServiceUtils::indexSpaceKey("test_space"),
                      std::string(reinterpret_cast<const char*>(&id), sizeof(GraphSpaceID)));
    data.emplace_back(MetaServiceUtils::spaceKey(id), MetaServiceUtils::spaceVal(properties));

    std::unordered_map<HostAddr, std::vector<size_t>> partInfo;

    for (auto partId = 1; partId <= partNum; partId++) {
        std::vector<HostAddr> hosts4;
        size_t idx = partId;
        for (int32_t i = 0; i < 3; i++, idx++) {
            auto h = hosts[idx % 3];
            hosts4.emplace_back(h);
            partInfo[h].emplace_back(partId);
        }
        data.emplace_back(MetaServiceUtils::partKey(id, partId), MetaServiceUtils::partVal(hosts4));
    }
    folly::Baton<true, std::atomic> baton;
    kv->asyncMultiPut(0, 0, std::move(data), [&](kvstore::ResultCode code) {
        ret = (code == kvstore::ResultCode::SUCCEEDED);
        baton.post();
    });
    baton.wait();

    std::unordered_set<GraphSpaceID> spaces = {id};
    auto backupName = folly::format("BACKUP_{}", MetaServiceUtils::genTimestampStr()).str();
    auto partFile = MetaServiceUtils::backupPartsTable(kv.get(), spaces, backupName);
    DCHECK(ok(partFile));
    auto spaceNames = std::make_unique<std::vector<std::string>>();
    spaceNames->emplace_back("test_space");
    auto indexFile =
        MetaServiceUtils::backupIndexTable(kv.get(), spaces, backupName, spaceNames.get());
    DCHECK(ok(indexFile));
    auto spaceFile = MetaServiceUtils::backupSpaceTable(kv.get(), spaces, backupName);
    DCHECK(ok(spaceFile));

    {
        fs::TempDir rootRestorePath("/tmp/RestoreTest.XXXXXX");
        std::unique_ptr<kvstore::KVStore> kvRestore(
            MockCluster::initMetaKV(rootRestorePath.path()));
        cpp2::RestoreMetaReq req;
        std::vector<std::string> files = {
            value(partFile)[0], value(indexFile)[0], value(spaceFile)[0]};
        req.set_files(std::move(files));
        std::vector<cpp2::HostPair> hostPairs;
        HostAddr host4("127.0.0.4", 3360);
        HostAddr host5("127.0.0.5", 3360);
        HostAddr host6("127.0.0.6", 3360);
        std::unordered_map<HostAddr, HostAddr> hostMap = {
            {host1, host4}, {host2, host5}, {host3, host6}};

        for (auto hm : hostMap) {
            hostPairs.emplace_back(apache::thrift::FragileConstructor(), hm.first, hm.second);
        }

        req.set_hosts(std::move(hostPairs));

        auto* processor = RestoreProcessor::instance(kvRestore.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());

        kvstore::ResultCode result;
        std::unique_ptr<kvstore::KVIterator> iter;

        const auto& partPrefix = MetaServiceUtils::partPrefix(id);
        result = kvRestore->prefix(kDefaultSpaceId, kDefaultPartId, partPrefix, &iter);
        ASSERT_EQ(kvstore::ResultCode::SUCCEEDED, result);

        std::unordered_map<HostAddr, std::vector<size_t>> toPartInfo;

        while (iter->valid()) {
            auto key = iter->key();
            auto partId = MetaServiceUtils::parsePartKeyPartId(key);
            auto partHosts = MetaServiceUtils::parsePartVal(iter->val());
            for (auto& host : partHosts) {
                LOG(INFO) << "partHost: " << host.toString();
                toPartInfo[host].emplace_back(partId);
            }
            iter->next();
        }

        for (auto pi : partInfo) {
            auto parts = toPartInfo[hostMap[pi.first]];
            ASSERT_EQ(parts.size(), pi.second.size());
            ASSERT_TRUE(std::equal(parts.cbegin(), parts.cend(), pi.second.cbegin()));
        }
    }
}

}   // namespace meta
}   // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
