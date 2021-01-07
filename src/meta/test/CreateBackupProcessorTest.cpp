/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "meta/processors/admin/CreateBackupProcessor.h"
#include "meta/test/TestUtils.h"
#include "utils/Utils.h"

namespace nebula {
namespace meta {

#define RETURN_OK(req)                                                                             \
    UNUSED(req);                                                                                   \
    do {                                                                                           \
        folly::Promise<storage::cpp2::AdminExecResp> pro;                                          \
        auto f = pro.getFuture();                                                                  \
        storage::cpp2::AdminExecResp resp;                                                         \
        storage::cpp2::ResponseCommon result;                                                      \
        std::vector<storage::cpp2::PartitionResult> partRetCode;                                   \
        result.set_failed_parts(partRetCode);                                                      \
        resp.set_result(result);                                                                   \
        pro.setValue(std::move(resp));                                                             \
        return f;                                                                                  \
    } while (false)

class TestStorageService : public storage::cpp2::StorageAdminServiceSvIf {
public:
    folly::Future<storage::cpp2::AdminExecResp> future_addPart(
        const storage::cpp2::AddPartReq& req) override {
        RETURN_OK(req);
    }

    folly::Future<storage::cpp2::CreateCPResp> future_createCheckpoint(
        const storage::cpp2::CreateCPRequest& req) override {
        UNUSED(req);
        folly::Promise<storage::cpp2::CreateCPResp> pro;
        auto f = pro.getFuture();
        storage::cpp2::CreateCPResp resp;
        storage::cpp2::ResponseCommon result;
        std::vector<storage::cpp2::PartitionResult> partRetCode;
        result.set_failed_parts(partRetCode);
        resp.set_result(result);
        resp.set_path("snapshot_path");
        pro.setValue(std::move(resp));
        return f;
    }

    folly::Future<storage::cpp2::AdminExecResp> future_dropCheckpoint(
        const storage::cpp2::DropCPRequest& req) override {
        RETURN_OK(req);
    }

    folly::Future<storage::cpp2::AdminExecResp> future_blockingWrites(
        const storage::cpp2::BlockingSignRequest& req) override {
        RETURN_OK(req);
    }
};

TEST(ProcessorTest, CreateBackupTest) {
    auto rpcServer = std::make_unique<mock::RpcServer>();
    auto handler = std::make_shared<TestStorageService>();
    rpcServer->start("storage-admin", 0, handler);
    LOG(INFO) << "Start storage server on " << rpcServer->port_;

    std::string localIp("127.0.0.1");

    LOG(INFO) << "Now test interfaces with retry to leader!";

    fs::TempDir rootPath("/tmp/create_backup_test.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
    auto now = time::WallClock::fastNowInMilliSec();
    HostAddr host(localIp, rpcServer->port_);
    ActiveHostsMan::updateHostInfo(
        kv.get(), host, HostInfo(now, meta::cpp2::HostRole::STORAGE, ""));

    HostAddr storageHost = Utils::getStoreAddrFromAdminAddr(host);

    auto client = std::make_unique<AdminClient>(kv.get());
    std::vector<HostAddr> hosts;
    hosts.emplace_back(host);
    meta::TestUtils::registerHB(kv.get(), hosts);

    // mock admin client
    bool ret = false;
    cpp2::SpaceDesc properties;
    GraphSpaceID id = 1;
    properties.set_space_name("test_space");
    properties.set_partition_num(1);
    properties.set_replica_factor(1);
    auto spaceVal = MetaServiceUtils::spaceVal(properties);
    std::vector<nebula::kvstore::KV> data;
    data.emplace_back(MetaServiceUtils::indexSpaceKey("test_space"),
                      std::string(reinterpret_cast<const char*>(&id), sizeof(GraphSpaceID)));
    data.emplace_back(MetaServiceUtils::spaceKey(id), MetaServiceUtils::spaceVal(properties));

    std::string indexName = "test_space_index";
    int32_t tagIndex = 2;

    cpp2::IndexItem item;
    item.set_index_id(tagIndex);
    item.set_index_name(indexName);
    cpp2::SchemaID schemaID;
    TagID tagID = 3;
    std::string tagName = "test_space_tag1";
    schemaID.set_tag_id(tagID);
    item.set_schema_id(schemaID);
    item.set_schema_name(tagName);
    data.emplace_back(MetaServiceUtils::indexIndexKey(id, indexName),
                      std::string(reinterpret_cast<const char*>(&tagIndex), sizeof(IndexID)));
    data.emplace_back(MetaServiceUtils::indexKey(id, tagIndex), MetaServiceUtils::indexVal(item));

    std::vector<HostAddr> allHosts;
    allHosts.emplace_back(storageHost);

    for (auto partId = 1; partId <= 1; partId++) {
        std::vector<HostAddr> hosts2;
        size_t idx = partId;
        for (int32_t i = 0; i < 1; i++, idx++) {
            hosts2.emplace_back(allHosts[idx % 1]);
        }
        data.emplace_back(MetaServiceUtils::partKey(id, partId), MetaServiceUtils::partVal(hosts2));
    }
    folly::Baton<true, std::atomic> baton;
    kv->asyncMultiPut(0, 0, std::move(data), [&](kvstore::ResultCode code) {
        ret = (code == kvstore::ResultCode::SUCCEEDED);
        baton.post();
    });
    baton.wait();

    {
        cpp2::CreateBackupReq req;
        auto* processor = CreateBackupProcessor::instance(kv.get(), client.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        LOG(INFO) << folly::to<int>(resp.get_code());
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        auto meta = resp.get_meta();

        auto metaFiles = meta.get_meta_files();
        for (auto m : metaFiles) {
            LOG(INFO) << "meta files name:" << m;
        }

        auto it = std::find_if(metaFiles.cbegin(), metaFiles.cend(), [](auto const& m) {
            auto name = m.substr(m.size() - sizeof("__indexes__.sst") + 1);

            if (name == "__indexes__.sst") {
                return true;
            }
            return false;
        });

        ASSERT_NE(it, metaFiles.cend());

        ASSERT_EQ(1, meta.get_backup_info().size());
        for (auto s : meta.get_backup_info()) {
            ASSERT_EQ(1, s.first);
            ASSERT_EQ(1, s.second.get_cp_dirs().size());
            auto checkInfo = s.second.get_cp_dirs()[0];
            ASSERT_EQ("snapshot_path", checkInfo.get_checkpoint_dir());
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
