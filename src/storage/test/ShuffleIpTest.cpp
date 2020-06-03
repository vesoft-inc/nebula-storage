/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/network/NetworkUtils.h"
#include "common/clients/meta/MetaClient.h"
#include "common/meta/GflagsManager.h"
#include "common/meta/ServerBasedSchemaManager.h"
#include "common/meta/ClientBasedGflagsManager.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "utils/NebulaKeyUtils.h"
#include <folly/executors/ThreadedExecutor.h>

#include "common/base/Base.h"
#include "common/clients/meta/MetaClient.h"
#include "utils/NebulaKeyUtils.h"
#include "common/fs/TempDir.h"
#include "common/meta/ClientBasedGflagsManager.h"
#include "common/meta/GflagsManager.h"
#include "meta/MetaServiceUtils.h"
#include "meta/test/TestUtils.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"

DECLARE_int32(heartbeat_interval_secs);
DECLARE_string(rocksdb_db_options);

DEFINE_int32(storage_client_timeout_ms, 60 * 1000, "storage client timeout");

namespace nebula {
namespace storage {

using nebula::meta::cpp2::PropertyType;
using nebula::meta::cpp2::HBResp;
using nebula::Value;

void printHostItem(nebula::meta::cpp2::HostItem& host) {
    std::ostringstream oss;
    oss << "\nhost: " << host.hostAddr;
    oss << "\nstatus(0:ONLINE, 1:OFFLINE): " << static_cast<int>(host.status);
    oss << "\n, leader_parts: size=" << host.leader_parts.size();
    for (auto& it : host.leader_parts) {
        oss << ", bin: " << it.first;
        for (auto partId : it.second) {
            oss << " " << partId;
        }
    }
    oss << "\n, all_parts: size=" << host.all_parts.size();
    for (auto& it : host.all_parts) {
        oss << ", bin: " << it.first;
        for (auto partId : it.second) {
            oss << " " << partId;
        }
    }
    oss << "\n role = " << static_cast<int>(host.role);
    oss << "\n git_info_sha = " << host.git_info_sha;
    LOG(INFO) << oss.str();
}

std::vector<cpp2::NewVertex> genDummyVertices(int tagId, int howMany) {
    std::vector<cpp2::NewVertex> newVertices;

    int vid = 1000;
    for (int32_t i = 0; i < howMany; i++) {
        storage::cpp2::NewVertex newVertex;
        newVertex.set_id(vid++);

        storage::cpp2::NewTag newTag;
        std::vector<storage::cpp2::NewTag> newTags;
        newTag.set_tag_id(tagId);
        // auto props = genData(FLAGS_size);
        // newTag.set_props(std::move(props));
        newTags.emplace_back(std::move(newTag));

        newVertex.set_tags(std::move(newTags));
        newVertices.emplace_back(std::move(newVertex));
    }
    return newVertices;
}

TEST(ShuffleIpTest, GenData) {
    using StorageClient = storage::GraphStorageClient;
    // need to manual change ip(host), which means has nothing to do
    // in CI autotest, so comments out all the code
    // will available in manual check

    std::string meta_name{"hp-server"};
    uint32_t meta_port = 6500;

    auto ioThreadPool_ = std::make_shared<folly::IOThreadPoolExecutor>(3);

    std::vector<HostAddr> metas;
    metas.emplace_back(HostAddr(meta_name, meta_port));

    meta::MetaClientOptions options;
    auto metaClient = std::make_unique<meta::MetaClient>(ioThreadPool_,
                                                     metas,
                                                     options);
    if (!metaClient->waitForMetadReady()) {
        LOG(ERROR) << "waitForMetadReady error!";
        return;
    }
    meta::SpaceDesc spaceDesc("default2", 3, 1);
    auto futCreateSpace = metaClient->createSpace(spaceDesc);

    futCreateSpace.wait();
    auto spaceId = futCreateSpace.value().value();
    LOG(INFO) << "Created space \"default\", its id is " << spaceId;

    nebula::meta::cpp2::Schema schema;
    nebula::meta::cpp2::ColumnDef column;
    column.name = "name";
    column.type = meta::cpp2::PropertyType::STRING;
    schema.columns.emplace_back(std::move(column));

    auto createTagSchemaRet = metaClient->createTagSchema(spaceId, "player", schema);
    EXPECT_TRUE(createTagSchemaRet.value().ok());
    if (!createTagSchemaRet.value().ok()) {
        LOG(ERROR) << "Create tag failed: " << createTagSchemaRet.value().status();
        return;
    }
    auto tagId = createTagSchemaRet.value().value();

    auto storageClient = std::make_unique<StorageClient>(ioThreadPool_,
                                                         metaClient.get());

    auto vertices = genDummyVertices(tagId, 10);
    std::unordered_map<TagID, std::vector<std::string>> propNames;
    std::vector<std::string> props{"name"};
    propNames.insert(std::make_pair(tagId, props));
    /*
    folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>>
    addVertices(
        GraphSpaceID space,
        std::vector<cpp2::NewVertex> vertices,
        std::unordered_map<TagID, std::vector<std::string>> propNames,
        bool overwritable,
        folly::EventBase* evb = nullptr);
    */
    storageClient->addVertices(spaceId, vertices, propNames, true);

    auto ret = metaClient->listHosts().get();
    ASSERT_TRUE(ret.ok());
    auto hosts = ret.value();
    for (auto& host : hosts) {
        printHostItem(host);
    }
}

TEST(ShuffleIpTest, ListHosts) {
    // need to manual change ip(host), which means has nothing to do
    // in CI autotest, so comments out all the code
    // will available in manual check

    // std::string meta_name{"hp-server"};
    // uint32_t meta_port = 6500;

    // auto ioThreadPool_ = std::make_shared<folly::IOThreadPoolExecutor>(3);

    // std::vector<HostAddr> metas;
    // metas.emplace_back(HostAddr(meta_name, meta_port));

    // meta::MetaClientOptions options;

    // auto client = std::make_unique<meta::MetaClient>(ioThreadPool_,
    //                                                  metas,
    //                                                  options);
    // meta::SpaceDesc spaceDesc("default", 10, 1);
    // auto rc = client->createSpace(spaceDesc);
    // if (!client->waitForMetadReady()) {
    //     LOG(ERROR) << "waitForMetadReady error!";
    //     return;
    // }

    // auto ret = client->listHosts().get();
    // ASSERT_TRUE(ret.ok());
    // auto hosts = ret.value();
    // for (auto& host : hosts) {
    //     printHostItem(host);
    // }
}

TEST(ShuffleIpTest, validateTest) {
    std::vector<std::string> addrs;
    addrs.emplace_back("hp-server");
    addrs.emplace_back("nebula-dev-1");
    addrs.emplace_back("192.168.8.5");

    std::vector<bool> result;
    for (size_t i = 0; i < addrs.size(); ++i) {
        result.push_back(folly::IPAddress::validate(addrs[i]));
    }

    for (size_t i = 0; i < addrs.size(); ++i) {
        LOG(INFO) << addrs[i] << " is valid " <<  result[i];
    }
}

}  // namespace storage
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
