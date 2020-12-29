/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


#include <gtest/gtest.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/synchronization/Baton.h>

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/meta/ServerBasedSchemaManager.h"
#include "common/meta/ServerBasedIndexManager.h"
#include "common/clients/storage/GraphStorageClient.h"
#include "kvstore/PartManager.h"
#include "meta/processors/admin/Balancer.h"
#include "meta/test/TestUtils.h"
#include "storage/test/TestUtils.h"
#include "storage/GraphStorageServiceHandler.h"

DECLARE_int32(heartbeat_interval_secs);
DECLARE_uint32(raft_heartbeat_interval_secs);
DECLARE_int32(expired_threshold_sec);
namespace nebula {
namespace meta {

using PriorityThreadManager = apache::thrift::concurrency::PriorityThreadManager;

void startMetaService(const char* rootPath) {
    const nebula::ClusterID kClusterId = 10;
    auto pool = std::make_shared<folly::IOThreadPoolExecutor>(4);
    auto partMan = std::make_unique<kvstore::MemPartManager>();
    auto workers = PriorityThreadManager::newPriorityThreadManager(1, true);
    workers->setNamePrefix("executor");
    workers->start();

    // GraphSpaceID =>  {PartitionIDs}
    auto& partsMap = partMan->partsMap();
    partsMap[0][0] = PartHosts();

    std::vector<std::string> paths;
    paths.emplace_back(folly::stringPrintf("%s/disk1", rootPath));

    kvstore::KVOptions options;
    options.dataPaths_ = std::move(paths);
    options.partMan_ = std::move(partMan);

    auto port = network::NetworkUtils::getAvailablePort();
    HostAddr localhost = HostAddr("127.0.0.1", port);
    auto store = std::make_unique<kvstore::NebulaStore>(std::move(options),
                                                        pool,
                                                        localhost,
                                                        workers);
    store->init();

    while (true) {
        auto retLeader = store->partLeader(0, 0);
        if (ok(retLeader)) {
            auto leader = value(std::move(retLeader));
            LOG(INFO) << "Leader: " << leader;
            if (leader == localhost) {
                break;
            }
        }
        usleep(100000);
    }

    auto server = std::make_unique<apache::thrift::ThriftServer>();
    auto handler = std::make_shared<nebula::meta::MetaServiceHandler>(store.get(), kClusterId);
    server->setInterface(std::move(handler));
    server->setPort(port);
    auto thread = std::make_unique<thread::NamedThread>("meta", [&server] {
        server->serve();
        LOG(INFO) << "The meta server has been stopped";
    });

    while (!server->getServeEventBase() ||
           !server->getServeEventBase()->isRunning()) {
        usleep(10000);
    }
}

void startStorageService(const char* rootPath, meta::MetaClient* metaClient) {
    auto pool = std::make_shared<folly::IOThreadPoolExecutor>(4);
    auto workers = PriorityThreadManager::newPriorityThreadManager(1, true);
    workers->setNamePrefix("executor");
    workers->start();

    kvstore::KVOptions options;

    std::vector<std::string> paths;
    paths.emplace_back(folly::stringPrintf("%s/disk1", rootPath));
    paths.emplace_back(folly::stringPrintf("%s/disk2", rootPath));
    options.dataPaths_ = std::move(paths);

    auto port = network::NetworkUtils::getAvailablePort();
    auto store = std::make_unique<kvstore::NebulaStore>(std::move(options),
                                                        pool,
                                                        HostAddr("127.0.0.1", port),
                                                        workers);
    store->init();


    auto schemaMan = std::make_unique<ServerBasedSchemaManager>();
    schemaMan->init(metaClient);

    auto indexMan = std::make_unique<ServerBasedIndexManager>();
    indexMan->init(metaClient);

    storage::StorageEnv env;
    env.schemaMan_ = schemaMan.get();
    env.indexMan_ = indexMan.get();
    auto handler = std::make_shared<storage::GraphStorageServiceHandler>(&env);

    auto server = std::make_unique<apache::thrift::ThriftServer>();
    auto storagePort = network::NetworkUtils::getAvailablePort();
    server->setInterface(std::move(handler));
    server->setPort(storagePort);
    auto thread = std::make_unique<thread::NamedThread>("storage", [&server] {
        server->serve();
        LOG(INFO) << "The Storage Service has been stopped";
    });

    while (!server->getServeEventBase() ||
           !server->getServeEventBase()->isRunning()) {
        usleep(10000);
    }
}

TEST(BalanceIntegrationTest, BalanceTest) {
    fs::TempDir rootPath("/tmp/balance_integration_test.XXXXXX");
    mock::MockCluster cluster;
    // uint32_t metaPort = network::NetworkUtils::getAvailablePort();
    // cluster.startMeta(rootPath.path(), HostAddr("127.0.0.1", metaPort));
    // cluster.initMetaClient();

    auto metaPath = folly::stringPrintf("%s/meta", rootPath.path());
    startMetaService(metaPath.c_str());

    int32_t partition = 1;
    int32_t replica = 3;
    LOG(INFO) << "Start " << replica << " storage services, partition number " << partition;

    std::vector<HostAddr> peers;
    std::vector<uint32_t> storagePorts;
    for (int32_t i = 0; i < replica; i++) {
        uint32_t storagePort = network::NetworkUtils::getAvailablePort();
        storagePorts.emplace_back(storagePort);
        peers.emplace_back("127.0.0.1", storagePort);

        VLOG(1) << "The storage server has been added to the meta service";
        auto dataPath = folly::stringPrintf("%s/%d/data", rootPath.path(), i);
    }

    LOG(INFO) << "Create space and schema";
    cpp2::SpaceDesc properties;
    properties.set_space_name("default_space");
    properties.set_partition_num(partition);
    properties.set_replica_factor(replica);
    auto* metaClient = cluster.metaClient_.get();
    auto ret = metaClient->createSpace(std::move(properties)).get();
    ASSERT_TRUE(ret.ok());

    cpp2::Schema schema;
    cpp2::ColumnDef column;
    column.name = "column0";
    column.type.set_type(PropertyType::STRING);
    schema.columns.emplace_back(std::move(column));
    auto spaceId = ret.value();
    auto tagRet = metaClient->createTagSchema(spaceId, "tag", std::move(schema)).get();
    ASSERT_TRUE(tagRet.ok());

    auto tagId = tagRet.value();
    sleep(FLAGS_heartbeat_interval_secs + FLAGS_raft_heartbeat_interval_secs + 3);

    LOG(INFO) << "Let's write some data";

    auto pool = std::make_shared<folly::IOThreadPoolExecutor>(4);
    auto storageClient = std::make_unique<storage::GraphStorageClient>(pool, metaClient);
    {
        std::vector<storage::cpp2::NewVertex> vertices;
        for (int32_t vId = 0; vId < 10000; vId++) {
            storage::cpp2::NewVertex vertex;
            vertex.set_id(vId);

            std::vector<storage::cpp2::NewTag> tags;
            storage::cpp2::NewTag tag;
            tag.set_tag_id(tagId);

            decltype(tag.props) props;
            Value val(std::string(1024, 'A'));
            props.emplace_back(val);
            tag.set_props(std::move(props));

            tags.emplace_back(std::move(tag));
            vertex.set_tags(std::move(tags));
            vertices.emplace_back(std::move(vertex));
        }

        int32_t retry = 10;
        std::unordered_map<TagID, std::vector<std::string>> propNames;
        propNames[tagId].emplace_back("column0");

        while (retry-- > 0) {
            auto f = storageClient->addVertices(spaceId, vertices, propNames, true);
            LOG(INFO) << "Waiting for the response...";
            auto resp = std::move(f).get();
            if (resp.completeness() == 100) {
                LOG(INFO) << "All requests has been processed!";
                break;
            }
            if (!resp.succeeded()) {
                for (auto& err : resp.failedParts()) {
                    LOG(ERROR) << "Partition " << err.first
                               << " failed: " << static_cast<int32_t>(err.second);
                }
            }
            LOG(INFO) << "Failed, the remaining retry times " << retry;
        }
    }

    {
        LOG(INFO) << "Check data...";
        nebula::DataSet input;
        input.colNames = {kVid};
        nebula::Row row;
        for (int32_t vId = 0; vId < 10000; vId++) {
            row.values.emplace_back(vId);
        }
        input.emplace_back(std::move(row));

        std::vector<storage::cpp2::VertexProp> vertexProps;
        storage::cpp2::VertexProp vertexProp;
        vertexProp.tag = tagId;
        vertexProp.props = {"column0"};
        vertexProps.emplace_back(std::move(vertexProp));

        auto f = storageClient->getProps(spaceId, std::move(input), &vertexProps, nullptr, nullptr);
        auto resp = std::move(f).get();
        if (!resp.succeeded()) {
            std::stringstream ss;
            for (auto& part : resp.failedParts()) {
                ss << "Part " << part.first
                   << ": " << static_cast<int32_t>(part.second)
                   << "; ";
            }
            LOG(ERROR) << "Failed partitions:: " << ss.str();
        }
        ASSERT_TRUE(resp.succeeded());
        auto& results = resp.responses();
        EXPECT_EQ(partition, results.size());
        EXPECT_EQ(0, results[0].result.failed_parts.size());
        EXPECT_EQ(1, results[0].props.colNames.size());
        EXPECT_EQ(10000, results[0].props.rows.size());
    }
}

TEST(BalanceIntegrationTest, LeaderBalanceTest) {
    fs::TempDir rootPath("/tmp/balance_integration_leader_test.XXXXXX");
    const nebula::ClusterID kClusterId = 10;
    mock::MockCluster cluster;
    uint32_t metaPort = network::NetworkUtils::getAvailablePort();
    cluster.startMeta(rootPath.path(), HostAddr("127.0.0.1", metaPort));
    cluster.initMetaClient();

    auto adminClient = std::make_unique<AdminClient>(cluster.metaKV_.get());
    Balancer balancer(cluster.metaKV_.get(), adminClient.get());

    int32_t partition = 9;
    int32_t replica = 3;
    LOG(INFO) << "Start " << replica << " storage services, partition number " << partition;

    std::vector<HostAddr> peers;
    std::vector<uint32_t> storagePorts;
    auto pool = std::make_shared<folly::IOThreadPoolExecutor>(4);
    std::vector<HostAddr> metaHosts = {HostAddr("127.0.0.1", metaPort)};
    std::vector<std::shared_ptr<meta::MetaClient>> metaClients;
    for (int32_t i = 0; i < replica; i++) {
        auto storagePort = network::NetworkUtils::getAvailablePort();
        meta::MetaClientOptions options;
        options.localHost_ = HostAddr("127.0.0.1", storagePort);
        options.clusterId_ = kClusterId;
        options.role_ = meta::cpp2::HostRole::STORAGE;
        auto metaClient = std::make_shared<meta::MetaClient>(pool,
                                                             metaHosts,
                                                             options);
        metaClient->waitForMetadReady();
        metaClients.emplace_back(metaClient);


        storagePorts.emplace_back(storagePort);
        peers.emplace_back("127.0.0.1", storagePort);

        VLOG(1) << "The storage server has been added to the meta service";
        auto dataPath = folly::stringPrintf("%s/%d/data", rootPath.path(), i);
    }

    LOG(INFO) << "Create space";
    cpp2::SpaceDesc properties;
    properties.set_space_name("default_space");
    properties.set_partition_num(partition);
    properties.set_replica_factor(replica);
    auto* metaClient = cluster.metaClient_.get();
    auto ret = metaClient->createSpace(std::move(properties)).get();
    ASSERT_TRUE(ret.ok());

    while (true) {
        int totalLeaders = 0;
        // for (int i = 0; i < replica; i++) {
        //     std::unordered_map<GraphSpaceID, std::vector<PartitionID>> leaderIds;
        //     totalLeaders += serverContexts[i]->kvStore_->allLeader(leaderIds);
        // }
        if (totalLeaders == partition) {
            break;
        }
        LOG(INFO) << "Waiting for leader election, current total leader number " << totalLeaders
                  << ", expected " << partition;
        sleep(1);
    }

    auto code = balancer.leaderBalance();
    ASSERT_EQ(code, cpp2::ErrorCode::SUCCEEDED);

    LOG(INFO) << "Waiting for the leader balance";
    sleep(FLAGS_raft_heartbeat_interval_secs + 1);
    size_t leaderCount = 0;
    for (int i = 0; i < replica; i++) {
        std::unordered_map<GraphSpaceID, std::vector<PartitionID>> leaderIds;
        // leaderCount += serverContexts[i]->kvStore_->allLeader(leaderIds);
    }
    EXPECT_EQ(9, leaderCount);
    for (auto& c : metaClients) {
        c->stop();
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

