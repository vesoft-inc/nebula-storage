/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "mock/MockCluster.h"
#include "mock/AdHocIndexManager.h"
#include "mock/AdHocSchemaManager.h"
#include "mock/MockData.h"
#include "meta/MetaServiceHandler.h"
#include "common/meta/ServerBasedSchemaManager.h"
#include "common/clients/meta/MetaClient.h"
#include "storage/StorageAdminServiceHandler.h"
#include "storage/GraphStorageServiceHandler.h"
#include "storage/GeneralStorageServiceHandler.h"

namespace nebula {
namespace mock {

// static
void MockCluster::waitUntilAllElected(kvstore::NebulaStore* kvstore,
                                      GraphSpaceID spaceId,
                                      const std::vector<PartitionID>& partIds) {
    while (true) {
        size_t readyNum = 0;
        for (auto partId : partIds) {
            auto retLeader = kvstore->partLeader(spaceId, partId);
            if (ok(retLeader)) {
                auto leader = value(std::move(retLeader));
                if (leader != HostAddr("", 0)) {
                    readyNum++;
                }
            }
        }
        if (readyNum == partIds.size()) {
            LOG(INFO) << "All leaders have been elected!";
            break;
        }
        usleep(100000);
    }
}

// static
std::unique_ptr<kvstore::MemPartManager>
MockCluster::memPartMan(GraphSpaceID spaceId, const std::vector<PartitionID>& parts) {
    auto memPartMan = std::make_unique<kvstore::MemPartManager>();
    // GraphSpaceID =>  {PartitionIDs}
    auto& partsMap = memPartMan->partsMap();
    for (auto partId : parts) {
        partsMap[spaceId][partId] = meta::PartHosts();
    }
    return memPartMan;
}

// static
std::string MockCluster::localIP() {
    return network::NetworkUtils::getHostname();
}

// static
std::unique_ptr<kvstore::NebulaStore>
MockCluster::initKV(kvstore::KVOptions options, HostAddr localHost) {
    auto ioPool = std::make_shared<folly::IOThreadPoolExecutor>(4);
    auto workers = apache::thrift::concurrency::PriorityThreadManager::newPriorityThreadManager(
                             1, true /*stats*/);
    workers->setNamePrefix("executor");
    workers->start();
    if (localHost.host == 0) {
        localHost.host = localIP();
    }
    if (localHost.port == 0) {
        localHost.port = network::NetworkUtils::getAvailablePort();
    }
    auto store = std::make_unique<kvstore::NebulaStore>(std::move(options),
                                                        ioPool,
                                                        localHost,
                                                        workers);
    store->init();
    return store;
}

// static
std::unique_ptr<kvstore::NebulaStore>
MockCluster::initMetaKV(const char* dataPath, HostAddr addr) {
    kvstore::KVOptions options;
    options.partMan_ = memPartMan(0, {0});
    std::vector<std::string> paths;
    paths.emplace_back(folly::stringPrintf("%s/disk1", dataPath));
    options.dataPaths_ = std::move(paths);
    auto kv = initKV(std::move(options), addr);
    waitUntilAllElected(kv.get(), 0, {0});
    return kv;
}

void MockCluster::startMeta(int32_t port,
                            const std::string& rootPath,
                            std::string hostname) {
    metaKV_ = initMetaKV(rootPath.c_str(), {hostname, port});
    metaServer_ = std::make_unique<RpcServer>();
    auto handler = std::make_shared<meta::MetaServiceHandler>(metaKV_.get(),
                                                              clusterId_);
    metaServer_->start("meta", port, handler);
    LOG(INFO) << "The Meta Daemon started on port " << metaServer_->port_;
}

void MockCluster::initStorageKV(const char* dataPath,
                                HostAddr addr,
                                SchemaVer schemaVerCount) {
    const std::vector<PartitionID> parts{1, 2, 3, 4, 5, 6};
    totalParts_ = 6;  // don't not delete this...
    kvstore::KVOptions options;
    if (metaClient_ != nullptr) {
        LOG(INFO) << "Pull meta information from meta server";
        nebula::meta::cpp2::SpaceDesc spaceDesc;
        spaceDesc.space_name = "test_space";
        spaceDesc.partition_num = 6;
        spaceDesc.replica_factor = 1;
        spaceDesc.charset_name = "utf8";
        spaceDesc.collate_name = "utf8_bin";
        auto ret = metaClient_->createSpace(spaceDesc).get();
        if (!ret.ok()) {
            LOG(FATAL) << "can't create space";
        }
        GraphSpaceID spaceId = ret.value();
        LOG(INFO) << "spaceId = " << spaceId;
        options.partMan_ = std::make_unique<kvstore::MetaServerBasedPartManager>(
                                            addr,
                                            metaClient_.get());
        schemaMan_ = meta::SchemaManager::create(metaClient_.get());
        indexMan_ = meta::IndexManager::create();
        indexMan_->init(metaClient_.get());
    } else {
        LOG(INFO) << "Use meta in memory!";
        options.partMan_ = memPartMan(1, parts);;
        schemaMan_ = memSchemaMan(schemaVerCount);
        indexMan_ = memIndexMan();
    }
    std::vector<std::string> paths;
    paths.emplace_back(folly::stringPrintf("%s/disk1", dataPath));
    paths.emplace_back(folly::stringPrintf("%s/disk2", dataPath));
    // Prepare KVStore
    options.dataPaths_ = std::move(paths);
    // options.cffBuilder_ = std::move(cffBuilder);
    storageKV_ = initKV(std::move(options), addr);
    waitUntilAllElected(storageKV_.get(), 1, parts);

    storageEnv_ = std::make_unique<storage::StorageEnv>();
    storageEnv_->schemaMan_ = schemaMan_.get();
    storageEnv_->indexMan_ = indexMan_.get();
    storageEnv_->kvstore_ = storageKV_.get();
    storageEnv_->rebuildIndexGuard_ = std::make_unique<storage::IndexGuard>();
}

void MockCluster::startStorage(HostAddr addr,
                               const std::string& rootPath,
                               bool isGeneralService,
                               SchemaVer schemaVerCount) {
    initStorageKV(rootPath.c_str(), addr, schemaVerCount);

    auto *env = storageEnv_.get();
    storageAdminServer_ = std::make_unique<RpcServer>();
    auto adminHandler = std::make_shared<storage::StorageAdminServiceHandler>(env);
    storageAdminServer_->start("admin-storage", addr.port - 1, adminHandler);
    LOG(INFO) << "The admin storage daemon started on port " << storageAdminServer_->port_;

    if (!isGeneralService) {
        graphStorageServer_ = std::make_unique<RpcServer>();
        auto graphHandler = std::make_shared<storage::GraphStorageServiceHandler>(env);
        graphStorageServer_->start("graph-storage", addr.port, graphHandler);
        LOG(INFO) << "The graph storage daemon started on port " << graphStorageServer_->port_;
    } else {
        generalStorageServer_ = std::make_unique<RpcServer>();
        auto generalHandler = std::make_shared<storage::GeneralStorageServiceHandler>(env);
        generalStorageServer_->start("general-storage", addr.port, generalHandler);
        LOG(INFO) << "The general storage daemon started on port " << generalStorageServer_->port_;
    }
}

std::unique_ptr<meta::SchemaManager>
MockCluster::memSchemaMan(SchemaVer schemaVerCount, GraphSpaceID spaceId) {
    auto schemaMan = std::make_unique<AdHocSchemaManager>();
    // if have multi version schema, need to add from oldest to newest
    for (SchemaVer ver = 0; ver < schemaVerCount; ver++) {
        // Vertex has two tags: players and teams
        // When tagId is 1, use players data
        schemaMan->addTagSchema(spaceId, 1, MockData::mockPlayerTagSchema(ver));
        // When tagId is 2, use teams data
        schemaMan->addTagSchema(spaceId, 2, MockData::mockTeamTagSchema(ver));

        // Edge has two type: serve and teammate
        // When edgeType is 101, use serve data
        schemaMan->addEdgeSchema(spaceId, 101, MockData::mockServeEdgeSchema(ver));
        // When edgeType is 102, use teammate data
        schemaMan->addEdgeSchema(spaceId, 102, MockData::mockTeammateEdgeSchema(ver));
    }

    schemaMan->addTagSchema(spaceId, 3, MockData::mockGeneralTagSchemaV1());

    schemaMan->addTagSchema(spaceId, 3, MockData::mockGeneralTagSchemaV2());

    return schemaMan;
}

std::unique_ptr<meta::IndexManager>
MockCluster::memIndexMan(GraphSpaceID spaceId) {
    auto indexMan = std::make_unique<AdHocIndexManager>();
    indexMan->addTagIndex(spaceId, 1, 1, MockData::mockPlayerTagIndexColumns());
    indexMan->addTagIndex(spaceId, 2, 2, MockData::mockTeamTagIndexColumns());
    indexMan->addTagIndex(spaceId, 3, 3, MockData::mockGeneralTagIndexColumns());
    indexMan->addEdgeIndex(spaceId, 101, 101, MockData::mockServeEdgeIndexColumns());
    indexMan->addEdgeIndex(spaceId, 102, 102, MockData::mockTeammateEdgeIndexColumns());
    return indexMan;
}

void MockCluster::initMetaClient(meta::MetaClientOptions options) {
    CHECK(metaServer_ != nullptr);
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    auto localhosts = std::vector<HostAddr>{HostAddr(localIP(), metaServer_->port_)};
    metaClient_ = std::make_unique<meta::MetaClient>(threadPool, localhosts, options);
    metaClient_->waitForMetadReady();
    LOG(INFO) << "Meta client has been ready!";
}

storage::GraphStorageClient* MockCluster::initGraphStorageClient() {
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    storageClient_ = std::make_unique<storage::GraphStorageClient>(threadPool, metaClient_.get());
    return storageClient_.get();
}

storage::GeneralStorageClient* MockCluster::initGeneralStorageClient() {
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    generalClient_ = std::make_unique<storage::GeneralStorageClient>(threadPool, metaClient_.get());
    return generalClient_.get();
}

}  // namespace mock
}  // namespace nebula
