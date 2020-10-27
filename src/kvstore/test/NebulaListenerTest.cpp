/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/fs/FileUtils.h"
#include "common/network/NetworkUtils.h"
#include "common/meta/Common.h"
#include <thrift/lib/cpp/concurrency/ThreadManager.h>
#include <gtest/gtest.h>
#include "kvstore/NebulaStore.h"
#include "kvstore/PartManager.h"

DECLARE_uint32(raft_heartbeat_interval_secs);
using nebula::meta::PartHosts;
using nebula::meta::ListenerHosts;

namespace nebula {
namespace kvstore {

class DummyListener : public Listener {
public:
    DummyListener(GraphSpaceID spaceId,
                  PartitionID partId,
                  HostAddr localAddr,
                  const std::string& walPath,
                  std::shared_ptr<folly::IOThreadPoolExecutor> ioPool,
                  std::shared_ptr<thread::GenericThreadPool> workers,
                  std::shared_ptr<folly::Executor> handlers)
        : Listener(spaceId, partId, localAddr, walPath,
                   ioPool, workers, handlers, nullptr, nullptr) {
        setCallback(std::bind(&DummyListener::commitLog, this,
                              std::placeholders::_1, std::placeholders::_2),
                    std::bind(&DummyListener::updateCommit, this,
                              std::placeholders::_1, std::placeholders::_2));
    }

protected:
    bool commitLog(LogID logId, folly::StringPiece logMsg) {
        data_.emplace_back(logId, logMsg);
        return true;
    }

    bool updateCommit(LogID, TermID) {
        return true;
    }

    std::pair<LogID, TermID> lastCommittedLogId() override {
        return std::make_pair(committedLogId_, term_);
    }

    std::pair<int64_t, int64_t> commitSnapshot(const std::vector<std::string>&,
                                               LogID,
                                               TermID,
                                               bool) override {
        LOG(FATAL) << "Not implemented";
    }

    void cleanup() override {
        data_.clear();
    }

private:
    std::vector<std::pair<LogID, std::string>> data_;
};

std::shared_ptr<apache::thrift::concurrency::PriorityThreadManager> getWorkers() {
    auto worker =
        apache::thrift::concurrency::PriorityThreadManager::newPriorityThreadManager(1, true);
    worker->setNamePrefix("executor");
    worker->start();
    return worker;
}

TEST(NebulaStoreTest, SimpleTest) {
    // 1 space, 1 part, 1 replica, 1 listener
    auto initNebulaStore = [](const std::vector<HostAddr>& peers,
                              int32_t index,
                              const std::string& path,
                              bool asListener = false)
        -> std::unique_ptr<NebulaStore> {
        auto partMan = std::make_unique<MemPartManager>();
        auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);

        GraphSpaceID spaceId = 1;
        PartitionID partId = 1;
        if (!asListener) {
            PartHosts ph;
            ph.spaceId_ = spaceId;
            ph.partId_ = partId;
            ph.hosts_ = peers;
            partMan->partsMap_[spaceId][partId] = std::move(ph);
        } else {
            ListenerHosts lh;
            lh.spaceId_ = spaceId;
            lh.partId_ = partId;
            lh.hosts_ = peers;
            partMan->listenersMap_[spaceId][partId] = std::move(lh);
        }

        fs::TempDir rootPath("/tmp/nebula_listener_test.XXXXXX");
        std::vector<std::string> paths;
        paths.emplace_back(folly::stringPrintf("%s/disk%d", path.c_str(), index));

        KVOptions options;
        options.dataPaths_ = std::move(paths);
        options.listenerPath_ = folly::stringPrintf("%s/listener%d", path.c_str(), index);
        options.partMan_ = std::move(partMan);
        HostAddr local = peers[index];
        return std::make_unique<NebulaStore>(std::move(options),
                                             ioThreadPool,
                                             local,
                                             getWorkers());
    };

    fs::TempDir rootPath("/tmp/nebula_store_test.XXXXXX");
    int32_t replicas = 1;
    std::string ip("127.0.0.1");
    std::vector<HostAddr> peers;
    for (int32_t i = 0; i < replicas; i++) {
        peers.emplace_back(ip, network::NetworkUtils::getAvailablePort());
    }

    // init data replica
    std::vector<std::unique_ptr<NebulaStore>> stores;
    for (int32_t i = 0; i < replicas; i++) {
        stores.emplace_back(initNebulaStore(peers, i, rootPath.path()));
        stores.back()->init();
    }

    // init listener replica
    auto listener = initNebulaStore(peers, replicas, rootPath.path(), true);
    listener->init();

    sleep(5);
}

}  // namespace kvstore
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
