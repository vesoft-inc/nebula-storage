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
#include "kvstore/LogEncoder.h"

DECLARE_uint32(raft_heartbeat_interval_secs);
using nebula::meta::PartHosts;
using nebula::meta::ListenerHosts;

namespace nebula {
namespace kvstore {

class ListenerBasicTest : public ::testing::TestWithParam<std::tuple<int32_t, int32_t, int32_t>> {
};

class DummyListener : public Listener {
public:
    DummyListener(GraphSpaceID spaceId,
                  PartitionID partId,
                  HostAddr localAddr,
                  const std::string& walPath,
                  std::shared_ptr<folly::IOThreadPoolExecutor> ioPool,
                  std::shared_ptr<thread::GenericThreadPool> workers,
                  std::shared_ptr<folly::Executor> handlers,
                  meta::SchemaManager* schemaMan)
        : Listener(spaceId, partId, localAddr, walPath,
                   ioPool, workers, handlers, nullptr, nullptr, schemaMan) {
        setCallback(std::bind(&DummyListener::commitLog, this,
                              std::placeholders::_1, std::placeholders::_2),
                    std::bind(&DummyListener::updateCommit, this,
                              std::placeholders::_1, std::placeholders::_2));
    }

    std::vector<KV> data() {
        return data_;
    }

protected:
    bool commitLog(LogID, folly::StringPiece log) {
        // decode a wal
        auto kvs = decodeMultiValues(log);
        CHECK_EQ((kvs.size() + 1) / 2, kvs.size() / 2);
        // mock persist log
        for (size_t i = 0; i < kvs.size(); i += 2) {
            data_.emplace_back(kvs[i], kvs[i + 1]);
        }
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
    std::vector<KV> data_;
};

std::shared_ptr<apache::thrift::concurrency::PriorityThreadManager> getWorkers() {
    auto worker =
        apache::thrift::concurrency::PriorityThreadManager::newPriorityThreadManager(1, true);
    worker->setNamePrefix("executor");
    worker->start();
    return worker;
}

TEST_P(ListenerBasicTest, SimpleTest) {
    auto param = GetParam();
    int32_t partCount = std::get<0>(param);
    int32_t replicas = std::get<1>(param);
    int32_t listenerCount = std::get<2>(param);

    auto initStore = [partCount] (const std::vector<HostAddr>& peers,
                                  int32_t index,
                                  const std::string& path,
                                  const std::vector<HostAddr>& listeners = {}) {
        auto partMan = std::make_unique<MemPartManager>();
        auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);

        GraphSpaceID spaceId = 1;
        for (int32_t partId = 1; partId <= partCount; partId++) {
            PartHosts ph;
            ph.spaceId_ = spaceId;
            ph.partId_ = partId;
            ph.hosts_ = peers;
            partMan->partsMap_[spaceId][partId] = std::move(ph);
            // add remote listeners
            if (!listeners.empty()) {
                partMan->remoteListeners_[spaceId][partId].emplace_back(
                    listeners[partId % listeners.size()], meta::cpp2::ListenerType::UNKNOWN);
            }
        }

        std::vector<std::string> paths;
        paths.emplace_back(folly::stringPrintf("%s/disk%d", path.c_str(), index));

        KVOptions options;
        options.dataPaths_ = std::move(paths);
        options.partMan_ = std::move(partMan);
        HostAddr local = peers[index];
        return std::make_unique<NebulaStore>(std::move(options),
                                             ioThreadPool,
                                             local,
                                             getWorkers());
    };

    auto initListener = [] (const std::vector<HostAddr>& listeners,
                            int32_t index,
                            const std::string& path) {
        auto partMan = std::make_unique<MemPartManager>();
        auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);

        KVOptions options;
        options.listenerPath_ = folly::stringPrintf("%s/listener%d", path.c_str(), index);
        options.partMan_ = std::move(partMan);
        HostAddr local = listeners[index];
        return std::make_unique<NebulaStore>(std::move(options),
                                             ioThreadPool,
                                             local,
                                             getWorkers());
    };

    fs::TempDir rootPath("/tmp/nebula_store_test.XXXXXX");
    GraphSpaceID spaceId = 1;
    std::string ip("127.0.0.1");
    std::vector<HostAddr> peers, listenerHosts;
    for (int32_t i = 0; i < replicas; i++) {
        peers.emplace_back(ip, network::NetworkUtils::getAvailablePort());
    }
    for (int32_t i = 0; i < listenerCount; i++) {
        listenerHosts.emplace_back(ip, network::NetworkUtils::getAvailablePort());
    }

    LOG(INFO) << "Init data replica";
    std::vector<std::unique_ptr<NebulaStore>> stores;
    for (int32_t i = 0; i < replicas; i++) {
        stores.emplace_back(initStore(peers, i, rootPath.path(), listenerHosts));
        stores.back()->init();
    }

    LOG(INFO) << "Init listener replica by manual";
    std::vector<std::unique_ptr<NebulaStore>> listeners;
    for (int32_t i = 0; i < listenerCount; i++) {
        listeners.emplace_back(initListener(listenerHosts, i, rootPath.path()));
        listeners.back()->init();
        listeners.back()->spaceListeners_.emplace(spaceId, std::make_shared<SpaceListenerInfo>());
    }
    std::unordered_map<PartitionID, std::shared_ptr<DummyListener>> dummys;
    // start dummy listener on listener hosts
    for (int32_t partId = 1; partId <= partCount; partId++) {
        // count which listener holds this part
        auto index = partId % listenerHosts.size();
        auto walPath = folly::stringPrintf(
            "%s/listener%lu/%d/%d/wal", rootPath.path(), index, spaceId, partId);
        auto local = NebulaStore::getRaftAddr(listenerHosts[index]);
        auto dummy = std::make_shared<DummyListener>(spaceId,
                                                     partId,
                                                     local,
                                                     walPath,
                                                     listeners[index]->ioPool_,
                                                     listeners[index]->bgWorkers_,
                                                     listeners[index]->workers_,
                                                     nullptr);
        listeners[index]->raftService_->addPartition(dummy);
        std::vector<HostAddr> raftPeers;
        std::transform(peers.begin(), peers.end(), std::back_inserter(raftPeers),
                       [] (const auto& host) {
            return NebulaStore::getRaftAddr(host);
        });
        dummy->start(std::move(raftPeers));
        listeners[index]->spaceListeners_[spaceId]->listeners_[partId].emplace(
            meta::cpp2::ListenerType::UNKNOWN, dummy);
        dummys.emplace(partId, dummy);
    }

    auto waitLeader = [replicas, partCount, &stores] () {
        while (true) {
            int32_t leaderCount = 0;
            for (int i = 0; i < replicas; i++) {
                std::unordered_map<GraphSpaceID, std::vector<PartitionID>> leaderIds;
                leaderCount += stores[i]->allLeader(leaderIds);
            }
            if (leaderCount == partCount) {
                break;
            }
            usleep(100000);
        }
    };

    auto findStoreIndex = [&peers] (const HostAddr& addr) {
        for (size_t i = 0; i < peers.size(); i++) {
            if (peers[i] == addr) {
                return i;
            }
        }
        LOG(FATAL) << "Should not reach here!";
    };

    LOG(INFO) << "Waiting for all leaders elected!";
    waitLeader();

    LOG(INFO) << "Insert some data";
    for (int32_t partId = 1; partId <= partCount; partId++) {
        std::vector<KV> data;
        for (int32_t i = 0; i < 100; i++) {
            data.emplace_back(folly::stringPrintf("key_%d_%d", partId, i),
                              folly::stringPrintf("val_%d_%d", partId, i));
        }
        auto leader = value(stores[0]->partLeader(spaceId, partId));
        auto index = findStoreIndex(leader);
        folly::Baton<true, std::atomic> baton;
        stores[index]->asyncMultiPut(spaceId, partId, std::move(data), [&baton](ResultCode code) {
            EXPECT_EQ(ResultCode::SUCCEEDED, code);
            baton.post();
        });
        baton.wait();
    }

    // wait listener commit
    sleep(FLAGS_raft_heartbeat_interval_secs);

    LOG(INFO) << "Check listener's data";
    for (int32_t partId = 1; partId <= partCount; partId++) {
        auto dummy = dummys[partId];
        const auto& data = dummy->data();
        CHECK_EQ(100, data.size());
        for (int32_t i = 0; i < static_cast<int32_t>(data.size()); i++) {
            CHECK_EQ(folly::stringPrintf("key_%d_%d", partId, i), data[i].first);
            CHECK_EQ(folly::stringPrintf("val_%d_%d", partId, i), data[i].second);
        }
    }
}

INSTANTIATE_TEST_CASE_P(
    PartCount_Replicas_ListenerCount,
    ListenerBasicTest,
    ::testing::Values(
        std::make_tuple(1, 1, 1),
        std::make_tuple(3, 1, 1),
        std::make_tuple(10, 1, 1),
        std::make_tuple(1, 3, 1),
        std::make_tuple(3, 3, 1),
        std::make_tuple(10, 3, 1),
        std::make_tuple(1, 1, 2),
        std::make_tuple(3, 1, 3),
        std::make_tuple(3, 3, 3),
        std::make_tuple(10, 3, 3),
        std::make_tuple(10, 3, 10)));

}  // namespace kvstore
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
