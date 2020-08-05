
/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "common/base/Base.h"
#include <gtest/gtest.h>
#include <folly/Benchmark.h>
#include <folly/Format.h>
#include <chrono>

#include "common/clients/storage/GraphStorageClient.h"
#include "common/expression/ConstantExpression.h"

DECLARE_int32(heartbeat_interval_secs);
DECLARE_int32(update_edge_mode);

DEFINE_string(meta_server, "192.168.8.5:6500", "Meta servers' address.");

namespace nebula {
namespace storage {

using StorageClient = storage::GraphStorageClient;
// nocheck 0, singleRowTxn, TOSS, atomicOp

class Timer {
public:
    explicit Timer(std::string name) : name_(name) {
        startTs_ = std::chrono::high_resolution_clock::now();
        stopTs_ = startTs_;
    }
    ~Timer() {
        using std::chrono::duration_cast;
        auto micro_secs = duration_cast<std::chrono::microseconds>(stopTs_ - startTs_);
        std::cout << name_ << " spend " << micro_secs.count() / 1000 << "ms" << std::endl;
    }
    void Stop() {
        stopTs_ = std::chrono::high_resolution_clock::now();
    }

private:
    std::string name_;
    std::chrono::high_resolution_clock::time_point startTs_;
    std::chrono::high_resolution_clock::time_point stopTs_;
};

struct BenchmarkEnv {
    static BenchmarkEnv* getInstance() {
        static BenchmarkEnv inst;
        return &inst;
    }

    static std::string toVertexId(int64_t id) {
        return std::string(reinterpret_cast<char*>(&id), sizeof(int64_t));
    }

    ~BenchmarkEnv() {
        std::cout << "addEdgesInBatchCalled: " << addEdgesInBatchCalled << std::endl;
    }

    void setupMetaClient(std::string serverName, uint32_t port) {
        std::vector<HostAddr> metas;
        metas.emplace_back(HostAddr(serverName, port));
        meta::MetaClientOptions options;
        metaClient_ = std::make_unique<meta::MetaClient>(ioThreadPool_,
                                                         metas,
                                                         options);
        if (!metaClient_->waitForMetadReady()) {
            LOG(ERROR) << "waitForMetadReady error!";
        }
    }

    int setupSpace(std::string spaceName, int nPart, int nReplica) {
        auto futDropSpace = metaClient_->dropSpace(spaceName, true);
        futDropSpace.wait();
        std::cout << "dropSpace(" << spaceName << ")" << std::endl;

        meta::SpaceDesc spaceDesc(spaceName, nPart, nReplica);
        auto futCreateSpace = metaClient_->createSpace(spaceDesc, true);
        futCreateSpace.wait();
        spaceId_ = futCreateSpace.value().value();
        auto msg = folly::sformat("spaceId_ = {}", spaceId_);
        std::cout << msg << std::endl;
        return spaceId_;
    }

    EdgeType setupEdgeSchema(const std::string& edgeName) {
        std::vector<meta::cpp2::ColumnDef> columns;
        Value defaultValue;
        defaultValue.setInt(0);
        columns.emplace_back();
        columns.back().set_name(colName_);
        columns.back().set_default_value(defaultValue);
        columns.back().set_type(meta::cpp2::PropertyType::INT32);

        meta::cpp2::Schema schema;
        schema.set_columns(std::move(columns));
        auto edgeTypeFut = metaClient_->createEdgeSchema(spaceId_, edgeName, schema, true);
        edgeTypeFut.wait();

        if (!edgeTypeFut.valid()) {
            throw std::runtime_error("!edgeTypeFut.valid()");
        }
        if (!edgeTypeFut.value().ok()) {
            throw std::runtime_error("!edgeTypeFut.value().ok()");
        }
        edgeType_ = edgeTypeFut.value().value();
        auto msg = folly::sformat("edgeType_ = {}", edgeType_);
        std::cout << msg << std::endl;
        return edgeType_;
    }

    void init(int partition , int replica) {
        if (!unInitialized) {
            return;
        }
        std::string msg{""};
        ioThreadPool_ = std::make_shared<folly::IOThreadPoolExecutor>(3);
        setupMetaClient("hp-server", 6500);
        std::string spaceName{"test"};

        spaceId_ = setupSpace(spaceName, partition, replica);
        edgeType_ = setupEdgeSchema("test_edge");

        storageClient_ = std::make_unique<StorageClient>(ioThreadPool_, metaClient_.get());
        unInitialized = false;

        msg = folly::sformat("sleep for FLAGS_heartbeat_interval_secs + 2 secs");
        std::cout << msg << std::endl;
        sleep(FLAGS_heartbeat_interval_secs + 2);
    }

    cpp2::EdgeKey getOutEdgeKey(int srcId, int rank = 0) {
        static int sum = 10000 * 10000;
        cpp2::EdgeKey edgeKey;
        edgeKey.set_src(toVertexId(srcId));
        edgeKey.set_edge_type(edgeType_);
        edgeKey.set_ranking(rank);
        edgeKey.set_dst(toVertexId(sum - srcId));
        return edgeKey;
    }

    cpp2::NewEdge getOutEdges(int srcId) {
        cpp2::EdgeKey edgeKey = getOutEdgeKey(srcId);

        std::vector<nebula::Value> vals(1);
        vals.back().setInt(0);

        cpp2::NewEdge newEdge;
        newEdge.set_key(std::move(edgeKey));
        newEdge.set_props(std::move(vals));

        return newEdge;
    }

    void reverseEdgeKeyDirection(cpp2::EdgeKey& key) {
        std::swap(key.src, key.dst);
        key.edge_type *= -1;
    }

    void addEdges(int srcIdBase, int num, int batch = 500) {
        std::vector<cpp2::NewEdge> outEdges;
        for (int i = 0; i != num; ++i) {
            // std::cout << "messi i = " << i << std::endl;
            if (outEdges.size() == batch) {
                addEdgesInBatch(std::move(outEdges));
                outEdges.clear();
            }
            outEdges.emplace_back(getOutEdges(srcIdBase + i));
        }
        if (!outEdges.empty()) {
            addEdgesInBatch(std::move(outEdges));
        }
    }

    void addEdgesInBatch(std::vector<cpp2::NewEdge>&& outEdges, bool addRelativeInEdges = false) {
        ++addEdgesInBatchCalled;

        std::vector<std::string> propNames{colName_};
        auto futAddOutEdge = storageClient_->addEdges(spaceId_,
                                                     outEdges,
                                                     propNames,
                                                     true);
        futAddOutEdge.wait();

        if (addRelativeInEdges) {
            auto inEdges = outEdges;
            auto toInEdge = [](cpp2::NewEdge& edge) {
                std::swap(edge.key.src, edge.key.dst);
                edge.key.edge_type *= -1;
            };
            std::for_each(inEdges.begin(), inEdges.end(), toInEdge);
            auto futAddInEdge = storageClient_->addEdges(spaceId_,
                                                        inEdges,
                                                        propNames,
                                                        true);

            futAddInEdge.wait();
        }
    }

    void updateSingleEdgeSync(storage::cpp2::EdgeKey edgeKey, int val) {
        std::vector<cpp2::UpdatedProp> updatedProps(1);
        updatedProps.back().set_name(colName_);
        ConstantExpression val1(val);
        updatedProps.back().set_value(Expression::encode(val1));

        bool insertable = true;
        std::vector<std::string> returnProps;
        std::string condition{""};
        auto updateEdgeFut = storageClient_->updateEdge(spaceId_,
                                                        edgeKey,
                                                        updatedProps,
                                                        insertable,
                                                        returnProps,
                                                        condition);
        updateEdgeFut.wait();
        if (!updateEdgeFut.valid()) {
            std::cout << "!updateEdgeFut.valid()" << std::endl;
        }

        auto updateEdgeStatus = updateEdgeFut.value();
        if (!updateEdgeStatus.ok()) {
            std::cout << "!!!!!!updateEdgeFut.valid()" << std::endl;
        }
    }

    void updateEdges(int srcIdBegin, int num, int rank) {
        for (int i = 0; i != num; ++i) {
            storage::cpp2::EdgeKey key = getOutEdgeKey(srcIdBegin + i, rank);
            updateSingleEdgeSync(key, rank);

            // reverseEdgeKeyDirection(key);
            // updateSingleEdge(key, rank);
        }
    }

    // folly::Future<StatusOr<storage::cpp2::UpdateResponse>>
    void updateSingleEdgeAsync(
            std::vector<folly::Future<StatusOr<storage::cpp2::UpdateResponse>>>& futures,
            storage::cpp2::EdgeKey edgeKey, int val) {
        std::vector<cpp2::UpdatedProp> updatedProps(1);
        updatedProps.back().set_name(colName_);
        ConstantExpression val1(val);
        updatedProps.back().set_value(Expression::encode(val1));

        bool insertable = true;
        std::vector<std::string> returnProps;
        std::string condition{""};
        auto updateEdgeFut = storageClient_->updateEdge(spaceId_,
                                                        edgeKey,
                                                        updatedProps,
                                                        insertable,
                                                        returnProps,
                                                        condition);
        futures.emplace_back(std::move(updateEdgeFut));
    }

    void updateEdgesAsync(int srcIdBegin, int num, int rank, int batch = 1) {
        std::vector<folly::Future<StatusOr<storage::cpp2::UpdateResponse>>> futures;
        for (int i = 0; i != num; ++i) {
            storage::cpp2::EdgeKey key = getOutEdgeKey(srcIdBegin + i, rank);
            updateSingleEdgeAsync(futures, key, rank);
            if (batch == futures.size()) {
                auto fut = folly::collect(futures);
                fut.wait();
                futures.clear();
            }
            // reverseEdgeKeyDirection(key);
            // updateSingleEdge(key, rank);
        }
        if (!futures.empty()) {
            auto fut = folly::collect(futures);
            fut.wait();
        }
    }


    meta::MetaClient* getMetaClient() { return metaClient_.get(); }
    StorageClient* getStorageClient() { return storageClient_.get(); }

    BenchmarkEnv() = default;

    std::unique_ptr<meta::MetaClient> metaClient_;
    std::unique_ptr<StorageClient> storageClient_;
    std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool_;

    int                 spaceId_{0};
    int                 edgeType_{0};
    std::string         colName_{"col1"};
    bool                unInitialized{true};

    int                 addEdgesInBatchCalled{0};
};

void func() {
    BenchmarkEnv* env = BenchmarkEnv::getInstance();
    int partition = 1;
    int replica = 3;
    int srcIdBase = 5000;
    int cnt = 50000;
    int batch = 30;

    env->init(partition, replica);

    /*
     * case 1: executeWithoutLock
     * case 2: executeViaInMemorySingleRowTxn
     * case 4: executeViaAtomicOp
     * case 9: return kvstore::ResultCode::SUCCEEDED;
     * */

    auto timer0 = std::make_unique<Timer>(folly::sformat("insert {} edges", cnt));
    env->addEdges(srcIdBase, cnt, batch);
    timer0->Stop();

    // auto timer1 = std::make_unique<Timer>(folly::sformat("update {} edges (no lock)", cnt));
    // env->updateEdgesAsync(srcIdBase, cnt, 1, batch);
    // timer1->Stop();

    // auto timer2 = std::make_unique<Timer>(folly::sformat("update {} edges (mem lock)", cnt));
    // env->updateEdgesAsync(srcIdBase, cnt, 2, batch);
    // timer2->Stop();

    // auto timer3 = std::make_unique<Timer>(folly::sformat("update {} edges (raft lock)", cnt));
    // env->updateEdgesAsync(srcIdBase, cnt, 3, batch);
    // timer3->Stop();

    // auto timer4 = std::make_unique<Timer>(folly::sformat("update {} edges atomic op", cnt));
    // env->updateEdgesAsync(srcIdBase, cnt, 4, batch);
    // timer4->Stop();

    auto timer5 = std::make_unique<Timer>(folly::sformat("update {} edges (write only)", cnt));
    env->updateEdgesAsync(srcIdBase, cnt, 5, batch);
    timer5->Stop();

    // auto timer9 = std::make_unique<Timer>(folly::sformat("update no edges"));
    // env->updateEdgesAsync(srcIdBase, cnt, 9);
    // timer9->Stop();

    std::cout << folly::sformat("update {} edges, partition {}, replica {}, batch {}",
                                cnt,
                                partition,
                                replica,
                                batch) << std::endl;
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
    FLAGS_heartbeat_interval_secs = 1;
    folly::init(&argc, &argv, false);
    // folly::runBenchmarks();
    nebula::storage::func();
    return 0;
}

/*
40 processors, Intel(R) Xeon(R) CPU E5-2690 v2 @ 3.00GHz.
============================================================================
StorageDAGBenchmark.cpprelative                            time/iter  iters/s
============================================================================
future_fanout                                              150.59us    6.64K
recursive_fanout                                34326.27%   438.69ns    2.28M
----------------------------------------------------------------------------
future_chain                                               128.58us    7.78K
recursive_chain                                 27904.03%   460.81ns    2.17M
============================================================================
*/
