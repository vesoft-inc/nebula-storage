
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
using TAddEdgeRet = folly::Future<StorageRpcResponse<cpp2::ExecResponse>>;

class Timer {
public:
    explicit Timer(std::string name) : name_(name) {
        startTs_ = std::chrono::high_resolution_clock::now();
        stopTs_ = startTs_;
    }
    ~Timer() {
        using std::chrono::duration_cast;
        auto micro_secs = duration_cast<std::chrono::microseconds>(stopTs_ - startTs_);
        std::cout << name_ << " spend " << micro_secs.count() / 1000 << " ms" << std::endl;
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

    void setupMetaClient(std::string serverName, uint32_t port) {
        std::vector<HostAddr> metas;
        metas.emplace_back(HostAddr(serverName, port));
        meta::MetaClientOptions options;
        metaClient_ = std::make_unique<meta::MetaClient>(worker_,
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

    void init() {
        if (!unInitialized) {
            return;
        }
        std::string msg{""};
        worker_ = std::make_shared<folly::IOThreadPoolExecutor>(20);
        setupMetaClient("hp-server", 6500);
        std::string spaceName{"test"};

        spaceId_ = setupSpace(spaceName, partition_, replica_);
        edgeType_ = setupEdgeSchema("test_edge");

        storageClient_ = std::make_unique<StorageClient>(worker_, metaClient_.get());
        unInitialized = false;

        msg = folly::sformat("sleep for FLAGS_heartbeat_interval_secs + 2 secs");
        std::cout << msg << std::endl;
        sleep(FLAGS_heartbeat_interval_secs + 2);
    }

    cpp2::EdgeKey getOutEdgeKey(int srcId, int rank) {
        static int sum = 10000 * 10000;
        cpp2::EdgeKey edgeKey;
        edgeKey.set_src(toVertexId(srcId));
        edgeKey.set_edge_type(edgeType_);
        edgeKey.set_ranking(rank);
        edgeKey.set_dst(toVertexId(sum - srcId));
        return edgeKey;
    }

    cpp2::NewEdge getOutEdge(int srcId, int rank) {
        cpp2::EdgeKey edgeKey = getOutEdgeKey(srcId, rank);

        std::vector<nebula::Value> vals(1);
        vals.back().setInt(0);

        cpp2::NewEdge newEdge;
        newEdge.set_key(std::move(edgeKey));
        newEdge.set_props(std::move(vals));

        return newEdge;
    }

    cpp2::EdgeKey reverseEdgeKeyDirection(cpp2::EdgeKey& keyIn) {
        cpp2::EdgeKey keyReverse(keyIn);
        std::swap(keyReverse.src, keyReverse.dst);
        keyReverse.edge_type *= -1;
        return keyReverse;
    }

    // folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>>
    TAddEdgeRet addSingleEdge(cpp2::NewEdge edge) {
        std::vector<std::string> propNames{colName_};
        std::vector<cpp2::NewEdge> edges{edge};
        return storageClient_->addEdges(spaceId_, edges, propNames, true).via(worker_.get());
    }

    void addEdgesAsync(int srcIdBase, int num, int concurrent) {
        std::vector<TAddEdgeRet> futures;
        for (int i = 0; i != num; ++i) {
            if (futures.size() >= concurrent) {
                auto fut = folly::collect(futures);
                fut.wait();
                futures.clear();
            }
            cpp2::NewEdge edge = getOutEdge(srcIdBase + i, 0);
            futures.emplace_back(addSingleEdge(edge));

            edge.key = reverseEdgeKeyDirection(edge.key);
            futures.emplace_back(addSingleEdge(edge));
        }
        if (!futures.empty()) {
            folly::collect(futures).wait();
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
            cpp2::EdgeKey key = getOutEdgeKey(srcIdBegin + i, rank);
            updateSingleEdgeAsync(futures, key, rank);

            // cpp2::EdgeKey rKey = reverseEdgeKeyDirection(key);
            // updateSingleEdgeAsync(futures, rKey, rank);

            if (futures.size() >= batch) {
                auto fut = folly::collect(futures);
                fut.wait();
                futures.clear();
            }
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
    std::shared_ptr<folly::IOThreadPoolExecutor> worker_;

    int                 partition_{1};
    int                 replica_{3};
    int                 spaceId_{0};
    int                 srcIdBase_{5000};
    int                 cnt_{50000};
    int                 batch_{30};
    int                 edgeType_{0};
    std::string         colName_{"col1"};
    bool                unInitialized{true};
};

void func() {
    BenchmarkEnv* env = BenchmarkEnv::getInstance();
    int partition = 1;
    int replica = 3;
    int srcIdBase = 5000;
    int cnt = 1000;
    int batch = 30;

    env->init();

    /*
     * case 1: update (no lock)
     * case 2: update (mem lock)
     * case 3: update (raft lock)
     * case 4: update (atomic op)
     * case 5: update (write directly)
     * case 9: return kvstore::ResultCode::SUCCEEDED;
     * */

    // auto timer0 = std::make_unique<Timer>(folly::sformat("insert {} edges", cnt));
    // env->addEdgesAsync(srcIdBase, cnt, batch);
    // timer0->Stop();

    auto timer = std::make_unique<Timer>(folly::sformat("update {} edges atomic op", cnt));
    env->updateEdgesAsync(srcIdBase, cnt, 1, batch);
    timer->Stop();

    std::cout << folly::sformat("total {} edges, partition {}, replica {}, batch {}",
                                cnt,
                                partition,
                                replica,
                                batch) << std::endl;
}

// BENCHMARK(bm_insert, iters) {
//     BenchmarkEnv* env{nullptr};
//     BENCHMARK_SUSPEND {
//         env = BenchmarkEnv::getInstance();
//         env->init();
//     }

//     for (size_t i = 0; i < iters; i++) {
//         env->addEdgesAsync(env->srcIdBase_, env->cnt_, env->batch_);
//     }
// }

// BENCHMARK_RELATIVE(bm_update_write_directly, iters) {
//     BenchmarkEnv* env = BenchmarkEnv::getInstance();
//     for (size_t i = 0; i < iters; i++) {
//         env->updateEdgesAsync(env->srcIdBase_, env->cnt_, 5, env->batch_);
//     }
// }

// BENCHMARK_RELATIVE(bm_update_no_lock, iters) {
//     BenchmarkEnv* env = BenchmarkEnv::getInstance();
//     for (size_t i = 0; i < iters; i++) {
//         env->updateEdgesAsync(env->srcIdBase_, env->cnt_, 1, env->batch_);
//     }
// }

// BENCHMARK_RELATIVE(bm_update_mem_lock, iters) {
//     BenchmarkEnv* env = BenchmarkEnv::getInstance();
//     for (size_t i = 0; i < iters; i++) {
//         env->updateEdgesAsync(env->srcIdBase_, env->cnt_, 2, env->batch_);
//     }
// }

// BENCHMARK_RELATIVE(bm_update_raft_lock, iters) {
//     BenchmarkEnv* env = BenchmarkEnv::getInstance();
//     for (size_t i = 0; i < iters; i++) {
//         env->updateEdgesAsync(env->srcIdBase_, env->cnt_, 3, env->batch_);
//     }
// }

// BENCHMARK_RELATIVE(bm_update_atomic, iters) {
//     BenchmarkEnv* env = BenchmarkEnv::getInstance();
//     for (size_t i = 0; i < iters; i++) {
//         env->updateEdgesAsync(env->srcIdBase_, env->cnt_, 4, env->batch_);
//     }
// }

std::vector<cpp2::UpdatedProp> gUpdatedProps(1);
bool gInsertable = true;
std::vector<std::string> gReturnProps;
int gEdgeSrcBase = 5000;

BENCHMARK(bm_single_update_atomic, iters) {
    UNUSED(iters);
    BenchmarkEnv* env = BenchmarkEnv::getInstance();
    std::vector<cpp2::EdgeKey> keys;
    BENCHMARK_SUSPEND {
        env = BenchmarkEnv::getInstance();
        env->init();
        std::string colName{"c1"};
        gUpdatedProps.back().set_name(colName);
        ConstantExpression val1(0);
        gUpdatedProps.back().set_value(Expression::encode(val1));

        keys.emplace_back(env->getOutEdgeKey(gEdgeSrcBase++, 1));
    }

    // for (size_t i = 0; i < iters; i++) {
    //     auto fut = env->storageClient_->updateEdge(env->spaceId_,
    //                                             keys[0],
    //                                             gUpdatedProps,
    //                                             gInsertable,
    //                                             gReturnProps,
    //                                             gCondition);
    //     fut.wait();
    // }
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
    FLAGS_heartbeat_interval_secs = 1;
    folly::init(&argc, &argv, false);
    folly::runBenchmarks();
    // nebula::storage::func();
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

/*
============================================================================
TransactionPrototypeBenchmark.cpprelative                 time/iter  iters/s
============================================================================
bm_insert                                                     5.75s  173.83m
bm_update_no_lock                                 65.11%      8.84s  113.18m
bm_update_mem_lock                                62.86%      9.15s  109.27m
bm_update_raft_lock                               28.41%     20.25s   49.38m
bm_update_atomic                                   7.79%    1.23min   13.54m
============================================================================

============================================================================
bm_insert                                                     6.05s  165.38m
bm_update_no_lock                                 68.77%      8.79s  113.73m
bm_update_mem_lock                                65.97%      9.17s  109.10m
bm_update_raft_lock                               30.18%     20.03s   49.92m
bm_update_atomic                                  11.27%     53.65s   18.64m
============================================================================

============================================================================
bm_insert                                                     5.96s  167.73m
bm_update_write_directly                          95.50%      6.24s  160.18m
bm_update_no_lock                                 66.71%      8.94s  111.89m
bm_update_mem_lock                                64.97%      9.18s  108.97m
bm_update_raft_lock                               29.59%     20.15s   49.62m
bm_update_atomic                                   7.98%    1.25min   13.38m
============================================================================

*/

/* single update
============================================================================
/home/lionel.liu/code/nb20/nebula-storage/src/storage/test/TransactionPrototypeBenchmark.cpprelative  time/iter  iters/s
============================================================================
bm_single_update_atomic                                      3.00ms   333.06
============================================================================
*/
