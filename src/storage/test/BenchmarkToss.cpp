
/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include <chrono>
#include <folly/Benchmark.h>
#include <folly/Format.h>
#include <folly/container/Enumerate.h>
#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/clients/storage/GraphStorageClient.h"
#include "common/expression/ConstantExpression.h"

#define FLOG_FMT(...) LOG(INFO) << folly::sformat(__VA_ARGS__)

DECLARE_int32(heartbeat_interval_secs);
DEFINE_string(meta_server, "192.168.8.5:6500", "Meta servers' address.");

namespace nebula {
namespace storage {

using StorageClient = storage::GraphStorageClient;
using SFRpcResp = folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>>;

int gPart = 2;
int gReplica = 3;

struct TossExecEnv {
    std::shared_ptr<folly::IOThreadPoolExecutor> worker_;
    std::unique_ptr<meta::MetaClient> metaClient_;
    std::unique_ptr<StorageClient> sClient_;

    int                 spaceId_{0};

    int                 vertexId_{5000};
    int                 edgeType_{0};
    std::string         colName_{"col1"};

    static TossExecEnv* getInstance() {
        static TossExecEnv inst(gPart, gReplica);
        return &inst;
    }

    TossExecEnv(int part, int replica) {
        worker_ = std::make_shared<folly::IOThreadPoolExecutor>(20);
        setupMetaClient("hp-server", 6500);
        std::string spaceName{"test"};

        spaceId_ = setupSpace(spaceName, part, replica);
        edgeType_ = setupEdgeSchema("test_edge");

        sClient_ = std::make_unique<StorageClient>(worker_, metaClient_.get());

        int sleepSecs = FLAGS_heartbeat_interval_secs + 5;
        while (sleepSecs) {
            LOG(INFO) << "sleep for " << --sleepSecs << " sec";
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        auto statusOrLeaderMap = metaClient_->loadLeader();
        if (statusOrLeaderMap.ok()) {
            auto leaderMap = statusOrLeaderMap.value();
            LOG(INFO) << "statusOrLeaderMap.ok() leaderMap.size()=" << leaderMap.size();
            for (auto& leader : leaderMap) {
                auto mmm = folly::sformat("space/partition/host: {}/{}/",
                                           spaceId_, leader .first.second);
                LOG(INFO) << mmm << leader.second;
            }
        } else {
            LOG(INFO) << "!statusOrLeaderMap.ok()";
        }
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
        LOG(INFO) << "dropSpace(" << spaceName << ")";

        meta::SpaceDesc spaceDesc(spaceName, nPart, nReplica);
        auto futCreateSpace = metaClient_->createSpace(spaceDesc, true);
        futCreateSpace.wait();
        spaceId_ = futCreateSpace.value().value();
        auto msg = folly::sformat("spaceId_ = {}", spaceId_);
        LOG(INFO) << msg;
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
        FLOG_FMT("edgeType_ = {}", edgeType_);
        return edgeType_;
    }

    cpp2::EdgeKey generateEdgeKey(int srcId, int rank) {
        static int sum = 10000 * 10000;
        cpp2::EdgeKey edgeKey;
        edgeKey.set_src(std::to_string(srcId));
        edgeKey.set_edge_type(edgeType_);
        edgeKey.set_ranking(rank);
        edgeKey.set_dst(std::to_string(sum - srcId));
        return edgeKey;
    }

    cpp2::NewEdge generateEdge(int srcId, int rank) {
        cpp2::NewEdge newEdge;

        cpp2::EdgeKey edgeKey = generateEdgeKey(srcId, rank);
        newEdge.set_key(std::move(edgeKey));

        std::vector<nebula::Value> vals(1);
        vals.back().setInt(srcId);
        newEdge.set_props(std::move(vals));
        return newEdge;
    }

    cpp2::NewEdge reverseEdge(const cpp2::NewEdge& e0) {
        cpp2::NewEdge e(e0);
        std::swap(e.key.src, e.key.dst);
        e.key.edge_type *= -1;
        return e;
    }

    template<typename T>
    SFRpcResp addOldEdgeAsync(T&& e0) {
        std::vector<std::string> propNames{colName_};
        auto e1 = reverseEdge(e0);
        std::vector<cpp2::NewEdge> edges{e0, e1};
        return sClient_->addEdges(spaceId_, edges, propNames, true);
    }

    template<typename T>
    SFRpcResp addTossEdgeAsync(T&& edge) {
        std::vector<std::string> propNames{colName_};
        std::vector<cpp2::NewEdge> edges{edge};
        return sClient_->atomicAddEdges(spaceId_, edges, propNames, true);
    }

    template<typename T>
    SFRpcResp addEdgeAsync(T&& edge, bool useToss) {
        if (useToss) {
            return addTossEdgeAsync(std::forward<T>(edge));
        }
        return addOldEdgeAsync(std::forward<T>(edge));
    }

    template<typename T>
    void addOldEdgeSync(T&& edge) {
        auto f = addOldEdgeAsync(std::forward<T>(edge));
        f.wait();
    }

    template<typename T>
    void addTossEdgeSync(T&& edge) {
        auto f = addTossEdgeAsync(std::forward<T>(edge));
        f.wait();
    }

    std::vector<Value> getProps(cpp2::NewEdge e0) {
        // nebula::DataSet ds;  ===> will crash if not set
        nebula::DataSet ds({kSrc, kType, kRank, kDst});
        std::vector<Value> edgeInfo{e0.key.src, e0.key.edge_type, e0.key.ranking, e0.key.dst};
        List row(std::move(edgeInfo));
        ds.rows.emplace_back(std::move(row));

        std::vector<cpp2::EdgeProp> props;
        cpp2::EdgeProp oneProp;
        oneProp.type = e0.key.edge_type;
        props.emplace_back(oneProp);

        auto fut = sClient_->getProps(spaceId_,
                                     std::move(ds), /*DataSet*/
                                     nullptr, /*vector<cpp2::VertexProp>*/
                                     &props, /*vector<cpp2::EdgeProp>*/
                                     nullptr /*expressions*/).via(worker_.get());
        fut.wait();
        if (!fut.valid()) {
            LOG(FATAL) << "getProps rpc invalid()";
        }
        StorageRpcResponse<cpp2::GetPropResponse>& rpcResp = fut.value();
        auto resps = rpcResp.responses();
        if (resps.empty()) {
            LOG(FATAL) << "getProps() resps.empty())";
        }
        cpp2::GetPropResponse& propResp = resps.front();
        cpp2::ResponseCommon result = propResp.result;
        std::vector<cpp2::PartitionResult>& fparts = result.failed_parts;
        if (!fparts.empty()) {
            for (cpp2::PartitionResult& res :  fparts) {
                LOG(INFO) << "part_id: " << res.part_id
                          << ", part leader " << res.leader
                          << ", code " << static_cast<int>(res.code);
            }
            LOG(FATAL) << "getProps() !failed_parts.empty())";
        }
        nebula::DataSet& dataSet = propResp.props;
        std::vector<Row>& rows = dataSet.rows;
        if (rows.empty()) {
            LOG(FATAL) << "getProps() dataSet.rows.empty())";
        }
        std::vector<Value>& values = rows[0].values;
        if (values.empty()) {
            LOG(FATAL) << "getProps() values.empty())";
        }
        return values;
    }

    bool Equal(cpp2::NewEdge& e0, const std::vector<Value>& vals) {
        LOG(INFO) << folly::format("e0.props.size()={}, vals.size()={}",
                                   e0.props.size(),
                                   vals.size());

        auto k = e0.key;
        std::vector<std::string> lhs{k.src,
                                     std::to_string(k.edge_type),
                                     std::to_string(k.ranking),
                                     k.dst};
        std::vector<std::string> l1(e0.props.size());
        std::transform(e0.props.begin(), e0.props.end(), l1.begin(), [](auto &it){
            return it.toString();
        });

        lhs.insert(lhs.end(), l1.begin(), l1.end());

        std::vector<std::string> rhs(vals.size());
        std::transform(vals.begin(), vals.end(), rhs.begin(), [](auto &it){
            return it.toString();
        });

        bool ret = lhs == rhs;
        if (!ret) {
            for (auto&& it : folly::enumerate(lhs)) {
                LOG(INFO) << "lhs[" << it.index << "]=" << *it;
            }

            for (auto&& it : folly::enumerate(rhs)) {
                LOG(INFO) << "rhs[" << it.index << "]=" << *it;
            }
        }
        return ret;
    }
};

void addOldEdgeThenCheck() {
    int srcId = 5000;
    int type = 1000;
    TossExecEnv* env = TossExecEnv::getInstance();
    cpp2::NewEdge e0 = env->generateEdge(srcId, type);

    env->addOldEdgeSync(e0);
    auto vals = env->getProps(e0);

    LOG(INFO) << "addOldEdgeThenCheck succeed: " << env->Equal(e0, vals);
}

void addTossEdgeThenCheck() {
    int srcId = 6000;
    int type = 1000;
    TossExecEnv* env = TossExecEnv::getInstance();
    cpp2::NewEdge e0 = env->generateEdge(srcId, type);

    env->addTossEdgeSync(e0);
    auto vals = env->getProps(e0);

    LOG(INFO) << "addTossEdgeThenCheck succeed: " << env->Equal(e0, vals);
}

std::atomic<int> gSrdId{5000};

// BENCHMARK(bm_burn_1ms, iters) {
//     folly::BenchmarkSuspender braces;
//     TossExecEnv* env = TossExecEnv::getInstance();
//     UNUSED(env);
//     braces.dismiss();
//     for (size_t i = 0; i < iters; i++) {
//         std::this_thread::sleep_for(std::chrono::milliseconds(1));
//     }
// }

/*
 * single add edge req PK, old/toss ~ 5:1
 * one raft write takes about 1~2ms
 * toss need to 1: write lock
 *              2: write in edge
 *              3: write out edge
 *              4: remove lock
 * ============================================================================
 * nebula-storage/src/storage/test/BenchmarkToss.cpprelative  time/iter  iters/s
 * ============================================================================
 * bm_add_edge_old                                            991.90us    1.01K
 * bm_add_edge_toss                                  22.06%     4.50ms   222.42
 * ============================================================================
 *
 * BENCHMARK(bm_add_edge_old, iters) {
 *     folly::BenchmarkSuspender braces;
 *     TossExecEnv* env = TossExecEnv::getInstance();
 *     static int type = 999;
 *     cpp2::NewEdge e0 = env->generateEdge(gSrdId++, type);
 *     braces.dismiss();
 *
 *     for (size_t i = 0; i < iters; i++) {
 *         env->addOldEdgeSync(e0);
 *     }
 * }
 *
 * BENCHMARK_RELATIVE(bm_add_edge_toss, iters) {
 *     folly::BenchmarkSuspender braces;
 *     TossExecEnv* env = TossExecEnv::getInstance();
 *     static int type = 1000;
 *     cpp2::NewEdge e0 = env->generateEdge(gSrdId++, type);
 *     braces.dismiss();
 *
 *     for (size_t i = 0; i < iters; i++) {
 *         env->addTossEdgeSync(e0);
 *     }
 * }
 * */

/*
 * concurrent add edge req PK, old:toss => 5:1 weired...
 * one raft write takes about 1~2ms
 * toss need to 1: write lock
 *              2: write in edge
 *              3: write out edge
 *              4: remove lock
int gConcurrentNum = 30;
BENCHMARK(bm_concurrent_add_edge_old, iters) {
    UNUSED(iters);
    folly::BenchmarkSuspender braces;
    TossExecEnv* env = TossExecEnv::getInstance();
    static int type = 999;
    std::vector<cpp2::NewEdge> in;
    for (size_t i = 0; i != gConcurrentNum; ++i) {
        in.emplace_back(env->generateEdge(gSrdId++, type));
    }
    std::vector<SFRpcResp> resps;
    braces.dismiss();

    for (size_t i = 0; i != gConcurrentNum; ++i) {
        resps.emplace_back(env->addOldEdgeAsync(in[i]));
    }

    folly::collectAllSemiFuture(resps).wait();
}

BENCHMARK_RELATIVE(bm_concurrent_add_edge_toss, iters) {
    UNUSED(iters);
    folly::BenchmarkSuspender braces;
    TossExecEnv* env = TossExecEnv::getInstance();
    static int type = 999;
    std::vector<cpp2::NewEdge> in;
    for (size_t i = 0; i != gConcurrentNum; ++i) {
        in.emplace_back(env->generateEdge(gSrdId++, type));
    }
    std::vector<SFRpcResp> resps;
    braces.dismiss();

    for (size_t i = 0; i != gConcurrentNum; ++i) {
        resps.emplace_back(env->addTossEdgeAsync(in[i]));
    }

    folly::collectAllSemiFuture(resps).wait();
}

 * */

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
    FLAGS_heartbeat_interval_secs = 1;
    FLAGS_logtostderr = 1;

    folly::init(&argc, &argv, false);
    nebula::storage::TossExecEnv::getInstance();
    folly::runBenchmarks();
    return 0;
}
