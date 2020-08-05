
/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include <chrono>
#include <folly/Benchmark.h>
#include <folly/Format.h>
#include <folly/container/Enumerate.h>

#include "common/base/Base.h"
#include "common/clients/storage/GraphStorageClient.h"
#include "common/expression/ConstantExpression.h"

#define FLOG_FMT(...) LOG(INFO) << folly::sformat(__VA_ARGS__)

DECLARE_int32(heartbeat_interval_secs);
DEFINE_string(meta_server, "192.168.8.5:6500", "Meta servers' address.");

namespace nebula {
namespace storage {

constexpr int gSum = 10000 * 10000;

using StorageClient = storage::GraphStorageClient;
using SFRpcResp = folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>>;

struct TossEnvironment {
    std::shared_ptr<folly::IOThreadPoolExecutor>    worker_;
    std::unique_ptr<meta::MetaClient>               mClient_;
    std::unique_ptr<StorageClient>                  sClient_;

    int                                             spaceId_{0};
    int                                             edgeType_{0};
    std::string                                     colName_{"c1"};

    static TossEnvironment* getInstance(const std::string& metaName, int metaPort) {
        static TossEnvironment inst(metaName, metaPort);
        return &inst;
    }

    TossEnvironment(const std::string& metaHostName, int port) {
        worker_ = std::make_shared<folly::IOThreadPoolExecutor>(20);
        mClient_ = setupMetaClient(metaHostName, port);
        sClient_ = std::make_unique<StorageClient>(worker_, mClient_.get());
    }

    std::unique_ptr<meta::MetaClient> setupMetaClient(std::string serverName, uint32_t port) {
        std::vector<HostAddr> metas;
        metas.emplace_back(HostAddr(serverName, port));
        meta::MetaClientOptions options;
        auto client = std::make_unique<meta::MetaClient>(worker_,
                                                         metas,
                                                         options);
        if (!client->waitForMetadReady()) {
            LOG(FATAL) << "waitForMetadReady failed!";
        }
        return client;
    }

    /*
     * setup space and edge type, then describe the cluster
     * */
    void init(const std::string& spaceName,
              int part,
              int replica,
              std::vector<meta::cpp2::ColumnDef>* pCols = nullptr) {
        spaceId_ = setupSpace(spaceName, part, replica);
        if (pCols) {
            edgeType_ = setupEdgeSchema("test_edge", *pCols);
        } else {
            edgeType_ = setupDefaultIntEdgeSchema("test_edge");
        }

        int sleepSecs = FLAGS_heartbeat_interval_secs + 2;
        while (sleepSecs) {
            LOG(INFO) << "sleep for " << sleepSecs-- << " sec";
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        while (1) {
            auto statusOrLeaderMap = mClient_->loadLeader();
            if (!statusOrLeaderMap.ok()) {
                LOG(FATAL) << "mClient_->loadLeader() failed!!!!!!";
            }

            if (statusOrLeaderMap.value().size() == part) {
                for (auto& leader : statusOrLeaderMap.value()) {
                    auto mmm = folly::sformat("space/partition/host: {}/{}/",
                                            spaceId_, leader .first.second);
                    LOG(INFO) << mmm << leader.second;
                }
                break;
            }
        }
    }

    StorageClient* getStorageClient() {
        return sClient_.get();
    }

    int setupSpace(std::string spaceName, int nPart, int nReplica) {
        auto fDropSpace = mClient_->dropSpace(spaceName, true);
        fDropSpace.wait();
        LOG(INFO) << "dropSpace(" << spaceName << ")";

        meta::SpaceDesc spaceDesc(spaceName, nPart, nReplica);
        auto fCreateSpace = mClient_->createSpace(spaceDesc, true);
        fCreateSpace.wait();
        auto spaceId = fCreateSpace.value().value();
        LOG(INFO) << folly::sformat("spaceId = {}", spaceId);;
        return spaceId;
    }

    EdgeType setupDefaultIntEdgeSchema(const std::string& edgeName) {
        std::vector<meta::cpp2::ColumnDef> columns;
        Value defaultValue;
        defaultValue.setInt(0);
        columns.emplace_back();
        columns.back().set_name(colName_);
        columns.back().set_default_value(defaultValue);
        columns.back().set_type(meta::cpp2::PropertyType::INT32);
        return setupEdgeSchema(edgeName, columns);
    }

    EdgeType setupEdgeSchema(const std::string& edgeName,
                             std::vector<meta::cpp2::ColumnDef> columns) {
        meta::cpp2::Schema schema;
        schema.set_columns(std::move(columns));

        auto fCreateEdgeSchema = mClient_->createEdgeSchema(spaceId_, edgeName, schema, true);
        fCreateEdgeSchema.wait();

        if (!fCreateEdgeSchema.valid() || !fCreateEdgeSchema.value().ok()) {
            LOG(FATAL) << "createEdgeSchema failed";
        }
        return fCreateEdgeSchema.value().value();
    }

    cpp2::EdgeKey generateEdgeKey(int srcId, int rank, int dstId = 0) {
        cpp2::EdgeKey edgeKey;
        edgeKey.set_src(std::to_string(srcId));
        edgeKey.set_edge_type(edgeType_);
        edgeKey.set_ranking(rank);
        if (dstId == 0) {
            dstId = gSum - srcId;
        }
        edgeKey.set_dst(std::to_string(dstId));
        return edgeKey;
    }

    cpp2::NewEdge generateEdge(int srcId,
                               int rank,
                               std::vector<nebula::Value> values,
                               int dstId = 0) {
        cpp2::NewEdge newEdge;

        cpp2::EdgeKey edgeKey = generateEdgeKey(srcId, rank, dstId);
        newEdge.set_key(std::move(edgeKey));
        newEdge.set_props(std::move(values));
        
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
    SFRpcResp addTossEdgeAsync(std::vector<std::string> propNames, T&& edge) {
        std::vector<cpp2::NewEdge> edges{edge};
        return sClient_->atomicAddEdges(spaceId_, edges, propNames, true);
    }

    template<typename T>
    void addOldEdgeSync(T&& edge) {
        auto f = addOldEdgeAsync(std::forward<T>(edge));
        f.wait();
    }

    template<typename T>
    void addTossEdgeSync(const std::string& colName, T&& edge) {
        auto f = addTossEdgeAsync({colName}, std::forward<T>(edge));
        f.wait();
    }

    std::vector<Value> getProps(cpp2::NewEdge edge) {
        // nebula::DataSet ds;  ===> will crash if not set
        nebula::DataSet ds({kSrc, kType, kRank, kDst});
        std::vector<Value> edgeInfo{edge.key.src,
                                    edge.key.edge_type,
                                    edge.key.ranking,
                                    edge.key.dst};
        List row(std::move(edgeInfo));
        ds.rows.emplace_back(std::move(row));

        std::vector<cpp2::EdgeProp> props;
        cpp2::EdgeProp oneProp;
        oneProp.type = edge.key.edge_type;
        props.emplace_back(oneProp);

        auto frpc = sClient_->getProps(spaceId_,
                                      std::move(ds), /*DataSet*/
                                      nullptr, /*vector<cpp2::VertexProp>*/
                                      &props, /*vector<cpp2::EdgeProp>*/
                                      nullptr /*expressions*/).via(worker_.get());
        frpc.wait();
        if (!frpc.valid()) {
            LOG(FATAL) << "getProps rpc invalid()";
        }
        StorageRpcResponse<cpp2::GetPropResponse>& rpcResp = frpc.value();

        auto resps = rpcResp.responses();
        if (resps.empty()) {
            LOG(FATAL) << "getProps() resps.empty())";
        }
        cpp2::GetPropResponse& propResp = resps.front();
        cpp2::ResponseCommon result = propResp.result;
        std::vector<cpp2::PartitionResult>& fparts = result.failed_parts;
        if (!fparts.empty()) {
            for (cpp2::PartitionResult& res : fparts) {
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

        bool ret = (lhs == rhs);
        if (!ret) {
            size_t len = std::max(lhs.size(), rhs.size());
            for (size_t i = 0; i != len; ++i) {
                std::string str1 = i < lhs.size() ? lhs[i] : "n/a";
                std::string str2 = i < rhs.size() ? rhs[i] : "n/a";
                LOG(INFO) << folly::sformat("compare {0}, {1}", str1, str2);
            }
        }
        return ret;
    }

    void printDataSet(const DataSet& ds) {
        for (auto&& it : folly::enumerate(ds.colNames)) {
            LOG(INFO) << "colNames[" << it.index << "]=" << *it;
        }

        for (auto& row : ds.rows) {
            for (auto&& it : folly::enumerate(row.values)) {
                LOG(INFO) << "row.values[" << it.index << "]=" << *it;
            }
        }
    }
};

// deprecated
// TossEnvironment(int part, int replica) {
//     worker_ = std::make_shared<folly::IOThreadPoolExecutor>(20);
//     setupMetaClient("hp-server", 6500);
//     std::string spaceName{"test"};

//     spaceId_ = setupSpace(spaceName, part, replica);
//     edgeType_ = setupDefaultIntEdgeSchema("test_edge");

//     sClient_ = std::make_unique<StorageClient>(worker_, mClient_.get());

//     int sleepSecs = FLAGS_heartbeat_interval_secs + 5;
//     while (sleepSecs) {
//         LOG(INFO) << "sleep for " << sleepSecs-- << " sec";
//         std::this_thread::sleep_for(std::chrono::seconds(1));
//     }

//     auto statusOrLeaderMap = mClient_->loadLeader();
//     if (!statusOrLeaderMap.ok()) {
//         LOG(FATAL) << "mClient_->loadLeader() failed!!!!!!";
//     }

//     auto leaderMap = statusOrLeaderMap.value();
//     for (auto& leader : leaderMap) {
//         auto mmm = folly::sformat("space/partition/host: {}/{}/",
//                                     spaceId_, leader .first.second);
//         LOG(INFO) << mmm << leader.second;
//     }
// }

// BENCHMARK(bm_burn_1ms, iters) {
//     folly::BenchmarkSuspender braces;
//     TossEnvironment* env = TossEnvironment::getInstance();
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
 *     TossEnvironment* env = TossEnvironment::getInstance();
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
 *     TossEnvironment* env = TossEnvironment::getInstance();
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
    TossEnvironment* env = TossEnvironment::getInstance();
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
    TossEnvironment* env = TossEnvironment::getInstance();
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
