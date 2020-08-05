
/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "common/base/Base.h"
#include <gtest/gtest.h>
#include <folly/Format.h>
#include <chrono>

#include "common/clients/storage/GraphStorageClient.h"
#include "common/expression/ConstantExpression.h"

#define FLOG_FMT(...) LOG(INFO) << folly::sformat(__VA_ARGS__)

DECLARE_int32(heartbeat_interval_secs);
DEFINE_string(meta_server, "192.168.8.5:6500", "Meta servers' address.");

namespace nebula {
namespace storage {

using StorageClient = storage::GraphStorageClient;
using TAddEdgeRet = folly::Future<StorageRpcResponse<cpp2::ExecResponse>>;

struct TossExecEnv {
    static TossExecEnv* getInstance(int part, int replica) {
        static TossExecEnv inst(part, replica);
        return &inst;
    }

    TossExecEnv(int part, int replica) : part_(part), replica_(replica) {
        worker_ = std::make_shared<folly::IOThreadPoolExecutor>(20);
        setupMetaClient("hp-server", 6500);
        std::string spaceName{"test"};

        spaceId_ = setupSpace(spaceName, part_, replica_);
        edgeType_ = setupEdgeSchema("test_edge");

        storageClient_ = std::make_unique<StorageClient>(worker_, metaClient_.get());

        int sleepSecs = FLAGS_heartbeat_interval_secs + 5;
        while (sleepSecs) {
            LOG(INFO) << "sleep for " << sleepSecs-- << " sec";
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
        auto msg = folly::sformat("edgeType_ = {}", edgeType_);
        LOG(INFO) << msg;
        return edgeType_;
    }

    cpp2::EdgeKey getOutEdgeKey(int srcId, int rank) {
        static int sum = 10000 * 10000;
        cpp2::EdgeKey edgeKey;
        edgeKey.set_src(std::to_string(srcId));
        edgeKey.set_edge_type(edgeType_);
        edgeKey.set_ranking(rank);
        edgeKey.set_dst(std::to_string(sum - srcId));
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

    void addEdgesInParallel(int srcIdBase, int num, int concurrent) {
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

    // folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>>
    void addSingleTossEdge(cpp2::NewEdge& edge) {
        LOG(INFO) << "[enter] " << __func__;
        std::vector<std::string> propNames{colName_};
        std::vector<cpp2::NewEdge> edges{edge};
        auto f = storageClient_->atomicAddEdges(spaceId_,
                                                edges,
                                                propNames,
                                                true).via(worker_.get());

        f.wait();
        if (!f.valid()) {
            std::string msg{"future not valid"};
            LOG(INFO) << msg;
            LOG(FATAL) << msg;
        }
        auto resp = std::move(f).get();
        if (!resp.succeeded()) {
            std::ostringstream oss;
            for (auto& part : resp.failedParts()) {
                oss << "part=" << part.first << ", err=" << static_cast<int>(part.second) << "\n";
            }
            auto errMessage = oss.str();
            LOG(INFO) << errMessage;
            LOG(FATAL) << errMessage << "!resp.succeeded";
        }
        LOG(INFO) << "[enter] " << __func__;
    }

    void addTossEdge(int srcIdBase, int num, int concurrent) {
        UNUSED(concurrent);
        LOG(INFO) << "[enter] " << __func__;

        for (int i = 0; i != num; ++i) {
            cpp2::NewEdge edge = getOutEdge(srcIdBase + i, 0);
            addSingleTossEdge(edge);
        }
        LOG(INFO) << "[exit] " << __func__;
    }

    std::shared_ptr<folly::IOThreadPoolExecutor> worker_;
    std::unique_ptr<meta::MetaClient> metaClient_;
    std::unique_ptr<StorageClient> storageClient_;


    int                 part_{1};
    int                 replica_{1};
    int                 spaceId_{0};

    int                 srcIdBase_{5000};
    int                 edgeType_{0};
    std::string         colName_{"col1"};
};

void func() {
    int part = 2;
    int replica = 3;
    TossExecEnv* env = TossExecEnv::getInstance(part, replica);

    int vidStart = 5000;
    int cnt = 1;
    int concurrent = 30;

    // env->addEdgesAsync(vidStart, cnt, concurrent);
    env->addTossEdge(vidStart, cnt, concurrent);

    FLOG_FMT("edges={}, part={}, rep={}, concurrent={}", cnt, part, replica, concurrent);
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
    FLAGS_heartbeat_interval_secs = 1;
    FLAGS_logtostderr = 1;
    folly::init(&argc, &argv, false);
    // folly::runBenchmarks();
    // nebula::storage::func();
    return 0;
}

