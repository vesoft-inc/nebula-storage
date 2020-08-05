/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "TossEnvironment.hpp"

#define FLOG_FMT(...) LOG(INFO) << folly::sformat(__VA_ARGS__)

namespace nebula {
namespace storage {

using StorageClient = storage::GraphStorageClient;
/*
 * equal to TossPhase
 * */
enum class TossPhase {
    PREPATRE            = 10,
    LOCK                = 15,
    EDGE2_PROCESSOR     = 20,
    COMMIT_EDGE2_REQ    = 25,
    COMMIT_EDGE2_RESP   = 26,
    COMMIT_EDGE1        = 30,
    UNLOCK_EDGE         = 35,
    CLEANUP             = 40,
};

class TossTest : public ::testing::Test {
protected:
    std::string     gMetaName{"hp-server"};
    int             gMetaPort = 6500;

    std::string     gSpaceName{"test"};
    int             gPart = 2;
    int             gReplica = 3;

    int             gSrcVid = 5000;

    int             gBombPrefix = 7777;
};

// TEST_F(TossTest, AddEdgeTestSucceed) {
//     auto* env = TossEnvironment::getInstance();

//     int srcId = 5000;
//     int rank = 1000;
//     cpp2::NewEdge e0 = env->generateEdge(srcId, rank);

//     env->addOldEdgeSync(e0);
//     auto vals = env->getProps(e0);

//     ASSERT_TRUE(env->Equal(e0, vals));
// }

TEST_F(TossTest, BasicApiReturnErrorCode) {
    // auto* env = TossEnvironment::getInstance();

    // int srcId = 5000;
    // int rank = 77771001;
    // cpp2::NewEdge edge = env->generateEdge(srcId, rank);
    // auto* sClient = env->getStorageClient();

    // std::vector<std::string> propNames{env->colName_};
    // std::vector<cpp2::NewEdge> edges{edge};

    // auto f = sClient->atomicAddEdges(env->spaceId_,
    //                                     edges,
    //                                     propNames,
    //                                     true);
    // f.wait();
    // ASSERT_TRUE(f.valid());
    // auto rpcResp = f.value();
    // ASSERT_FALSE(rpcResp.succeeded());
    // if (!rpcResp.succeeded()) {
    //     for (auto& it : rpcResp.failedParts()) {
    //         LOG(INFO) << "failed part " << it.first << ", err=" << static_cast<int>(it.second);
    //     }
    // }
}

TEST_F(TossTest, getPropsWorksWell_try_get_invalid_edge) {
    // auto* env = TossEnvironment::getInstance();

    // int srcId = 5000;
    // int rank = 10001;
    // cpp2::NewEdge edge = env->generateEdge(srcId, rank);

    // auto vals = env->getProps(edge);

    // for (auto&& it : folly::enumerate(vals)) {
    //     LOG(INFO) << "vals[" << it.index << "].type=" << static_cast<int>(it->type());
    // }

    // ASSERT_TRUE(std::all_of(vals.begin(), vals.end(), [](auto& val){
    //                 return val.toString() == "__NULL__";
    //             }));
}

TEST_F(TossTest, AddEdgeFailedTest_prepare) {
    // auto* env = TossEnvironment::getInstance();

    // int srcId = 5000;
    // int rank = 10001001;
    // cpp2::NewEdge edge = env->generateEdge(srcId, rank);

    // env->addTossEdgeSync(edge);
    // auto vals = env->getProps(edge);

    // ASSERT_TRUE(std::all_of(vals.begin(), vals.end(), [](auto& val){
    //                 return val.toString() == "__NULL__";
    //             }));

    // cpp2::NewEdge edge2 = env->generateEdge(srcId, rank/10000);
    // env->addTossEdgeSync(edge2);
    // auto vals2 = env->getProps(edge2);

    // LOG(INFO) << "check env->Equal(edge2, vals2)";
    // ASSERT_TRUE(env->Equal(edge2, vals2));
}

TEST_F(TossTest, PrepareThrowException) {
    // auto* env = TossEnvironment::getInstance();

    // int srcId = 5000;
    // int rank = 77771002;
    // cpp2::NewEdge edge = env->generateEdge(srcId, rank);
    // auto* sClient = env->getStorageClient();

    // std::vector<std::string> propNames{env->colName_};
    // std::vector<cpp2::NewEdge> edges{edge};

    // auto f = sClient->atomicAddEdges(env->spaceId_,
    //                                     edges,
    //                                     propNames,
    //                                     true);
    // f.wait();
    // ASSERT_TRUE(f.valid());
    // auto rpcResp = f.value();
    // ASSERT_FALSE(rpcResp.succeeded());
    // if (!rpcResp.succeeded()) {
    //     for (auto& it : rpcResp.failedParts()) {
    //         LOG(INFO) << "failed part " << it.first << ", err=" << static_cast<int>(it.second);
    //     }
    // }
}

TEST_F(TossTest, ConflictAddEdgeRequest) {
    // auto* env = TossEnvironment::getInstance();
    // int srcId = 5000;
    // int rank = 77771100;
    // cpp2::NewEdge edge = env->generateEdge(srcId, rank);
    // auto* sClient = env->getStorageClient();

    // std::vector<std::string> propNames{env->colName_};
    // std::vector<cpp2::NewEdge> edges{edge};
    // auto f = sClient->atomicAddEdges(env->spaceId_,
    //                                     edges,
    //                                     propNames,
    //                                     true);
    // f.wait();
    // ASSERT_TRUE(f.valid());
    // auto rpcResp = f.value();
    // ASSERT_FALSE(rpcResp.succeeded());
    // if (!rpcResp.succeeded()) {
    //     for (auto& it : rpcResp.failedParts()) {
    //         LOG(INFO) << "failed part " << it.first << ", err=" << static_cast<int>(it.second);
    //         ASSERT_EQ(it.second, cpp2::ErrorCode::E_ADD_EDGE_CONFILCT);
    //     }
    // }
}

TEST_F(TossTest, forwarTransactionThrowException) {
    // auto* env = TossEnvironment::getInstance();
    // int srcId = 5000;
    // int rank = 77771200;
    // cpp2::NewEdge edge = env->generateEdge(srcId, rank);
    // auto* sClient = env->getStorageClient();

    // std::vector<std::string> propNames{env->colName_};
    // std::vector<cpp2::NewEdge> edges{edge};
    // auto f = sClient->atomicAddEdges(env->spaceId_,
    //                                     edges,
    //                                     propNames,
    //                                     true);
    // f.wait();
    // ASSERT_TRUE(f.valid());
    // auto rpcResp = f.value();
    // ASSERT_FALSE(rpcResp.succeeded());
    // if (!rpcResp.succeeded()) {
    //     for (auto& it : rpcResp.failedParts()) {
    //         FLOG_FMT("failed part {}, err {}",
    //                  it.first, static_cast<int>(it.second));
    //         // ASSERT_EQ(it.second, cpp2::ErrorCode::E_ADD_EDGE_CONFILCT);
    //     }
    // }
}

/*
 * commit in edge succeeded then failed
 * assert get prop will restore this transaction (2020-07-31)
 * */
// TEST_F(TossTest, commitInEdgeSucceededThenFailed) {
//     /*
//      * Step 1 of 3: add inEdge succeed, outEdge failed
//      * */
//     auto* env = TossEnvironment::getInstance(gMetaName, gMetaPort);
//     env->init(gSpaceName, gPart, gReplica);

//     int rank = 77771302;
//     std::vector<nebula::Value> vals(1);
//     vals.back().setInt(gSrcVid);

//     cpp2::NewEdge edge = env->generateEdge(gSrcVid, rank, vals);
//     auto* sClient = env->getStorageClient();

//     std::vector<std::string> propNames{env->colName_};
//     std::vector<cpp2::NewEdge> edges{edge};
//     auto f = sClient->atomicAddEdges(env->spaceId_,
//                                      edges,
//                                      propNames,
//                                      true);
//     f.wait();
//     ASSERT_TRUE(f.valid());
//     auto rpcResp = f.value();
//     ASSERT_FALSE(rpcResp.succeeded());
//     for (auto& it : rpcResp.failedParts()) {
//         FLOG_FMT("failed part {}, err {}", it.first, static_cast<int>(it.second));
//     }

//     /*
//      * Step 2 of 3: check get inedge directly will succeed
//      * */
//     auto reversedEdge = env->reverseEdge(edge);
//     auto vals2 = env->getProps(reversedEdge);
//     for (auto&& it : folly::enumerate(vals2)) {
//         LOG(INFO) << "vals2[" << it.index << "], type= " << it->type() << ", val=" << *it;
//     }

//     /*
//      * Step 3 of 3: get out edge will triage incomplete transaction
//      *              out edge will getProp succeed
//      * */
//     auto vals3 = env->getProps(edge);
//     for (auto&& it : folly::enumerate(vals3)) {
//         LOG(INFO) << "vals3[" << it.index << "], type= " << it->type() << ", val=" << *it;
//     }

//     ASSERT_EQ(edge.key.src, vals3[0]);
//     ASSERT_EQ(edge.key.edge_type, vals3[1]);
//     ASSERT_EQ(edge.key.ranking, vals3[2]);
//     ASSERT_EQ(edge.key.dst, vals3[3]);
//     ASSERT_EQ(edge.props[0], vals3[4]);
// }

// TEST_F(TossTest, getPropsSucceedWithMultiProps) {
//     auto* env = TossEnvironment::getInstance(gMetaName, gMetaPort);

//     size_t nType = 4;
//     std::vector<std::string> colNames;
//     for (auto i = 0U; i < nType; ++i) {
//         colNames.emplace_back(folly::sformat("c{}", i+1));
//     }

//     std::vector<meta::cpp2::PropertyType> types {
//         meta::cpp2::PropertyType::BOOL,
//         meta::cpp2::PropertyType::INT64,
//         meta::cpp2::PropertyType::DOUBLE,
//         meta::cpp2::PropertyType::STRING
//     };

//     std::vector<meta::cpp2::ColumnDef> colDefs(nType);
//     for (auto i = 0U; i != nType; ++i) {
//         colDefs[i].set_name(colNames[i]);
//         // colDefs[i].set_default_value(values[i]);
//         colDefs[i].set_type(types[i]);
//     }
//     env->init(gSpaceName, gPart, gReplica, &colDefs);

//     int failedPhase = static_cast<int>(TossPhaseT::SUCCEEDED);
//     int errorCode = static_cast<int>(cpp2::ErrorCode::E_TXN_ERR_UNKNOWN);
//     int rank = failedPhase * 100 + errorCode;

//     std::vector<nebula::Value> values(nType);
//     values[0].setBool(false);
//     values[1].setInt(65536);
//     values[2].setFloat(3.14f);
//     values[3].setStr("tang_yan");
//     cpp2::NewEdge edge = env->generateEdge(gSrcVid, rank, values);

//     auto f = env->addTossEdgeAsync(colNames, edge);
//     f.wait();
//     ASSERT_TRUE(f.valid());
//     ASSERT_TRUE(f.value().succeeded());

//     auto rpcResp = f.value();
//     /*
//      * Step 2 of 2: out edge shoule be get without error
//      * */
//     auto v2 = env->getProps(edge);
//     for (auto&& it : folly::enumerate(v2)) {
//         LOG(INFO) << "v2[" << it.index << "]=" << *it;
//     }
// }

/*
 * commit an edge with all kinds of values
 * in edge committed succeeded then failed
 * assert get prop will restore this transaction
 * */
TEST_F(TossTest, commitMultiPropInEdgeSucceededThenFailed) {
    // /*
    //  * Step 1 of 2: add inEdge succeed, outEdge failed
    //  * */
    // auto* env = TossEnvironment::getInstance(gMetaName, gMetaPort);

    // std::vector<meta::cpp2::PropertyType> types {
    //     meta::cpp2::PropertyType::BOOL,
    //     meta::cpp2::PropertyType::INT64,
    //     meta::cpp2::PropertyType::FLOAT,
    //     meta::cpp2::PropertyType::STRING,
    //     meta::cpp2::PropertyType::DOUBLE,
    // };

    // size_t nType = types.size();
    // std::vector<std::string> colNames;
    // for (auto i = 0U; i < nType; ++i) {
    //     colNames.emplace_back(folly::sformat("c{}", i+1));
    // }

    // // nebula::Value defaultVal;
    // // defaultVal.setNull(NullType::__NULL__);
    // std::vector<meta::cpp2::ColumnDef> colDefs(nType);
    // for (auto i = 0U; i != nType; ++i) {
    //     colDefs[i].set_name(colNames[i]);
    //     colDefs[i].set_default_value(Value::kNullValue);
    //     colDefs[i].set_type(types[i]);
    //     colDefs[i].set_nullable(true);
    // }
    // env->init(gSpaceName, gPart, gReplica, &colDefs);

    // // int rank = 77771370;
    // int failedPhase = static_cast<int>(TossPhase::COMMIT_EDGE2_RESP);
    // int errorCode = static_cast<int>(cpp2::ErrorCode::E_TXN_ERR_UNKNOWN);
    // int rank = gBombPrefix * 10000 + failedPhase * 100 + std::abs(errorCode);
    // LOG(INFO) << "messi rank=" << rank;

    // std::vector<nebula::Value> values(nType);
    // values[0].setBool(false);
    // values[1].setInt(65536);
    // values[2].setFloat(3.14f);
    // values[3].setStr("feng~timo~");
    // values[4].setNull(NullType::__NULL__);
    // cpp2::NewEdge edge = env->generateEdge(gSrcVid, rank, values);

    // auto f = env->addTossEdgeAsync(colNames, edge);
    // f.wait();
    // ASSERT_TRUE(f.valid());
    // ASSERT_FALSE(f.value().succeeded());

    // /*
    //  * Step 2 of 2: out edge shoule be get without error
    //  * */
    // auto v2 = env->getProps(edge);
    // for (auto&& it : folly::enumerate(v2)) {
    //     LOG(INFO) << "v2[" << it.index << "]=" << *it;
    // }

    // ASSERT_EQ(v2.size(), 4+nType);

    // ASSERT_EQ(edge.key.src, v2[0]);
    // ASSERT_EQ(edge.key.edge_type, v2[1]);
    // ASSERT_EQ(edge.key.ranking, v2[2]);
    // ASSERT_EQ(edge.key.dst, v2[3]);

    // for (auto i = 0U; i < nType; ++i) {
    //     ASSERT_EQ(edge.props[i], v2[i+4]);
    // }
}

/*
 * commit an toss edge without any error
 * check getNeighbors can succeeded
 * */
TEST_F(TossTest, getNeighborsTestHappyPath) {
    // auto* env = TossEnvironment::getInstance(gMetaName, gMetaPort);

    // std::vector<meta::cpp2::PropertyType> types {
    //     meta::cpp2::PropertyType::BOOL,
    //     meta::cpp2::PropertyType::INT64,
    //     meta::cpp2::PropertyType::FLOAT,
    //     meta::cpp2::PropertyType::STRING,
    //     meta::cpp2::PropertyType::DOUBLE,
    // };

    // size_t nType = types.size();
    // std::vector<std::string> colNames;
    // for (auto i = 0U; i < nType; ++i) {
    //     colNames.emplace_back(folly::sformat("c{}", i+1));
    // }

    // std::vector<meta::cpp2::ColumnDef> colDefs(nType);
    // for (auto i = 0U; i != nType; ++i) {
    //     colDefs[i].set_name(colNames[i]);
    //     colDefs[i].set_default_value(Value::kNullValue);
    //     colDefs[i].set_type(types[i]);
    //     colDefs[i].set_nullable(true);
    // }
    // env->init(gSpaceName, gPart, gReplica, &colDefs);

    // int iPrefix = 0;  // gBombPrefix
    // int failedPhase = static_cast<int>(TossPhase::COMMIT_EDGE2_RESP);
    // int errorCode = static_cast<int>(cpp2::ErrorCode::E_TXN_ERR_UNKNOWN);
    // int rank = iPrefix * 10000 + failedPhase * 100 + std::abs(errorCode);
    // LOG(INFO) << "messi rank=" << rank;

    // std::vector<nebula::Value> values(nType);
    // values[0].setBool(false);
    // values[1].setInt(65536);
    // values[2].setFloat(3.14f);
    // values[3].setStr("feng~timo~");
    // values[4].setNull(NullType::__NULL__);
    // // cpp2::NewEdge edge = env->generateEdge(gSrcVid, rank, values);

    // std::vector<cpp2::NewEdge> edges;
    // auto idst = gSum - gSrcVid;
    // for (auto i = 0U; i < 20; ++i) {
    //     edges.emplace_back(env->generateEdge(gSrcVid, rank, values, idst+i));
    //     auto f = env->addTossEdgeAsync(colNames, edges.back());
    //     f.wait();
    //     ASSERT_TRUE(f.valid());
    //     ASSERT_TRUE(f.value().succeeded());
    // }

    // auto* client = env->getStorageClient();
    // GraphSpaceID space = env->spaceId_;     // para1

    // std::vector<nebula::VertexID> vids;
    // for (auto& e : edges) {
    //     vids.emplace_back(e.key.src);
    //     // vids.emplace_back(e.key.dst);
    // }

    // auto last = std::unique(vids.begin(), vids.end());
    // vids.erase(last, vids.end());

    // // para3
    // std::vector<Row> vertices;
    // for (auto& vid : vids) {
    //     vertices.emplace_back();
    //     vertices.back().values.emplace_back(vid);
    // }

    // // para 4
    // std::vector<EdgeType> edgeTypes;
    // // para 5
    // cpp2::EdgeDirection edgeDirection = cpp2::EdgeDirection::BOTH;

    // // para 6
    // std::vector<cpp2::StatProp>* statProps = nullptr;
    // // para 7
    // std::vector<cpp2::VertexProp>* vertexProps = nullptr;
    // // para 8
    // const std::vector<cpp2::EdgeProp> edgeProps;
    // // para 9
    // const std::vector<cpp2::Expr>* expressions = nullptr;

    // LOG(INFO) << "messi getNeighbors paras colNames";
    // for (auto&& it : folly::enumerate(colNames)) {
    //     LOG(INFO) << "colNames[" << it.index << "]=" << *it;
    // }

    // LOG(INFO) << "messi before call getNeighbors";
    // auto sf = client->getNeighbors(space,
    //                                colNames,
    //                                vertices,
    //                                edgeTypes,
    //                                edgeDirection,
    //                                statProps,
    //                                vertexProps,
    //                                &edgeProps,
    //                                expressions);
    // sf.wait();
    // LOG(INFO) << "messi after call getNeighbors";

    // ASSERT_TRUE(sf.valid());
    // StorageRpcResponse<cpp2::GetNeighborsResponse> rpc = sf.value();
    // ASSERT_TRUE(rpc.succeeded());

    // std::vector<cpp2::GetNeighborsResponse> resps = rpc.responses();
    // LOG(INFO) << "cpp2::GetNeighborsResponse.size() = " << resps.size();
    // for (cpp2::GetNeighborsResponse& resp : resps) {
    //     nebula::DataSet ds = resp.vertices;
    //     env->printDataSet(ds);
    // }

    // ASSERT_EQ(resps.size(), 2);
}

/*
 * commit an toss edge
 * assume inedge commit failed
 * check getNeighbors will not get this edge
 * */
TEST_F(TossTest, getNeighborsFailOverTest1) {
    /*
     * Step 1 of 2: add en edge
     * */
    auto* env = TossEnvironment::getInstance(gMetaName, gMetaPort);

    std::vector<meta::cpp2::PropertyType> types {
        meta::cpp2::PropertyType::BOOL,
        meta::cpp2::PropertyType::INT64,
        meta::cpp2::PropertyType::FLOAT,
        meta::cpp2::PropertyType::STRING,
        meta::cpp2::PropertyType::DOUBLE,
    };

    size_t nType = types.size();
    std::vector<std::string> colNames;
    for (auto i = 0U; i < nType; ++i) {
        colNames.emplace_back(folly::sformat("c{}", i+1));
    }

    std::vector<meta::cpp2::ColumnDef> colDefs(nType);
    for (auto i = 0U; i != nType; ++i) {
        colDefs[i].set_name(colNames[i]);
        colDefs[i].set_default_value(Value::kNullValue);
        colDefs[i].set_type(types[i]);
        colDefs[i].set_nullable(true);
    }
    env->init(gSpaceName, gPart, gReplica, &colDefs);

    int iPrefix = gBombPrefix;  // gBombPrefix
    int failedPhase = static_cast<int>(TossPhase::COMMIT_EDGE2_RESP);
    int errorCode = static_cast<int>(cpp2::ErrorCode::E_TXN_ERR_UNKNOWN);
    int rank = iPrefix * 10000 + failedPhase * 100 + std::abs(errorCode);
    LOG(INFO) << "messi rank=" << rank;

    std::vector<nebula::Value> values(nType);
    values[0].setBool(false);
    values[1].setInt(65536);
    values[2].setFloat(3.14f);
    values[3].setStr("feng~timo~");
    values[4].setNull(NullType::__NULL__);
    // cpp2::NewEdge edge = env->generateEdge(gSrcVid, rank, values);

    std::vector<cpp2::NewEdge> edges;
    auto idst = gSum - gSrcVid;
    for (auto i = 0U; i < 20; ++i) {
        edges.emplace_back(env->generateEdge(gSrcVid, rank, values, idst+i));
        auto f = env->addTossEdgeAsync(colNames, edges.back());
        f.wait();
        ASSERT_TRUE(f.valid());
        ASSERT_FALSE(f.value().succeeded());
    }

    // para1
    auto* client = env->getStorageClient();
    GraphSpaceID space = env->spaceId_;

    // para3
    std::vector<nebula::VertexID> vids;
    for (auto& e : edges) {
        vids.emplace_back(e.key.src);
    }
    auto last = std::unique(vids.begin(), vids.end());
    vids.erase(last, vids.end());
    std::vector<Row> vertices;
    for (auto& vid : vids) {
        vertices.emplace_back();
        vertices.back().values.emplace_back(vid);
    }

    std::vector<EdgeType> edgeTypes;  // para 4
    cpp2::EdgeDirection edgeDirection = cpp2::EdgeDirection::BOTH;  // para 5
    std::vector<cpp2::StatProp>* statProps = nullptr;  // para 6
    std::vector<cpp2::VertexProp>* vertexProps = nullptr;  // para 7
    const std::vector<cpp2::EdgeProp> edgeProps;  // para 8
    const std::vector<cpp2::Expr>* expressions = nullptr;  // para 9

    LOG(INFO) << "messi getNeighbors paras colNames";
    for (auto&& it : folly::enumerate(colNames)) {
        LOG(INFO) << "colNames[" << it.index << "]=" << *it;
    }

    LOG(INFO) << "messi before call getNeighbors";
    auto sf = client->getNeighbors(space,
                                   colNames,
                                   vertices,
                                   edgeTypes,
                                   edgeDirection,
                                   statProps,
                                   vertexProps,
                                   &edgeProps,
                                   expressions);
    sf.wait();
    LOG(INFO) << "messi after call getNeighbors";

    ASSERT_TRUE(sf.valid());
    StorageRpcResponse<cpp2::GetNeighborsResponse> rpc = sf.value();
    ASSERT_TRUE(rpc.succeeded());

    std::vector<cpp2::GetNeighborsResponse> resps = rpc.responses();
    LOG(INFO) << "cpp2::GetNeighborsResponse.size() = " << resps.size();
    for (cpp2::GetNeighborsResponse& resp : resps) {
        nebula::DataSet ds = resp.vertices;
        env->printDataSet(ds);
    }

    ASSERT_EQ(resps.size(), 1);
}

/*
 * commit 2 toss edges
 * assume inedge commit failed
 * check getNeighbors will not get this edge
 * */
TEST_F(TossTest, getNeighborsFailOverTest2) {
}

/*
 * commit 1 toss edge
 * assume inedge commit succeeded
 * check getNeighbors will get this edge
 * */
TEST_F(TossTest, getNeighborsFailOverTest3) {
}

/*
 * commit 2 toss edges
 * assume inedge commit succeeded
 * check getNeighbors will get this edge
 * */
TEST_F(TossTest, getNeighborsFailOverTest4) {
}

/*
 * commit 100 edges by toss,
 * let 5 of them failed after in-edge commit succeeded
 * check getNeighbors will get all this edge
 * */
TEST_F(TossTest, getNeighborsFailOverTest5) {
}

/*
 * commit 100 edges by toss,
 * let 5 of them failed after in-edge commit succeeded
 * and another 5 of them failed before in-edge commit
 * check getNeighbors will get 95 of these edges
 * */
TEST_F(TossTest, getNeighborsFailOverTest6) {
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
    FLAGS_heartbeat_interval_secs = 3;

    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, false);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
