/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "utils/NebulaKeyUtils.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "storage/mutate/UpdateVertexProcessor.h"
#include "storage/mutate/UpdateEdgeProcessor.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"
#include "common/expression/ConstantExpression.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include <folly/Benchmark.h>


namespace nebula {
namespace storage {

GraphSpaceID spaceId = 1;
VertexID vertexId = "";
TagID tagId = 1;
PartitionID partId = 0;
int32_t spaceVidLen = 0;
VertexID srcId = "";
VertexID dstId = "";
EdgeRanking rank = 0;
EdgeType edgeType = 0;
storage::cpp2::EdgeKey edgeKey;
int parts = 0;
storage::StorageEnv* env;

bool encode(const meta::NebulaSchemaProvider* schema,
            const std::string& key,
            const std::vector<Value>& props,
            std::vector<kvstore::KV>& data) {
    RowWriterV2 writer(schema);
    for (size_t i = 0; i < props.size(); i++) {
        auto r = writer.setValue(i, props[i]);
        if (r != WriteResult::SUCCEEDED) {
            LOG(ERROR) << "Invalid prop " << i;
            return false;
        }
    }
    auto ret = writer.finish();
    if (ret != WriteResult::SUCCEEDED) {
        LOG(ERROR) << "Failed to write data";
        return false;
    }
    auto encode = std::move(writer).moveEncodedStr();
    data.emplace_back(std::move(key), std::move(encode));
    return true;
}

bool mockVertexData(storage::StorageEnv* ev, int32_t totalParts, int32_t vidLen) {
    auto verticesPart = mock::MockData::mockVerticesofPart(totalParts);

    folly::Baton<true, std::atomic> baton;
    std::atomic<size_t> count(verticesPart.size());

    for (const auto& part : verticesPart) {
        std::vector<kvstore::KV> data;
        data.clear();
        for (const auto& vertex : part.second) {
            TagID tId = vertex.tId_;
            // Switch version to big-endian, make sure the key is in ordered.
            auto version = std::numeric_limits<int64_t>::max() - 0L;
            version = folly::Endian::big(version);
            auto key = NebulaKeyUtils::vertexKey(vidLen,
                                                 part.first,
                                                 vertex.vId_,
                                                 tId,
                                                 version);
            auto schema = ev->schemaMan_->getTagSchema(spaceId, tId);
            if (!schema) {
                LOG(ERROR) << "Invalid tagId " << tId;
                return false;
            }

            auto ret = encode(schema.get(), key, vertex.props_, data);
            if (!ret) {
                LOG(ERROR) << "Write field failed";
                return false;
            }
        }

        ev->kvstore_->asyncMultiPut(spaceId, part.first, std::move(data),
                                    [&](kvstore::ResultCode code) {
                                        CHECK_EQ(code, kvstore::ResultCode::SUCCEEDED);
                                        count.fetch_sub(1);
                                        if (count.load() == 0) {
                                            baton.post();
                                        }
                                   });
    }
    baton.wait();
    return true;
}

bool mockEdgeData(storage::StorageEnv* ev, int32_t totalParts, int32_t vidLen) {
    auto edgesPart = mock::MockData::mockEdgesofPart(totalParts);

    folly::Baton<true, std::atomic> baton;
    std::atomic<size_t> count(edgesPart.size());

    for (const auto& part : edgesPart) {
        std::vector<kvstore::KV> data;
        data.clear();

        for (const auto& edge : part.second) {
            // Switch version to big-endian, make sure the key is in ordered.
            auto version = std::numeric_limits<int64_t>::max() - 0L;
            version = folly::Endian::big(version);
            auto key = NebulaKeyUtils::edgeKey(vidLen,
                                               part.first,
                                               edge.srcId_,
                                               edge.type_,
                                               edge.rank_,
                                               edge.dstId_,
                                               version);
            auto schema = ev->schemaMan_->getEdgeSchema(spaceId, std::abs(edge.type_));
            if (!schema) {
                LOG(ERROR) << "Invalid edge " << edge.type_;
                return false;
            }

            auto ret = encode(schema.get(), key, edge.props_, data);
            if (!ret) {
                LOG(ERROR) << "Write field failed";
                return false;
            }
        }
        ev->kvstore_->asyncMultiPut(spaceId, part.first, std::move(data),
                                    [&](kvstore::ResultCode code) {
                                        CHECK_EQ(code, kvstore::ResultCode::SUCCEEDED);
                                        count.fetch_sub(1);
                                        if (count.load() == 0) {
                                            baton.post();
                                        }
                                   });
    }
    baton.wait();
    return true;
}

void setUp(storage::StorageEnv* ev) {
    auto status = ev->schemaMan_->getSpaceVidLen(spaceId);
    if (!status.ok()) {
        LOG(ERROR) << "Get space vidLen failed";
        return;
    }

    spaceVidLen = status.value();

    if (!mockVertexData(ev, parts, spaceVidLen)) {
        LOG(ERROR) << "Mock data faild";
        return;
    }
    if (!mockEdgeData(ev, parts, spaceVidLen)) {
        LOG(ERROR) << "Mock data faild";
        return;
    }
}


cpp2::UpdateVertexRequest buildUpdateVertexReq(bool insertable) {
    cpp2::UpdateVertexRequest req;
    req.set_space_id(spaceId);
    req.set_tag_id(tagId);

    if (!insertable) {
        partId = std::hash<std::string>()("Tim Duncan") % parts + 1;
        vertexId = "Tim Duncan";
        req.set_part_id(partId);
        req.set_vertex_id(vertexId);

        // Build updated props
        std::vector<cpp2::UpdatedProp> updatedProps;
        // int: player.age = 45
        cpp2::UpdatedProp uProp1;
        uProp1.set_name("age");
        ConstantExpression val1(45L);
        uProp1.set_value(Expression::encode(val1));
        updatedProps.emplace_back(uProp1);

        // string: player.country= China
        cpp2::UpdatedProp uProp2;
        uProp2.set_name("country");
        std::string col4new("China");
        ConstantExpression val2(col4new);
        uProp2.set_value(Expression::encode(val2));
        updatedProps.emplace_back(uProp2);
        req.set_updated_props(std::move(updatedProps));

        // Build yield
        // Return player props: name, age, country
        decltype(req.return_props) tmpProps;
        auto* yTag1 = new std::string("1");
        auto* yProp1 = new std::string("name");
        SourcePropertyExpression sourcePropExp1(yTag1, yProp1);
        tmpProps.emplace_back(Expression::encode(sourcePropExp1));

        auto* yTag2 = new std::string("1");
        auto* yProp2 = new std::string("age");
        SourcePropertyExpression sourcePropExp2(yTag2, yProp2);
        tmpProps.emplace_back(Expression::encode(sourcePropExp2));

        auto* yTag3 = new std::string("1");
        auto* yProp3 = new std::string("country");
        SourcePropertyExpression sourcePropExp3(yTag3, yProp3);
        tmpProps.emplace_back(Expression::encode(sourcePropExp3));

        req.set_return_props(std::move(tmpProps));
        req.set_insertable(false);
    } else {
        partId = std::hash<std::string>()("Brandon Ingram") % parts + 1;
        vertexId = "Brandon Ingram";
        req.set_part_id(partId);
        req.set_vertex_id(vertexId);

        // Build updated props
        std::vector<cpp2::UpdatedProp> updatedProps;
        // string: player.name= "Brandon Ingram"
        cpp2::UpdatedProp uProp1;
        uProp1.set_name("name");
        std::string colnew("Brandon Ingram");
        ConstantExpression val1(colnew);
        uProp1.set_value(Expression::encode(val1));
        updatedProps.emplace_back(uProp1);

        cpp2::UpdatedProp uProp2;
        uProp2.set_name("age");
        ConstantExpression val2(45L);
        uProp2.set_value(Expression::encode(val2));
        updatedProps.emplace_back(uProp2);
        req.set_updated_props(std::move(updatedProps));

        // Build yield
        // Return player props: name, age, country
        decltype(req.return_props) tmpProps;
        auto* yTag1 = new std::string("1");
        auto* yProp1 = new std::string("name");
        SourcePropertyExpression sourcePropExp1(yTag1, yProp1);
        tmpProps.emplace_back(Expression::encode(sourcePropExp1));

        auto* yTag2 = new std::string("1");
        auto* yProp2 = new std::string("age");
        SourcePropertyExpression sourcePropExp2(yTag2, yProp2);
        tmpProps.emplace_back(Expression::encode(sourcePropExp2));

        auto* yTag3 = new std::string("1");
        auto* yProp3 = new std::string("country");
        SourcePropertyExpression sourcePropExp3(yTag3, yProp3);
        tmpProps.emplace_back(Expression::encode(sourcePropExp3));

        req.set_return_props(std::move(tmpProps));
        req.set_insertable(true);
    }
    return req;
}


cpp2::UpdateEdgeRequest buildUpdateEdgeReq(bool insertable) {
    cpp2::UpdateEdgeRequest req;
    req.set_space_id(spaceId);


    if (!insertable) {
        partId = std::hash<std::string>()("Tim Duncan") % parts + 1;
        req.set_part_id(partId);

        srcId = "Tim Duncan";
        dstId = "Spurs";
        rank = 1997;
        edgeType = 101;
        edgeKey.set_src(srcId);
        edgeKey.set_edge_type(edgeType);
        edgeKey.set_ranking(rank);
        edgeKey.set_dst(dstId);
        req.set_edge_key(edgeKey);

        // Build updated props
        std::vector<cpp2::UpdatedProp> updatedProps;
        // int: 101.teamCareer = 20
        cpp2::UpdatedProp uProp1;
        uProp1.set_name("teamCareer");
        ConstantExpression val1(20L);
        uProp1.set_value(Expression::encode(val1));
        updatedProps.emplace_back(uProp1);

        // bool: 101.type = trade
        cpp2::UpdatedProp uProp2;
        uProp2.set_name("type");
        std::string colnew("trade");
        ConstantExpression val2(colnew);
        uProp2.set_value(Expression::encode(val2));
        updatedProps.emplace_back(uProp2);
        req.set_updated_props(std::move(updatedProps));

        // Return serve props: playerName, teamName, teamCareer, type
        decltype(req.return_props) tmpProps;
        auto* yEdge1 = new std::string("101");
        auto* yProp1 = new std::string("playerName");
        EdgePropertyExpression edgePropExp1(yEdge1, yProp1);
        tmpProps.emplace_back(Expression::encode(edgePropExp1));

        auto* yEdge2 = new std::string("101");
        auto* yProp2 = new std::string("teamName");
        EdgePropertyExpression edgePropExp2(yEdge2, yProp2);
        tmpProps.emplace_back(Expression::encode(edgePropExp2));

        auto* yEdge3 = new std::string("101");
        auto* yProp3 = new std::string("teamCareer");
        EdgePropertyExpression edgePropExp3(yEdge3, yProp3);
        tmpProps.emplace_back(Expression::encode(edgePropExp3));

        auto* yEdge4 = new std::string("101");
        auto* yProp4 = new std::string("type");
        EdgePropertyExpression edgePropExp4(yEdge4, yProp4);
        tmpProps.emplace_back(Expression::encode(edgePropExp4));

        req.set_return_props(std::move(tmpProps));
        req.set_insertable(false);
    } else {
        partId = std::hash<std::string>()("Brandon Ingram") % parts + 1;
        req.set_part_id(partId);

        srcId = "Brandon Ingram";
        dstId = "Lakers";
        rank = 2016;
        edgeType = 101;
        edgeKey.set_src(srcId);
        edgeKey.set_edge_type(edgeType);
        edgeKey.set_ranking(rank);
        edgeKey.set_dst(dstId);
        req.set_edge_key(edgeKey);

        // Build updated props
        std::vector<cpp2::UpdatedProp> updatedProps;
        // string: 101.playerName = Brandon Ingram
        cpp2::UpdatedProp uProp1;
        uProp1.set_name("playerName");
        std::string col1new("Brandon Ingram");
        ConstantExpression val1(col1new);
        uProp1.set_value(Expression::encode(val1));
        updatedProps.emplace_back(uProp1);

        // string: 101.teamName = Lakers
        cpp2::UpdatedProp uProp2;
        uProp2.set_name("teamName");
        std::string col2new("Lakers");
        ConstantExpression val2(col2new);
        uProp2.set_value(Expression::encode(val2));
        updatedProps.emplace_back(uProp2);
        req.set_updated_props(std::move(updatedProps));

        // Build yield
        // Return serve props: playerName, teamName, teamCareer, type
        decltype(req.return_props) tmpProps;
        auto* yEdge1 = new std::string("101");
        auto* yProp1 = new std::string("playerName");
        EdgePropertyExpression edgePropExp1(yEdge1, yProp1);
        tmpProps.emplace_back(Expression::encode(edgePropExp1));

        auto* yEdge2 = new std::string("101");
        auto* yProp2 = new std::string("teamName");
        EdgePropertyExpression edgePropExp2(yEdge2, yProp2);
        tmpProps.emplace_back(Expression::encode(edgePropExp2));

        auto* yEdge3 = new std::string("101");
        auto* yProp3 = new std::string("teamCareer");
        EdgePropertyExpression edgePropExp3(yEdge3, yProp3);
        tmpProps.emplace_back(Expression::encode(edgePropExp3));

        req.set_return_props(std::move(tmpProps));
        req.set_insertable(true);
    }
    return req;
}

}  // namespace storage
}  // namespace nebula

void updateVertex(int32_t iters, bool insertable) {
    nebula::storage::cpp2::UpdateVertexRequest req;
    BENCHMARK_SUSPEND {
        req = nebula::storage::buildUpdateVertexReq(insertable);
    }

    for (decltype(iters) i = 0; i < iters; i++) {
        // Test UpdateVertexRequest
        auto* processor
            = nebula::storage::UpdateVertexProcessor::instance(nebula::storage::env, nullptr);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        if (!resp.result.failed_parts.empty()) {
            LOG(ERROR) << "update faild";
            return;
        }
    }
}

void updateEdge(int32_t iters, bool insertable) {
    nebula::storage::cpp2::UpdateEdgeRequest req;
    BENCHMARK_SUSPEND {
        req = nebula::storage::buildUpdateEdgeReq(insertable);
    }

    for (decltype(iters) i = 0; i < iters; i++) {
        // Test UpdateEdgeRequest
        auto* processor
            =  nebula::storage::UpdateEdgeProcessor::instance(nebula::storage::env, nullptr);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();

        if (!resp.result.failed_parts.empty()) {
            LOG(ERROR) << "update faild";
            return;
        }
    }
}

BENCHMARK(update_vertex, iters) {
    updateVertex(iters, false);
}

BENCHMARK(upsert_vertex, iters) {
    updateVertex(iters, true);
}

BENCHMARK(update_edge, iters) {
    updateEdge(iters, false);
}

BENCHMARK(upsert_edge, iters) {
    updateEdge(iters, true);
}

int main(int argc, char** argv) {
    folly::init(&argc, &argv, true);
    nebula::fs::TempDir rootPath("/tmp/UpdateVertexTest.XXXXXX");
    nebula::mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    nebula::storage::env = cluster.storageEnv_.get();
    nebula::storage::parts = cluster.getTotalParts();
    nebula::storage::setUp(nebula::storage::env);
    folly::runBenchmarks();
    return 0;
}


/**

CPU : Intel(R) Xeon(R) CPU E5-2697 v3 @ 2.60GHz

update_vertex   : tag data exist and update

upsert_vertex   : tag data (first not exist) and upsert

update_edge     : edge data exist and update

upsert_edge     : edge data (first not exist)  and upsert

V1.0
==============================================================================
src/storage/test/UpdateVertexAndEdgeBenchmark.cpprelative  time/iter  iters/s
==============================================================================
update_vertex                                               322.47us    3.10K
upsert_vertex                                               324.23us    3.08K
update_edge                                                 387.79us    2.58K
upsert_edge                                                 369.61us    2.71K
==============================================================================

V2.0
==============================================================================
src/storage/test/UpdateVertexAndEdgeBenchmark.cpprelative  time/iter  iters/s
==============================================================================
update_vertex                                               253.68us    3.94K
upsert_vertex                                               250.62us    3.99K
update_edge                                                 292.83us    3.41K
upsert_edge                                                 285.98us    3.50K
==============================================================================
**/
