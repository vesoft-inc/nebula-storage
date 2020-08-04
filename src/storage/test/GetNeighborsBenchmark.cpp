/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include <folly/Benchmark.h>
#include "common/fs/TempDir.h"
#include "storage/query/GetNeighborsProcessor.h"
#include "storage/test/QueryTestUtils.h"
#include "storage/exec/EdgeNode.h"

DEFINE_uint64(max_rank, 1000, "max rank of each edge");
DEFINE_double(filter_ratio, 0.5, "ratio of data would pass filter");
DEFINE_bool(go_record, false, "");
DEFINE_bool(kv_record, false, "");

std::unique_ptr<nebula::mock::MockCluster> gCluster;

namespace nebula {
namespace storage {

cpp2::GetNeighborsRequest buildRequest(const std::vector<VertexID>& vertex,
                                       const std::vector<std::string>& playerProps,
                                       const std::vector<std::string>& serveProps) {
    TagID player = 1;
    EdgeType serve = 101;
    auto totalParts = gCluster->getTotalParts();
    std::vector<VertexID> vertices = vertex;
    std::vector<EdgeType> over = {serve};
    std::vector<std::pair<TagID, std::vector<std::string>>> tags;
    std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
    tags.emplace_back(player, playerProps);
    edges.emplace_back(serve, serveProps);
    auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
    return req;
}

void setUp(const char* path, EdgeRanking maxRank) {
    gCluster = std::make_unique<nebula::mock::MockCluster>();
    gCluster->initStorageKV(path);
    auto* env = gCluster->storageEnv_.get();
    auto totalParts = gCluster->getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockBenchEdgeData(env, totalParts, 1, maxRank));
}

}  // namespace storage
}  // namespace nebula

void initContext(std::unique_ptr<nebula::storage::PlanContext>& planCtx,
                 nebula::storage::EdgeContext& edgeContext,
                 const std::vector<std::string>& serveProps) {
    nebula::GraphSpaceID spaceId = 1;
    auto* env = gCluster->storageEnv_.get();
    auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId).value();
    planCtx = std::make_unique<nebula::storage::PlanContext>(env, spaceId, vIdLen);

    nebula::EdgeType serve = 101;
    edgeContext.schemas_ = std::move(env->schemaMan_->getAllVerEdgeSchema(spaceId)).value();

    auto edgeName = env->schemaMan_->toEdgeName(spaceId, std::abs(serve));
    edgeContext.edgeNames_.emplace(serve, std::move(edgeName).value());
    auto iter = edgeContext.schemas_.find(std::abs(serve));
    const auto& edgeSchema = iter->second.back();

    std::vector<nebula::storage::PropContext> ctxs;
    for (const auto& prop : serveProps) {
        auto field = edgeSchema->field(prop);
        nebula::storage::PropContext ctx(prop.c_str());
        ctx.returned_ = true;
        ctx.field_ = field;
        ctxs.emplace_back(std::move(ctx));
    }
    edgeContext.propContexts_.emplace_back(serve, std::move(ctxs));
    edgeContext.indexMap_.emplace(serve, edgeContext.propContexts_.size() - 1);

    const auto& ec = edgeContext.propContexts_.front();
    planCtx->props_ = &ec.second;
}

void go(int32_t iters,
        const std::vector<nebula::VertexID>& vertex,
        const std::vector<std::string>& playerProps,
        const std::vector<std::string>& serveProps) {
    nebula::storage::cpp2::GetNeighborsRequest req;
    BENCHMARK_SUSPEND {
        req = nebula::storage::buildRequest(vertex, playerProps, serveProps);
    }
    auto* env = gCluster->storageEnv_.get();
    for (decltype(iters) i = 0; i < iters; i++) {
        auto* processor = nebula::storage::GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        folly::doNotOptimizeAway(resp);
    }
}

void goFilter(int32_t iters,
              const std::vector<nebula::VertexID>& vertex,
              const std::vector<std::string>& playerProps,
              const std::vector<std::string>& serveProps,
              int64_t value = FLAGS_max_rank * FLAGS_filter_ratio) {
    nebula::storage::cpp2::GetNeighborsRequest req;
    BENCHMARK_SUSPEND {
        nebula::EdgeType serve = 101;
        req = nebula::storage::buildRequest(vertex, playerProps, serveProps);
        {
            // where serve.startYear < value
            nebula::RelationalExpression exp(
                nebula::Expression::Kind::kRelLT,
                new nebula::EdgePropertyExpression(
                    new std::string(folly::to<std::string>(serve)),
                    new std::string("startYear")),
                new nebula::ConstantExpression(nebula::Value(value)));
            req.traverse_spec.set_filter(nebula::Expression::encode(exp));
        }
    }
    auto* env = gCluster->storageEnv_.get();
    for (decltype(iters) i = 0; i < iters; i++) {
        auto* processor = nebula::storage::GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        folly::doNotOptimizeAway(resp);
    }
}

void goEdgeNode(int32_t iters,
                const std::vector<nebula::VertexID>& vertex,
                const std::vector<std::string>& playerProps,
                const std::vector<std::string>& serveProps) {
    UNUSED(playerProps);
    std::unique_ptr<nebula::storage::PlanContext> planCtx;
    std::unique_ptr<nebula::storage::SingleEdgeNode> edgeNode;
    nebula::storage::EdgeContext edgeContext;
    BENCHMARK_SUSPEND {
        initContext(planCtx, edgeContext, serveProps);
        const auto& ec = edgeContext.propContexts_.front();
        edgeNode = std::make_unique<nebula::storage::SingleEdgeNode>(
            planCtx.get(), &edgeContext, ec.first, &ec.second);
    }
    auto totalParts = gCluster->getTotalParts();
    for (decltype(iters) i = 0; i < iters; i++) {
        nebula::DataSet resultDataSet;
        std::hash<std::string> hash;
        for (const auto& vId : vertex) {
            nebula::PartitionID partId = (hash(vId) % totalParts) + 1;
            std::vector<nebula::Value> row;
            row.emplace_back(vId);
            row.emplace_back(nebula::List());
            {
                edgeNode->execute(partId, vId);
                int32_t count = 0;
                auto& cell = row[1].mutableList();
                for (; edgeNode->valid(); edgeNode->next()) {
                    nebula::List list;
                    auto key = edgeNode->key();
                    folly::doNotOptimizeAway(key);
                    auto reader = edgeNode->reader();
                    auto props = planCtx->props_;
                    for (const auto& prop : *props) {
                        auto value = nebula::storage::QueryUtils::readValue(
                            reader, prop.name_, prop.field_);
                        CHECK(value.ok());
                        list.emplace_back(std::move(value).value());
                    }
                    cell.values.emplace_back(std::move(list));
                    count++;
                }
                CHECK_EQ(FLAGS_max_rank, count);
            }
            resultDataSet.rows.emplace_back(std::move(row));
        }
        CHECK_EQ(vertex.size(), resultDataSet.rowSize());
    }
}

void prefix(int32_t iters,
            const std::vector<nebula::VertexID>& vertex,
            const std::vector<std::string>& playerProps,
            const std::vector<std::string>& serveProps) {
    std::unique_ptr<nebula::storage::PlanContext> planCtx;
    nebula::storage::EdgeContext edgeContext;
    BENCHMARK_SUSPEND {
        initContext(planCtx, edgeContext, serveProps);
    }
    for (decltype(iters) i = 0; i < iters; i++) {
        nebula::GraphSpaceID spaceId = 1;
        nebula::TagID player = 1;
        nebula::EdgeType serve = 101;

        std::hash<std::string> hash;
        auto* env = gCluster->storageEnv_.get();
        auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId).value();
        auto totalParts = gCluster->getTotalParts();

        auto tagSchemas = env->schemaMan_->getAllVerTagSchema(spaceId).value();
        auto tagSchemaIter = tagSchemas.find(player);
        CHECK(tagSchemaIter != tagSchemas.end());
        CHECK(!tagSchemaIter->second.empty());
        auto* tagSchema = &(tagSchemaIter->second);

        auto edgeSchemas = env->schemaMan_->getAllVerEdgeSchema(spaceId).value();
        auto edgeSchemaIter = edgeSchemas.find(std::abs(serve));
        CHECK(edgeSchemaIter != edgeSchemas.end());
        CHECK(!edgeSchemaIter->second.empty());
        auto* edgeSchema = &(edgeSchemaIter->second);

        nebula::DataSet resultDataSet;
        nebula::RowReaderWrapper reader;

        for (const auto& vId : vertex) {
            nebula::PartitionID partId = (hash(vId) % totalParts) + 1;
            std::vector<nebula::Value> row;
            row.emplace_back(vId);
            row.emplace_back(nebula::List());
            row.emplace_back(nebula::List());
            {
                // read tags
                std::unique_ptr<nebula::kvstore::KVIterator> iter;
                auto prefix = nebula::NebulaKeyUtils::vertexPrefix(vIdLen, partId, vId, player);
                auto code = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
                CHECK_EQ(code, nebula::kvstore::ResultCode::SUCCEEDED);
                CHECK(iter->valid());
                auto val = iter->val();
                reader.reset(*tagSchema, val);
                CHECK_NOTNULL(reader);
                auto& cell = row[1].mutableList();
                for (const auto& prop : playerProps) {
                    cell.emplace_back(reader->getValueByName(prop));
                }
            }
            {
                // read edges
                std::unique_ptr<nebula::kvstore::KVIterator> iter;
                auto prefix = nebula::NebulaKeyUtils::edgePrefix(vIdLen, partId, vId, serve);
                auto code = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
                CHECK_EQ(code, nebula::kvstore::ResultCode::SUCCEEDED);
                int32_t count = 0;
                auto& cell = row[2].mutableList();
                for (; iter->valid(); iter->next()) {
                    nebula::List list;
                    auto key = iter->key();
                    folly::doNotOptimizeAway(key);
                    auto val = iter->val();
                    reader.reset(*edgeSchema, val);
                    auto props = planCtx->props_;
                    for (const auto& prop : *props) {
                        auto value = nebula::storage::QueryUtils::readValue(
                            reader.get(), prop.name_, prop.field_);
                        CHECK(value.ok());
                        list.emplace_back(std::move(value).value());
                    }
                    cell.values.emplace_back(std::move(list));
                    count++;
                }
                CHECK_EQ(FLAGS_max_rank, count);
            }
            resultDataSet.rows.emplace_back(std::move(row));
        }
        CHECK_EQ(vertex.size(), resultDataSet.rowSize());
    }
}

// Players may serve more than one team, the total edges = teamCount * maxRank, which would effect
// the final result, so select some player only serve one team
BENCHMARK(OneVertexOneProperty, iters) {
    go(iters, {"Tim Duncan"}, {"name"}, {"teamName"});
}
BENCHMARK_RELATIVE(OneVertexOnePropertyWithFilter, iters) {
    goFilter(iters, {"Tim Duncan"}, {"name"}, {"teamName"});
}
BENCHMARK_RELATIVE(OneVertexOnePropertyOnlyEdgeNode, iters) {
    goEdgeNode(iters, {"Tim Duncan"}, {"name"}, {"teamName"});
}
BENCHMARK_RELATIVE(OneVertexOneProperyOnlyKV, iters) {
    prefix(iters, {"Tim Duncan"}, {"name"}, {"teamName"});
}

BENCHMARK_DRAW_LINE();

BENCHMARK(TenVertexOneProperty, iters) {
    go(iters,
       {"Tim Duncan", "Kobe Bryant", "Stephen Curry", "Manu Ginobili", "Joel Embiid",
        "Giannis Antetokounmpo", "Yao Ming", "Damian Lillard", "Dirk Nowitzki", "Klay Thompson"},
       {"name"},
       {"teamName"});
}
BENCHMARK_RELATIVE(TenVertexOnePropertyWithFilter, iters) {
    goFilter(
        iters,
        {"Tim Duncan", "Kobe Bryant", "Stephen Curry", "Manu Ginobili", "Joel Embiid",
        "Giannis Antetokounmpo", "Yao Ming", "Damian Lillard", "Dirk Nowitzki", "Klay Thompson"},
        {"name"},
        {"teamName"});
}
BENCHMARK_RELATIVE(TenVertexOnePropertyOnlyEdgeNode, iters) {
    goEdgeNode(
        iters,
        {"Tim Duncan", "Kobe Bryant", "Stephen Curry", "Manu Ginobili", "Joel Embiid",
        "Giannis Antetokounmpo", "Yao Ming", "Damian Lillard", "Dirk Nowitzki", "Klay Thompson"},
        {"name"},
        {"teamName"});
}
BENCHMARK_RELATIVE(TenVertexOneProperyOnlyKV, iters) {
    prefix(
        iters,
        {"Tim Duncan", "Kobe Bryant", "Stephen Curry", "Manu Ginobili", "Joel Embiid",
        "Giannis Antetokounmpo", "Yao Ming", "Damian Lillard", "Dirk Nowitzki", "Klay Thompson"},
        {"name"},
        {"teamName"});
}

int main(int argc, char** argv) {
    folly::init(&argc, &argv, true);
    nebula::fs::TempDir rootPath("/tmp/GetNeighborsBenchmark.XXXXXX");
    nebula::storage::setUp(rootPath.path(), FLAGS_max_rank);
    if (FLAGS_go_record) {
        go(1000000, {"Tim Duncan"}, {"name"}, {"teamName"});
    } else if (FLAGS_kv_record) {
        prefix(1000000, {"Tim Duncan"}, {"name"}, {"teamName"});
    } else {
        folly::runBenchmarks();
    }
    gCluster.reset();
    return 0;
}


/*
40 processors, Intel(R) Xeon(R) CPU E5-2690 v2 @ 3.00GHz.
release

--max_rank=1000 --filter_ratio=0.1
============================================================================
/home/doodle.wang/Git/nebula-storage/src/storage/test/GetNeighborsBenchmark.cpprelative  time/iter  iters/s
============================================================================
OneVertexOneProperty                                       533.81us    1.87K
OneVertexOnePropertyWithFilter                   111.64%   478.15us    2.09K
OneVertexOnePropertyOnlyEdgeNode                 108.02%   494.18us    2.02K
OneVertexOneProperyOnlyKV                        109.63%   486.93us    2.05K
----------------------------------------------------------------------------
TenVertexOneProperty                                         5.33ms   187.70
TenVertexOnePropertyWithFilter                   112.74%     4.73ms   211.62
TenVertexOnePropertyOnlyEdgeNode                 105.42%     5.05ms   197.88
TenVertexOneProperyOnlyKV                        107.75%     4.94ms   202.25
============================================================================

--max_rank=1000 --filter_ratio=0.5
============================================================================
/home/doodle.wang/Git/nebula-storage/src/storage/test/GetNeighborsBenchmark.cpprelative  time/iter  iters/s
============================================================================
OneVertexOneProperty                                       529.59us    1.89K
OneVertexOnePropertyWithFilter                    81.76%   647.75us    1.54K
OneVertexOnePropertyOnlyEdgeNode                 107.54%   492.47us    2.03K
OneVertexOneProperyOnlyKV                        108.38%   488.65us    2.05K
----------------------------------------------------------------------------
TenVertexOneProperty                                         5.30ms   188.54
TenVertexOnePropertyWithFilter                    81.95%     6.47ms   154.50
TenVertexOnePropertyOnlyEdgeNode                 106.09%     5.00ms   200.02
TenVertexOneProperyOnlyKV                        108.26%     4.90ms   204.12
============================================================================

--max_rank=1000 --filter_ratio=1
============================================================================
/home/doodle.wang/Git/nebula-storage/src/storage/test/GetNeighborsBenchmark.cpprelative  time/iter  iters/s
============================================================================
OneVertexOneProperty                                       522.41us    1.91K
OneVertexOnePropertyWithFilter                    62.76%   832.45us    1.20K
OneVertexOnePropertyOnlyEdgeNode                 108.25%   482.58us    2.07K
OneVertexOneProperyOnlyKV                        107.87%   484.31us    2.06K
----------------------------------------------------------------------------
TenVertexOneProperty                                         5.30ms   188.83
TenVertexOnePropertyWithFilter                    69.70%     7.60ms   131.62
TenVertexOnePropertyOnlyEdgeNode                 119.34%     4.44ms   225.34
TenVertexOneProperyOnlyKV                        120.89%     4.38ms   228.27
============================================================================
*/
