/* Copyright (c) 2020 vesoft inc. All rights reserved.
  *
  * This source code is licensed under Apache 2.0 License,
  * attached with Common Clause Condition 1.0, found in the LICENSES directory.
  */

#include "common/base/Base.h"
#include <gtest/gtest.h>
#include "common/fs/TempDir.h"
#include "storage/index/LookupTraverseProcessor.h"
#include "storage/test/QueryTestUtils.h"

namespace nebula {
namespace storage {

TEST(LookupTraverseTest, SimpleTagIndexTest) {
    fs::TempDir rootPath("/tmp/SimpleVertexIndexTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    GraphSpaceID spaceId = 1;
    auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(vIdLen.ok());
    auto totalParts = cluster.getTotalParts();
    ASSERT_TRUE(QueryTestUtils::mockVertexData(env, totalParts, true));
    ASSERT_TRUE(QueryTestUtils::mockEdgeData(env, totalParts));

    TagID player = 1;
    EdgeType serve = 101;

    // one IndexQueryContext, where player.name_ == "Rudy Gay"
    {
        auto* processor = LookupTraverseProcessor::instance(env, nullptr, nullptr);
        cpp2::LookupAndTraverseRequest req;
        decltype(req.indices) indices;
        req.set_space_id(spaceId);
        indices.set_tag_or_edge_id(1);
        indices.set_is_edge(false);

        decltype(req.parts) parts;
        for (int32_t p = 1; p <= totalParts; p++) {
            parts.emplace_back(p);
        }
        req.set_parts(std::move(parts));

        cpp2::IndexColumnHint columnHint;
        std::string name = "Rudy Gay";
        columnHint.set_begin_value(Value(name));
        columnHint.set_column_name("name");
        columnHint.set_scan_type(cpp2::ScanType::PREFIX);
        decltype(indices.contexts[0].column_hints) columnHints;
        columnHints.emplace_back(std::move(columnHint));
        cpp2::IndexQueryContext context1;
        context1.set_column_hints(std::move(columnHints));
        context1.set_filter("");
        context1.set_index_id(1);
        decltype(indices.contexts) contexts;
        contexts.emplace_back(std::move(context1));
        indices.set_contexts(std::move(contexts));
        req.set_indices(std::move(indices));

        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        req.set_traverse_spec(QueryTestUtils::buildTraverseSpec(over, tags, edges));

        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        ASSERT_EQ(0, resp.result.failed_parts.size());
        std::vector<VertexID> vertices = {"Rudy Gay"};
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 1, 5);
    }
    // two IndexQueryContext, where player.name_ == "Rudy Gay" OR player.name_ == "Kobe Bryant"
    {
        auto* processor = LookupTraverseProcessor::instance(env, nullptr, nullptr);
        cpp2::LookupAndTraverseRequest req;
        decltype(req.indices) indices;
        req.set_space_id(spaceId);
        indices.set_tag_or_edge_id(1);
        indices.set_is_edge(false);

        decltype(req.parts) parts;
        for (int32_t p = 1; p <= totalParts; p++) {
            parts.emplace_back(p);
        }
        req.set_parts(std::move(parts));

        // player.name_ == "Rudy Gay"
        cpp2::IndexColumnHint columnHint1;
        std::string name1 = "Rudy Gay";
        columnHint1.set_begin_value(Value(name1));
        columnHint1.set_column_name("name");
        columnHint1.set_scan_type(cpp2::ScanType::PREFIX);
        // player.name_ == "Kobe Bryant"
        cpp2::IndexColumnHint columnHint2;
        std::string name2 = "Kobe Bryant";
        columnHint2.set_begin_value(Value(name2));
        columnHint2.set_column_name("name");
        columnHint2.set_scan_type(cpp2::ScanType::PREFIX);
        decltype(indices.contexts[0].column_hints) columnHints1;
        columnHints1.emplace_back(std::move(columnHint1));
        decltype(indices.contexts[0].column_hints) columnHints2;
        columnHints2.emplace_back(std::move(columnHint2));
        cpp2::IndexQueryContext context1;
        context1.set_column_hints(std::move(columnHints1));
        context1.set_filter("");
        context1.set_index_id(1);
        cpp2::IndexQueryContext context2;
        context2.set_column_hints(std::move(columnHints2));
        context2.set_filter("");
        context2.set_index_id(1);
        decltype(indices.contexts) contexts;
        contexts.emplace_back(std::move(context1));
        contexts.emplace_back(std::move(context2));
        indices.set_contexts(std::move(contexts));
        req.set_indices(std::move(indices));

        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        req.set_traverse_spec(QueryTestUtils::buildTraverseSpec(over, tags, edges));

        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        ASSERT_EQ(0, resp.result.failed_parts.size());
        std::vector<VertexID> vertices = {"Rudy Gay", "Kobe Bryant"};
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 2, 5);
    }
}

TEST(LookupTraverseTest, SimpleEdgeIndexTest) {
    fs::TempDir rootPath("/tmp/SimpleEdgeIndexTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    GraphSpaceID spaceId = 1;
    auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(vIdLen.ok());
    auto totalParts = cluster.getTotalParts();
    ASSERT_TRUE(QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_TRUE(QueryTestUtils::mockEdgeData(env, totalParts, 1, true));

    TagID player = 1;
    EdgeType serve = 101;

    // one IndexQueryContext, where serve.player == "Tony Parker"
    {
        auto* processor = LookupTraverseProcessor::instance(env, nullptr, nullptr);
        cpp2::LookupAndTraverseRequest req;
        decltype(req.indices) indices;
        req.set_space_id(spaceId);
        indices.set_tag_or_edge_id(101);
        indices.set_is_edge(true);

        decltype(req.parts) parts;
        for (int32_t p = 1; p <= totalParts; p++) {
            parts.emplace_back(p);
        }
        req.set_parts(std::move(parts));

        std::string tony = "Tony Parker";
        cpp2::IndexColumnHint columnHint;
        columnHint.set_begin_value(Value(tony));
        columnHint.set_column_name("player");
        columnHint.set_scan_type(cpp2::ScanType::PREFIX);
        decltype(indices.contexts[0].column_hints) columnHints;
        columnHints.emplace_back(std::move(columnHint));
        cpp2::IndexQueryContext context1;
        context1.set_column_hints(std::move(columnHints));
        context1.set_filter("");
        context1.set_index_id(101);
        decltype(indices.contexts) contexts;
        contexts.emplace_back(std::move(context1));
        indices.set_contexts(std::move(contexts));
        req.set_indices(std::move(indices));

        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        req.set_traverse_spec(QueryTestUtils::buildTraverseSpec(over, tags, edges));

        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        ASSERT_EQ(0, resp.result.failed_parts.size());
        std::vector<VertexID> vertices = {"Tony Parker"};
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 1, 5);
    }
    // two IndexQueryContext
    // where serve.player == "Tony Parker" OR serve.player == "Yao Ming"
    {
        auto* processor = LookupTraverseProcessor::instance(env, nullptr, nullptr);
        cpp2::LookupAndTraverseRequest req;
        decltype(req.indices) indices;
        req.set_space_id(spaceId);
        indices.set_tag_or_edge_id(101);
        indices.set_is_edge(true);

        decltype(req.parts) parts;
        for (int32_t p = 1; p <= totalParts; p++) {
            parts.emplace_back(p);
        }
        req.set_parts(std::move(parts));

        std::string tony = "Tony Parker";
        std::string yao = "Yao Ming";
        // teammates.player1 == "Tony Parker"
        cpp2::IndexColumnHint columnHint1;
        columnHint1.set_begin_value(Value(tony));
        columnHint1.set_column_name("player1");
        columnHint1.set_scan_type(cpp2::ScanType::PREFIX);
        // teammates.player1 == "Yao Ming"
        cpp2::IndexColumnHint columnHint2;
        columnHint2.set_begin_value(Value(yao));
        columnHint2.set_column_name("player1");
        columnHint2.set_scan_type(cpp2::ScanType::PREFIX);
        decltype(indices.contexts[0].column_hints) columnHints1;
        columnHints1.emplace_back(std::move(columnHint1));
        decltype(indices.contexts[0].column_hints) columnHints2;
        columnHints2.emplace_back(std::move(columnHint2));
        cpp2::IndexQueryContext context1;
        context1.set_column_hints(std::move(columnHints1));
        context1.set_filter("");
        context1.set_index_id(101);
        cpp2::IndexQueryContext context2;
        context2.set_column_hints(std::move(columnHints2));
        context2.set_filter("");
        context2.set_index_id(101);
        decltype(indices.contexts) contexts;
        contexts.emplace_back(std::move(context1));
        contexts.emplace_back(std::move(context2));
        indices.set_contexts(std::move(contexts));
        req.set_indices(std::move(indices));

        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        req.set_traverse_spec(QueryTestUtils::buildTraverseSpec(over, tags, edges));

        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        ASSERT_EQ(0, resp.result.failed_parts.size());
        std::vector<VertexID> vertices = {"Tony Parker", "Yao Ming"};
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 2, 5);
    }
}

TEST(LookupTraverseTest, TagIndexFilterTest) {
    fs::TempDir rootPath("/tmp/TagIndexFilterTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    GraphSpaceID spaceId = 1;
    auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(vIdLen.ok());
    auto totalParts = cluster.getTotalParts();
    ASSERT_TRUE(QueryTestUtils::mockVertexData(env, totalParts, true));
    ASSERT_TRUE(QueryTestUtils::mockEdgeData(env, totalParts));

    TagID player = 1;
    EdgeType serve = 101;

    // one IndexQueryContext, where player.name == "Rudy Gay" AND player.name == 34
    {
        auto* processor = LookupTraverseProcessor::instance(env, nullptr, nullptr);
        cpp2::LookupAndTraverseRequest req;
        decltype(req.indices) indices;
        req.set_space_id(spaceId);
        indices.set_tag_or_edge_id(1);
        indices.set_is_edge(false);

        decltype(req.parts) parts;
        for (int32_t p = 1; p <= totalParts; p++) {
            parts.emplace_back(p);
        }
        req.set_parts(std::move(parts));

        cpp2::IndexColumnHint columnHint;
        std::string name = "Rudy Gay";
        columnHint.set_begin_value(Value(name));
        columnHint.set_column_name("name");
        columnHint.set_scan_type(cpp2::ScanType::PREFIX);
        decltype(indices.contexts[0].column_hints) columnHints;
        columnHints.emplace_back(std::move(columnHint));
        cpp2::IndexQueryContext context1;
        context1.set_column_hints(std::move(columnHints));
        RelationalExpression expr(
            Expression::Kind::kRelEQ,
            new TagPropertyExpression(
                new std::string(folly::to<std::string>("player")),
                new std::string("age")),
            new ConstantExpression(Value(34L)));
        context1.set_filter(expr.encode());
        context1.set_index_id(1);
        decltype(indices.contexts) contexts;
        contexts.emplace_back(std::move(context1));
        indices.set_contexts(std::move(contexts));
        req.set_indices(std::move(indices));

        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        req.set_traverse_spec(QueryTestUtils::buildTraverseSpec(over, tags, edges));

        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        ASSERT_EQ(0, resp.result.failed_parts.size());
        std::vector<VertexID> vertices = {"Rudy Gay"};
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 1, 5);
    }
    // one IndexQueryContext, where player.name == "Rudy Gay" AND player.name > 34
    {
        auto* processor = LookupTraverseProcessor::instance(env, nullptr, nullptr);
        cpp2::LookupAndTraverseRequest req;
        decltype(req.indices) indices;
        req.set_space_id(spaceId);
        indices.set_tag_or_edge_id(1);
        indices.set_is_edge(false);

        decltype(req.parts) parts;
        for (int32_t p = 1; p <= totalParts; p++) {
            parts.emplace_back(p);
        }
        req.set_parts(std::move(parts));

        cpp2::IndexColumnHint columnHint;
        std::string name = "Rudy Gay";
        columnHint.set_begin_value(Value(name));
        columnHint.set_column_name("name");
        columnHint.set_scan_type(cpp2::ScanType::PREFIX);
        decltype(indices.contexts[0].column_hints) columnHints;
        columnHints.emplace_back(std::move(columnHint));
        cpp2::IndexQueryContext context1;
        context1.set_column_hints(std::move(columnHints));
        RelationalExpression expr(
            Expression::Kind::kRelGT,
            new TagPropertyExpression(
                new std::string(folly::to<std::string>("player")),
                new std::string("age")),
            new ConstantExpression(Value(34L)));
        context1.set_filter(expr.encode());
        context1.set_index_id(1);
        decltype(indices.contexts) contexts;
        contexts.emplace_back(std::move(context1));
        indices.set_contexts(std::move(contexts));
        req.set_indices(std::move(indices));

        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        req.set_traverse_spec(QueryTestUtils::buildTraverseSpec(over, tags, edges));

        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        ASSERT_EQ(0, resp.result.failed_parts.size());
        ASSERT_EQ(0, resp.vertices.rows.size());
    }
}

TEST(LookupTraverseTest, EdgeIndexFilterTest) {
    fs::TempDir rootPath("/tmp/EdgeIndexFilterTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    GraphSpaceID spaceId = 1;
    auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(vIdLen.ok());
    auto totalParts = cluster.getTotalParts();
    ASSERT_TRUE(QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_TRUE(QueryTestUtils::mockEdgeData(env, totalParts, 1, true));

    TagID player = 1;
    EdgeType serve = 101;

    // one IndexQueryContext
    // where serve.player == "Tony Parker" AND serve.teamName == "Spurs"
    {
        auto* processor = LookupTraverseProcessor::instance(env, nullptr, nullptr);
        cpp2::LookupAndTraverseRequest req;
        decltype(req.indices) indices;
        req.set_space_id(spaceId);
        indices.set_tag_or_edge_id(101);
        indices.set_is_edge(true);

        decltype(req.parts) parts;
        for (int32_t p = 1; p <= totalParts; p++) {
            parts.emplace_back(p);
        }
        req.set_parts(std::move(parts));

        std::string tony = "Tony Parker";
        cpp2::IndexColumnHint columnHint;
        columnHint.set_begin_value(Value(tony));
        columnHint.set_column_name("player");
        columnHint.set_scan_type(cpp2::ScanType::PREFIX);
        decltype(indices.contexts[0].column_hints) columnHints;
        columnHints.emplace_back(std::move(columnHint));
        cpp2::IndexQueryContext context1;
        context1.set_column_hints(std::move(columnHints));
        RelationalExpression expr(
            Expression::Kind::kRelGT,
            new EdgePropertyExpression(
                new std::string(folly::to<std::string>("Teammate")),
                new std::string("teamName")),
            new ConstantExpression(Value("Spurs")));
        context1.set_filter("");
        context1.set_index_id(101);
        decltype(indices.contexts) contexts;
        contexts.emplace_back(std::move(context1));
        indices.set_contexts(std::move(contexts));
        req.set_indices(std::move(indices));

        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        req.set_traverse_spec(QueryTestUtils::buildTraverseSpec(over, tags, edges));

        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        ASSERT_EQ(0, resp.result.failed_parts.size());
        std::vector<VertexID> vertices = {"Tony Parker"};
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 1, 5);
    }
    // one IndexQueryContext
    // where serve.player == "Tony Parker" AND serve.startYear < 2001
    {
        auto* processor = LookupTraverseProcessor::instance(env, nullptr, nullptr);
        cpp2::LookupAndTraverseRequest req;
        decltype(req.indices) indices;
        req.set_space_id(spaceId);
        indices.set_tag_or_edge_id(101);
        indices.set_is_edge(true);

        decltype(req.parts) parts;
        for (int32_t p = 1; p <= totalParts; p++) {
            parts.emplace_back(p);
        }
        req.set_parts(std::move(parts));

        std::string tony = "Tony Parker";
        cpp2::IndexColumnHint columnHint;
        columnHint.set_begin_value(Value(tony));
        columnHint.set_column_name("player1");
        columnHint.set_scan_type(cpp2::ScanType::PREFIX);
        decltype(indices.contexts[0].column_hints) columnHints;
        columnHints.emplace_back(std::move(columnHint));
        cpp2::IndexQueryContext context1;
        context1.set_column_hints(std::move(columnHints));
        RelationalExpression expr(
            Expression::Kind::kRelLT,
            new EdgePropertyExpression(
                new std::string(folly::to<std::string>("StartYear")),
                new std::string("startYear")),
            new ConstantExpression(Value(2001L)));
        context1.set_filter(expr.encode());
        context1.set_index_id(101);
        decltype(indices.contexts) contexts;
        contexts.emplace_back(std::move(context1));
        indices.set_contexts(std::move(contexts));
        req.set_indices(std::move(indices));

        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        req.set_traverse_spec(QueryTestUtils::buildTraverseSpec(over, tags, edges));

        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        ASSERT_EQ(0, resp.result.failed_parts.size());
        ASSERT_EQ(0, resp.vertices.rows.size());
    }
}

}  // namespace storage
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
