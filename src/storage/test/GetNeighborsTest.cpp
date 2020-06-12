/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include <gtest/gtest.h>
#include "storage/query/GetNeighborsProcessor.h"
#include "storage/test/QueryTestUtils.h"

namespace nebula {
namespace storage {

TEST(GetNeighborsTest, PropertyTest) {
    fs::TempDir rootPath("/tmp/GetNeighborsTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));

    TagID player = 1;
    TagID team = 2;
    EdgeType serve = 101;
    EdgeType teammate = 102;

    {
        LOG(INFO) << "OneOutEdgeMultiProperty";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // vId, stat, player, serve
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 1, 4);
    }
    {
        LOG(INFO) << "OneOutEdgeKeyInProperty";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear",
                                                           _SRC, _TYPE, _RANK, _DST});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // vId, stat, player, serve
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 1, 4);
    }
    {
        LOG(INFO) << "OneInEdgeMultiProperty";
        std::vector<VertexID> vertices = {"Spurs"};
        std::vector<EdgeType> over = {-serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(team, std::vector<std::string>{"name"});
        edges.emplace_back(-serve, std::vector<std::string>{
                           "playerName", "startYear", "teamCareer"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        ASSERT_EQ(0, resp.result.failed_parts.size());
        // vId, stat, team, - serve
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 1, 4);
    }
    {
        LOG(INFO) << "OneInEdgeKeyInProperty";
        std::vector<VertexID> vertices = {"Spurs"};
        std::vector<EdgeType> over = {-serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(team, std::vector<std::string>{"name"});
        edges.emplace_back(-serve, std::vector<std::string>{
                           "playerName", "startYear", "teamCareer",
                           _SRC, _TYPE, _RANK, _DST});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        ASSERT_EQ(0, resp.result.failed_parts.size());
        // vId, stat, team, - serve
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 1, 4);
    }
    {
        LOG(INFO) << "GetNotExistTag";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        // Duncan would have not add any data of tag team
        tags.emplace_back(team, std::vector<std::string>{"name"});
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // vId, stat, team, player, serve
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 1, 5);
    }
    {
        LOG(INFO) << "OutEdgeReturnAllProperty";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        req.edge_direction = cpp2::EdgeDirection::OUT_EDGE;

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // vId, stat, player, serve, teammate
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 1, 5);
    }
    {
        LOG(INFO) << "InEdgeReturnAllProperty";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        req.edge_direction = cpp2::EdgeDirection::IN_EDGE;

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // vId, stat, player, - teammate, - serve
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 1, 5);
    }
    {
        LOG(INFO) << "InOutEdgeReturnAllProperty";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        req.edge_direction = cpp2::EdgeDirection::BOTH;

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // vId, stat, player, - teammate, - serve, serve, teammate
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 1, 7);
    }
    {
        LOG(INFO) << "InEdgeReturnAllProperty";
        std::vector<VertexID> vertices = {"Spurs"};
        std::vector<EdgeType> over = {};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(team, std::vector<std::string>{"name"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        req.edge_direction = cpp2::EdgeDirection::BOTH;

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // vId, stat, player, - teammate, - serve, serve, teammate
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 1, 7);
    }
    {
        LOG(INFO) << "Nullable";
        std::vector<VertexID> vertices = {"Steve Nash"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "champions"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "champions"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // vId, stat, player, serve
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 1, 4);
    }
    {
        LOG(INFO) << "DefaultValue";
        std::vector<VertexID> vertices = {"Dwight Howard"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "country"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "type"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // vId, stat, player, serve
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 1, 4);
    }
    {
        LOG(INFO) << "Misc";
        std::vector<VertexID> vertices = {"Dwight Howard"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{
            "name", "age", "playing", "career", "startYear", "endYear", "games",
            "avgScore", "serveTeams", "country", "champions"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear",
            "playerName", "teamCareer", "teamGames", "teamAvgScore", "type", "champions",
            _SRC, _TYPE, _RANK, _DST});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // vId, stat, player, serve
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 1, 4);
    }
    {
        LOG(INFO) << "MultiOutEdgeMultiProperty";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {serve, teammate};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        edges.emplace_back(teammate, std::vector<std::string>{"player1", "player2", "teamName"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // vId, stat, player, serve, teammate
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 1, 5);
    }
    {
        LOG(INFO) << "InOutEdgeMultiProperty";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {serve, -teammate};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        edges.emplace_back(teammate, std::vector<std::string>{"player1", "player2", "teamName"});
        edges.emplace_back(-teammate, std::vector<std::string>{"player1", "player2", "teamName"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // vId, stat, player, serve, teammate, -teammate
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 1, 6);
    }
}

TEST(GetNeighborsTest, GoFromMultiVerticesTest) {
    fs::TempDir rootPath("/tmp/GetNeighborsTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));

    TagID player = 1;
    TagID team = 2;
    EdgeType serve = 101;
    EdgeType teammate = 102;

    {
        LOG(INFO) << "OneOutEdgeMultiProperty";
        std::vector<VertexID> vertices = {"Tim Duncan", "Tony Parker"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // vId, stat, player, serve
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 2, 4);
    }
    {
        LOG(INFO) << "OneInEdgeMultiProperty";
        std::vector<VertexID> vertices = {"Spurs", "Rockets"};
        std::vector<EdgeType> over = {-serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(team, std::vector<std::string>{"name"});
        edges.emplace_back(-serve, std::vector<std::string>{
                           "playerName", "startYear", "teamCareer"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        ASSERT_EQ(0, resp.result.failed_parts.size());
        // vId, stat, player, -serve
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 2, 4);
    }
    {
        LOG(INFO) << "Misc";
        std::vector<VertexID> vertices = {"Tracy McGrady", "Kobe Bryant"};
        std::vector<EdgeType> over = {serve, teammate};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{
            "name", "age", "playing", "career", "startYear", "endYear", "games",
            "avgScore", "serveTeams", "country", "champions"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear",
            "teamCareer", "teamGames", "teamAvgScore", "type", "champions",
            _SRC, _TYPE, _RANK, _DST});
        edges.emplace_back(teammate, std::vector<std::string>{"player1", "player2",
            "teamName", "startYear", "endYear"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // vId, stat, player, serve, teammate
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 2, 5);
    }
}

TEST(GetNeighborsTest, StatTest) {
    fs::TempDir rootPath("/tmp/GetNeighborsTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));

    TagID player = 1;
    EdgeType serve = 101;

    {
        LOG(INFO) << "CollectStatOfDifferentProperty";
        std::vector<VertexID> vertices = {"LeBron James"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        std::vector<cpp2::StatProp> statProps;
        {
            // count teamGames_ in all served history
            cpp2::StatProp statProp;
            statProp.set_alias("Total games");
            EdgePropertyExpression exp(new std::string(folly::to<std::string>(serve)),
                                       new std::string("teamGames"));
            statProp.set_prop(Expression::encode(exp));
            statProp.stat = cpp2::StatType::SUM;
            statProps.emplace_back(std::move(statProp));
        }
        {
            // avg scores in all served teams
            cpp2::StatProp statProp;
            statProp.set_alias("Avg scores in all served teams");
            EdgePropertyExpression exp(new std::string(folly::to<std::string>(serve)),
                                       new std::string("teamAvgScore"));
            statProp.set_prop(Expression::encode(exp));
            statProp.stat = cpp2::StatType::AVG;
            statProps.emplace_back(std::move(statProp));
        }
        {
            // longest consecutive team career in a team
            cpp2::StatProp statProp;
            statProp.set_alias("Longest consecutive team career in a team");
            EdgePropertyExpression exp(new std::string(folly::to<std::string>(serve)),
                                       new std::string("teamCareer"));
            statProp.set_prop(Expression::encode(exp));
            statProp.stat = cpp2::StatType::MAX;
            statProps.emplace_back(std::move(statProp));
        }
        req.stat_props = std::move(statProps);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        std::unordered_map<VertexID, std::vector<Value>> expectStat;
        expectStat.emplace("LeBron James", std::vector<Value>{
            548 + 294 + 301 + 115, (29.7 + 27.1 + 27.5 + 25.7) / 4, 7});

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // vId, stat, player, serve
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges,
                                      1, 4, &expectStat);
    }
    {
        LOG(INFO) << "CollectStatOfSameProperty";
        std::vector<VertexID> vertices = {"LeBron James"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(
            serve, std::vector<std::string>{"teamName", "startYear", "teamAvgScore"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        std::vector<cpp2::StatProp> statProps;
        {
            // avg scores in all served teams
            cpp2::StatProp statProp;
            statProp.set_alias("Avg scores in all served teams");
            EdgePropertyExpression exp(new std::string(folly::to<std::string>(serve)),
                                       new std::string("teamAvgScore"));
            statProp.set_prop(Expression::encode(exp));
            statProp.stat = cpp2::StatType::AVG;
            statProps.emplace_back(std::move(statProp));
        }
        {
            // min avg scores in all served teams
            cpp2::StatProp statProp;
            statProp.set_alias("Min scores in all served teams");
            EdgePropertyExpression exp(new std::string(folly::to<std::string>(serve)),
                                       new std::string("teamAvgScore"));
            statProp.set_prop(Expression::encode(exp));
            statProp.stat = cpp2::StatType::MIN;
            statProps.emplace_back(std::move(statProp));
        }
        {
            // max avg scores in all served teams
            cpp2::StatProp statProp;
            statProp.set_alias("Max scores in all served teams");
            EdgePropertyExpression exp(new std::string(folly::to<std::string>(serve)),
                                       new std::string("teamAvgScore"));
            statProp.set_prop(Expression::encode(exp));
            statProp.stat = cpp2::StatType::MAX;
            statProps.emplace_back(std::move(statProp));
        }
        req.stat_props = std::move(statProps);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        std::unordered_map<VertexID, std::vector<Value>> expectStat;
        expectStat.emplace("LeBron James", std::vector<Value>{
            (29.7 + 27.1 + 27.5 + 25.7) / 4, 25.7, 29.7});

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // vId, stat, player, serve
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges,
                                      1, 4, &expectStat);
    }
}

/*
TEST(GetNeighborsTest, SampleTest) {
    fs::TempDir rootPath("/tmp/GetNeighborsTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));

    TagID player = 1;
    TagID team = 2;
    EdgeType serve = 101;
    EdgeType teammate = 102;

    {
        FLAGS_max_edge_returned_per_vertex = 10;
        LOG(INFO) << "SingleEdgeTypeCutOff";
        std::vector<VertexID> vertices = {"Spurs"};
        std::vector<EdgeType> over = {-serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(team, std::vector<std::string>{"name"});
        edges.emplace_back(-serve, std::vector<std::string>{
                           "playerName", "startYear", "teamCareer"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        ASSERT_EQ(0, resp.result.failed_parts.size());
        ASSERT_EQ(1, resp.vertices.rows.size());
        // 4 column, vId, stat, team, -serve
        ASSERT_EQ(4, resp.vertices.rows[0].columns.size());
        ASSERT_EQ(10, resp.vertices.rows[0].columns[3].getDataSet().rows.size());
    }
    {
        FLAGS_max_edge_returned_per_vertex = 4;
        LOG(INFO) << "MultiEdgeTypeCutOff";
        std::vector<VertexID> vertices = {"Dwyane Wade"};
        std::vector<EdgeType> over = {serve, teammate};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        edges.emplace_back(teammate, std::vector<std::string>{"player1", "player2", "teamName"});

        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        // 5 column, vId, stat, player, serve, teammate
        // Dwyane Wad has 4 serve edge, 2 teammate edge
        // with cut off = 4, only serve edges will be returned
        ASSERT_EQ(0, resp.result.failed_parts.size());
        ASSERT_EQ(1, resp.vertices.rows.size());
        ASSERT_EQ(5, resp.vertices.rows[0].columns.size());
        ASSERT_EQ(4, resp.vertices.rows[0].columns[3].getDataSet().rows.size());
        ASSERT_EQ(NullType::__NULL__, resp.vertices.rows[0].columns[4].getNull());
    }
    {
        FLAGS_max_edge_returned_per_vertex = 5;
        LOG(INFO) << "MultiEdgeTypeCutOff";
        std::vector<VertexID> vertices = {"Dwyane Wade"};
        std::vector<EdgeType> over = {serve, teammate};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        edges.emplace_back(teammate, std::vector<std::string>{"playe1", "player2", "teamName"});

        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        // 5 column, vId, stat, player, serve, teammate
        // Dwyane Wad has 4 serve edge, 2 teammate edge
        // with cut off = 5, 4 serve edges and 1 teammate edge will be returned
        ASSERT_EQ(0, resp.result.failed_parts.size());
        ASSERT_EQ(1, resp.vertices.rows.size());
        ASSERT_EQ(5, resp.vertices.rows[0].columns.size());
        ASSERT_EQ(4, resp.vertices.rows[0].columns[3].getDataSet().rows.size());
        ASSERT_EQ(1, resp.vertices.rows[0].columns[4].getDataSet().rows.size());
    }
    FLAGS_max_edge_returned_per_vertex = INT_MAX;
}
*/

TEST(GetNeighborsTest, VertexCacheTest) {
    fs::TempDir rootPath("/tmp/GetNeighborsTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));
    VertexCache vertexCache(1000, 4);

    TagID player = 1;
    EdgeType serve = 101;

    {
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, &vertexCache);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // vId, stat, player, serve
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 1, 4);
    }
    {
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, &vertexCache);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // vId, stat, player, serve
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 1, 4);
    }
}

TEST(GetNeighborsTest, TtlTest) {
    FLAGS_mock_ttl_col = true;

    fs::TempDir rootPath("/tmp/GetNeighborsTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));

    TagID player = 1;
    EdgeType serve = 101;

    {
        LOG(INFO) << "OutEdgeReturnAllProperty";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // vId, stat, player, serve
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 1, 4);
    }
    {
        LOG(INFO) << "GoFromPlayerOverAll";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        req.edge_direction = cpp2::EdgeDirection::BOTH;

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // 9 column: vId, stat, player, team, general tag, - teammate, - serve, + serve, + teammate
        QueryTestUtils::checkResponse(resp.vertices, vertices, 1, 9);
    }
    sleep(FLAGS_mock_ttl_duration + 1);
    {
        LOG(INFO) << "OutEdgeReturnAllProperty";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        ASSERT_EQ(1, resp.vertices.rows.size());
        ASSERT_EQ(4, resp.vertices.rows[0].columns.size());
        ASSERT_EQ(NullType::__NULL__, resp.vertices.rows[0].columns[2].getNull());
        ASSERT_EQ(NullType::__NULL__, resp.vertices.rows[0].columns[3].getNull());
    }
    {
        LOG(INFO) << "GoFromPlayerOverAll";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        req.edge_direction = cpp2::EdgeDirection::BOTH;

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // 9 column: vId, stat, player, team, general tag, - teammate, - serve, + serve, + teammate
        ASSERT_EQ(1, resp.vertices.rows.size());
        ASSERT_EQ(9, resp.vertices.rows[0].columns.size());
        ASSERT_TRUE(resp.vertices.rows[0].columns[2].isNull());     // player expired
        ASSERT_TRUE(resp.vertices.rows[0].columns[5].isList());     // - teammate valid
        ASSERT_TRUE(resp.vertices.rows[0].columns[8].isList());     // + teammate valid
        ASSERT_TRUE(resp.vertices.rows[0].columns[6].isNull());     // - serve expired
        ASSERT_TRUE(resp.vertices.rows[0].columns[7].isNull());     // + serve expired
    }
    FLAGS_mock_ttl_col = false;
}

TEST(GetNeighborsTest, FailedTest) {
    fs::TempDir rootPath("/tmp/GetNeighborsTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));

    TagID player = 1;
    EdgeType serve = 101;

    {
        LOG(INFO) << "TagNotExists";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(std::make_pair(9999, std::vector<std::string>{"name"}));
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(1, resp.result.failed_parts.size());
        ASSERT_EQ(cpp2::ErrorCode::E_TAG_NOT_FOUND, resp.result.failed_parts.front().code);
    }
    {
        LOG(INFO) << "EdgeNotExists";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {9999};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        edges.emplace_back(std::make_pair(9999, std::vector<std::string>{"teamName"}));
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(1, resp.result.failed_parts.size());
        ASSERT_EQ(cpp2::ErrorCode::E_EDGE_NOT_FOUND, resp.result.failed_parts.front().code);
    }
    {
        LOG(INFO) << "TagPropNotExists";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(std::make_pair(player, std::vector<std::string>{"prop_not_exists"}));
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(1, resp.result.failed_parts.size());
        ASSERT_EQ(cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND, resp.result.failed_parts.front().code);
    }
    {
        LOG(INFO) << "EdgePropNotExists";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        edges.emplace_back(std::make_pair(serve, std::vector<std::string>{"prop_not_exists"}));
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(1, resp.result.failed_parts.size());
        ASSERT_EQ(cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND, resp.result.failed_parts.front().code);
    }
}

TEST(GetNeighborsTest, GoOverAllTest) {
    fs::TempDir rootPath("/tmp/GetNeighborsTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));

    {
        LOG(INFO) << "NoPropertyReturned";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges, true);
        req.edge_direction = cpp2::EdgeDirection::BOTH;

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // 2 column: vId, stat
        QueryTestUtils::checkResponse(resp.vertices, vertices, 1, 2);
    }
    {
        LOG(INFO) << "GoFromPlayerOverAll";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        req.edge_direction = cpp2::EdgeDirection::BOTH;

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // 9 column: vId, stat, player, team, general tag, - teammate, - serve, + serve, + teammate
        QueryTestUtils::checkResponse(resp.vertices, vertices, 1, 9);
    }
    {
        LOG(INFO) << "GoFromTeamOverAll";
        std::vector<VertexID> vertices = {"Spurs"};
        std::vector<EdgeType> over = {};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        req.edge_direction = cpp2::EdgeDirection::BOTH;

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // 9 column: vId, stat, player, team, general tag, - teammate, - serve, + serve, + teammate
        QueryTestUtils::checkResponse(resp.vertices, vertices, 1, 9);
    }
    {
        LOG(INFO) << "GoFromPlayerOverInEdge";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        req.edge_direction = cpp2::EdgeDirection::IN_EDGE;

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // 7 column: vId, stat, player, team, general tag, - serve, - teammate
        QueryTestUtils::checkResponse(resp.vertices, vertices, 1, 7);
    }
    {
        LOG(INFO) << "GoFromPlayerOverOutEdge";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        req.edge_direction = cpp2::EdgeDirection::OUT_EDGE;

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // 7 column: vId, stat, player, team, general tag, + serve, + teammate
        QueryTestUtils::checkResponse(resp.vertices, vertices, 1, 7);
    }
    {
        LOG(INFO) << "GoFromMultiPlayerOverAll";
        std::vector<VertexID> vertices = {"Tim Duncan", "LeBron James", "Dwyane Wade"};
        std::vector<EdgeType> over = {};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        req.edge_direction = cpp2::EdgeDirection::BOTH;

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // 9 column: vId, stat, player, team, general tag, - teammate, - serve, + serve, + teammate
        QueryTestUtils::checkResponse(resp.vertices, vertices, 3, 9);
    }
    {
        LOG(INFO) << "GoFromMultiTeamOverAll";
        std::vector<VertexID> vertices = {"Spurs", "Lakers", "Heat"};
        std::vector<EdgeType> over = {};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        req.edge_direction = cpp2::EdgeDirection::BOTH;

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // 9 column: vId, stat, player, team, general tag, - teammate, - serve, + serve, + teammate
        QueryTestUtils::checkResponse(resp.vertices, vertices, 3, 9);
    }
}

TEST(GetNeighborsTest, MultiVersionTest) {
    fs::TempDir rootPath("/tmp/GetNeighborsTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts, 3));

    {
        LOG(INFO) << "GoFromPlayerOverAll";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        req.edge_direction = cpp2::EdgeDirection::BOTH;

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // 9 column: vId, stat, player, team, general tag, - teammate, - serve, + serve, + teammate
        QueryTestUtils::checkResponse(resp.vertices, vertices, 1, 9);
    }
}

TEST(GetNeighborsTest, FilterTest) {
    fs::TempDir rootPath("/tmp/GetNeighborsTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));

    TagID player = 1;
    EdgeType serve = 101;
    EdgeType teammate = 102;

    {
        LOG(INFO) << "RelExp";
        std::vector<VertexID> vertices = {"Tracy McGrady"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        {
            // where serve.teamAvgScore > 20
            RelationalExpression exp(
                Expression::Kind::kRelGT,
                new EdgePropertyExpression(new std::string(folly::to<std::string>(serve)),
                                           new std::string("teamAvgScore")),
                new ConstantExpression(Value(20)));
            req.set_filter(Expression::encode(exp));
        }

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // vId, stat, player, serve
        nebula::DataSet expected;
        expected.colNames = {"_vid",
                             "_stats",
                             "_tag:1:name:age:avgScore",
                             "_edge:101:teamName:startYear:endYear"};
        nebula::Row row({"Tracy McGrady",
                         NullType::__NULL__,
                         nebula::List({"Tracy McGrady", 41, 19.6}),
                         nebula::List({nebula::List({"Magic", 2000, 2004}),
                                       nebula::List({"Rockets", 2004, 2010})})});
        expected.rows.emplace_back(std::move(row));
        ASSERT_EQ(expected, resp.vertices);
    }
    {
        LOG(INFO) << "LogicalExp";
        std::vector<VertexID> vertices = {"Tracy McGrady"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        {
            // where serve.teamAvgScore > 20 && serve.teamCareer <= 4
            LogicalExpression exp(
                Expression::Kind::kLogicalAnd,
                new RelationalExpression(
                    Expression::Kind::kRelGT,
                    new EdgePropertyExpression(
                        new std::string(folly::to<std::string>(serve)),
                        new std::string("teamAvgScore")),
                    new ConstantExpression(Value(20))),
                new RelationalExpression(
                    Expression::Kind::kRelLE,
                    new EdgePropertyExpression(
                        new std::string(folly::to<std::string>(serve)),
                        new std::string("teamCareer")),
                    new ConstantExpression(Value(4))));
            req.set_filter(Expression::encode(exp));
        }

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // vId, stat, player, serve
        nebula::DataSet expected;
        expected.colNames = {"_vid",
                             "_stats",
                             "_tag:1:name:age:avgScore",
                             "_edge:101:teamName:startYear:endYear"};
        auto serveEdges = nebula::List();
        serveEdges.values.emplace_back(nebula::List({"Magic", 2000, 2004}));
        nebula::Row row({"Tracy McGrady",
                         NullType::__NULL__,
                         nebula::List({"Tracy McGrady", 41, 19.6}),
                         serveEdges});
        expected.rows.emplace_back(std::move(row));
        ASSERT_EQ(expected, resp.vertices);
    }
    {
        LOG(INFO) << "Tag + Edge exp";
        std::vector<VertexID> vertices = {"Tracy McGrady"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        {
            // where serve.teamAvgScore > 20 && $^.player.games < 1000
            LogicalExpression exp(Expression::Kind::kLogicalAnd,
                new RelationalExpression(
                    Expression::Kind::kRelGT,
                    new EdgePropertyExpression(
                            new std::string(folly::to<std::string>(serve)),
                            new std::string("teamAvgScore")),
                    new ConstantExpression(Value(20))),
                new RelationalExpression(
                    Expression::Kind::kRelLE,
                    new SourcePropertyExpression(
                        new std::string(folly::to<std::string>(player)),
                        new std::string("games")),
                    new ConstantExpression(Value(1000))));
            req.set_filter(Expression::encode(exp));
        }

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // vId, stat, player, serve
        nebula::DataSet expected;
        expected.colNames = {"_vid",
                             "_stats",
                             "_tag:1:name:age:avgScore",
                             "_edge:101:teamName:startYear:endYear"};
        nebula::Row row({"Tracy McGrady",
                         NullType::__NULL__,
                         nebula::List({"Tracy McGrady", 41, 19.6}),
                         nebula::List({nebula::List({"Magic", 2000, 2004}),
                                       nebula::List({"Rockets", 2004, 2010})})});
        expected.rows.emplace_back(std::move(row));
        ASSERT_EQ(expected, resp.vertices);
    }
    {
        LOG(INFO) << "Filter apply to multi vertices";
        std::vector<VertexID> vertices = {"Tracy McGrady", "Tim Duncan", "Tony Parker",
                                          "Manu Ginobili"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        {
            // where serve.teamAvgScore > 18 && $^.player.avgScore > 18
            LogicalExpression exp(Expression::Kind::kLogicalAnd,
                new RelationalExpression(
                    Expression::Kind::kRelGT,
                    new EdgePropertyExpression(
                            new std::string(folly::to<std::string>(serve)),
                            new std::string("teamAvgScore")),
                    new ConstantExpression(Value(18))),
                new RelationalExpression(
                    Expression::Kind::kRelGT,
                    new SourcePropertyExpression(
                        new std::string(folly::to<std::string>(player)),
                        new std::string("avgScore")),
                    new ConstantExpression(Value(18))));
            req.set_filter(Expression::encode(exp));
        }

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // vId, stat, player, serve
        nebula::DataSet expected;
        expected.colNames = {"_vid",
                             "_stats",
                             "_tag:1:name:age:avgScore",
                             "_edge:101:teamName:startYear:endYear"};
        ASSERT_EQ(expected.colNames, resp.vertices.colNames);
        ASSERT_EQ(4, resp.vertices.rows.size());
        {
            nebula::Row row({"Tracy McGrady",
                            NullType::__NULL__,
                            nebula::List({"Tracy McGrady", 41, 19.6}),
                            nebula::List({nebula::List({"Magic", 2000, 2004}),
                                          nebula::List({"Rockets", 2004, 2010})})});
            for (size_t i = 0; i < 4; i++) {
                if (resp.vertices.rows[i].columns[0].getStr() == "Tracy McGrady") {
                    ASSERT_EQ(row, resp.vertices.rows[i]);
                    break;
                }
            }
        }
        {
            auto serveEdges = nebula::List();
            serveEdges.values.emplace_back(nebula::List({"Spurs", 1997, 2016}));
            nebula::Row row({"Tim Duncan",
                            NullType::__NULL__,
                            nebula::List({"Tim Duncan", 44, 19.0}),
                            serveEdges});
            for (size_t i = 0; i < 4; i++) {
                if (resp.vertices.rows[i].columns[0].getStr() == "Tim Duncan") {
                    ASSERT_EQ(row, resp.vertices.rows[i]);
                    break;
                }
            }
        }
        {
            nebula::Row row({"Tony Parker",
                            NullType::__NULL__,
                            nebula::List({"Tony Parker", 38, 15.5}),
                            NullType::__NULL__});
            for (size_t i = 0; i < 4; i++) {
                if (resp.vertices.rows[i].columns[0].getStr() == "Tony Parker") {
                    ASSERT_EQ(row, resp.vertices.rows[i]);
                    break;
                }
            }
        }
        {
            // same as 1.0, tag data is returned even if can't pass the filter
            nebula::Row row({"Manu Ginobili",
                            NullType::__NULL__,
                            nebula::List({"Manu Ginobili", 42, 13.3}),
                            NullType::__NULL__});
            for (size_t i = 0; i < 4; i++) {
                if (resp.vertices.rows[i].columns[0].getStr() == "Manu Ginobili") {
                    ASSERT_EQ(row, resp.vertices.rows[i]);
                    break;
                }
            }
        }
    }
    {
        LOG(INFO) << "Go over multi edges, but filter is only applied to one edge";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        edges.emplace_back(teammate, std::vector<std::string>{"player1", "player2", "teamName"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        {
            // where serve.teamGames > 1000
            RelationalExpression exp(
                Expression::Kind::kRelGT,
                new EdgePropertyExpression(new std::string(folly::to<std::string>(serve)),
                                           new std::string("teamGames")),
                new ConstantExpression(Value(1000)));
            req.set_filter(Expression::encode(exp));
        }

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // vId, stat, player, serve
        nebula::DataSet expected;
        expected.colNames = {"_vid",
                             "_stats",
                             "_tag:1:name:age:avgScore",
                             "_edge:101:teamName:startYear:endYear",
                             "_edge:102:player1:player2:teamName"};
        auto serveEdges = nebula::List();
        serveEdges.values.emplace_back(nebula::List({"Spurs", 1997, 2016}));
        // This will only get the edge of serve, which does not make sense
        // see https://github.com/vesoft-inc/nebula/issues/2166
        nebula::Row row({"Tim Duncan",
                         NullType::__NULL__,
                         nebula::List({"Tim Duncan", 44, 19.0}),
                         serveEdges,
                         NullType::__NULL__});
        expected.rows.emplace_back(std::move(row));
        ASSERT_EQ(expected, resp.vertices);
    }
}

// wait ExpressionContext change interface to return Value instaed of const Value&
TEST(GetNeighborsTest, ComplicateYieldTest) {
    fs::TempDir rootPath("/tmp/GetNeighborsTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));

    TagID player = 1;
    EdgeType serve = 101;

    {
        std::vector<VertexID> vertices = {"Tracy McGrady"};
        std::vector<EdgeType> over = {serve};

        std::hash<std::string> hash;
        cpp2::GetNeighborsRequest req;
        req.space_id = 1;
        req.column_names.emplace_back("_vid");
        for (const auto& vertex : vertices) {
            PartitionID partId = (hash(vertex) % totalParts) + 1;
            nebula::Row row;
            row.columns.emplace_back(vertex);
            req.parts[partId].emplace_back(std::move(row));
        }
        for (const auto& edge : over) {
            req.edge_types.emplace_back(edge);
        }

        {
            LOG(INFO) << "$^.player.endYear - 2000 as player_yield1";
            LOG(INFO) << "$^.player.endYear > 2000 as player_yield2";
            std::vector<cpp2::EntryProp> vertexProps;
            cpp2::EntryProp tagProp;
            tagProp.tag_or_edge_id = player;
            {
                cpp2::PropExp propExp;
                propExp.alias = "player_yield1";
                ArithmeticExpression exp(
                    Expression::Kind::kMinus,
                    new SourcePropertyExpression(new std::string(folly::to<std::string>(player)),
                                                new std::string("endYear")),
                    new ConstantExpression(2000));
                propExp.set_prop(Expression::encode(exp));
                tagProp.props.emplace_back(std::move(propExp));
            }
            {
                cpp2::PropExp propExp;
                propExp.alias = "player_yield2";
                RelationalExpression exp(
                    Expression::Kind::kRelGT,
                    new SourcePropertyExpression(new std::string(folly::to<std::string>(player)),
                                                new std::string("endYear")),
                    new ConstantExpression(2000));
                propExp.set_prop(Expression::encode(exp));
                tagProp.props.emplace_back(std::move(propExp));
            }
            vertexProps.emplace_back(std::move(tagProp));
            req.set_vertex_props(std::move(vertexProps));
        }
        {
            LOG(INFO) << "serve.endYear - 2000 as serve_yield1";
            LOG(INFO) << "serve.endYear > 2000 as serve_yield2";
            std::vector<cpp2::EntryProp> edgeProps;
            cpp2::EntryProp edgeProp;
            edgeProp.tag_or_edge_id = serve;
            {
                cpp2::PropExp propExp;
                propExp.alias = "serve_yield1";
                ArithmeticExpression exp(
                    Expression::Kind::kMinus,
                    new EdgePropertyExpression(new std::string(folly::to<std::string>(serve)),
                                            new std::string("endYear")),
                    new ConstantExpression(2000));
                propExp.set_prop(Expression::encode(exp));
                edgeProp.props.emplace_back(std::move(propExp));
            }
            {
                cpp2::PropExp propExp;
                propExp.alias = "serve_yield2";
                RelationalExpression exp(
                    Expression::Kind::kRelGT,
                    new EdgePropertyExpression(new std::string(folly::to<std::string>(serve)),
                                            new std::string("endYear")),
                    new ConstantExpression(2000));
                propExp.set_prop(Expression::encode(exp));
                edgeProp.props.emplace_back(std::move(propExp));
            }
            edgeProps.emplace_back(std::move(edgeProp));
            req.set_edge_props(std::move(edgeProps));
        }
        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // vId, stat, player, serve
        nebula::DataSet expected;
        expected.colNames = {"_vid",
                             "_stats",
                             "_tag:1:player_yield1:player_yield2",
                             "_edge:101:serve_yield1:serve_yield2"};
        nebula::Row row({"Tracy McGrady",
                         NullType::__NULL__,
                         nebula::List({12, true}),
                         nebula::List({nebula::List({0, false}),
                                       nebula::List({4, true}),
                                       nebula::List({10, true}),
                                       nebula::List({10, true}),
                                       nebula::List({11, true}),
                                       nebula::List({12, true})})});
        expected.rows.emplace_back(std::move(row));
        ASSERT_EQ(expected, resp.vertices);
    }
    /*
    {
        std::vector<VertexID> vertices = {"Tracy McGrady"};
        std::vector<EdgeType> over = {serve};

        std::hash<std::string> hash;
        cpp2::GetNeighborsRequest req;
        req.space_id = 1;
        req.column_names.emplace_back("_vid");
        for (const auto& vertex : vertices) {
            PartitionID partId = (hash(vertex) % totalParts) + 1;
            nebula::Row row;
            row.columns.emplace_back(vertex);
            req.parts[partId].emplace_back(std::move(row));
        }
        for (const auto& edge : over) {
            req.edge_types.emplace_back(edge);
        }

        {
            LOG(INFO) << "$^.player.endYear - $^.player.startYear as year";
            std::vector<cpp2::EntryProp> vertexProps;
            cpp2::EntryProp tagProp;
            tagProp.tag_or_edge_id = player;
            cpp2::PropExp propExp;
            propExp.alias = "year";
            ArithmeticExpression exp(
                Expression::Kind::kMinus,
                new SourcePropertyExpression(new std::string(folly::to<std::string>(player)),
                                             new std::string("endYear")),
                new SourcePropertyExpression(new std::string(folly::to<std::string>(player)),
                                             new std::string("startYear")));
            propExp.set_prop(Expression::encode(exp));
            tagProp.props.emplace_back(std::move(propExp));
            vertexProps.emplace_back(std::move(tagProp));
            req.set_vertex_props(std::move(vertexProps));
        }
        {
            LOG(INFO) << "serve.endYear - serve.startYear as team_year";
            std::vector<cpp2::EntryProp> edgeProps;
            cpp2::EntryProp edgeProp;
            edgeProp.tag_or_edge_id = serve;
            cpp2::PropExp propExp;
            propExp.alias = "team_year";
            ArithmeticExpression exp(
                Expression::Kind::kMinus,
                new EdgePropertyExpression(new std::string(folly::to<std::string>(serve)),
                                           new std::string("endYear")),
                new EdgePropertyExpression(new std::string(folly::to<std::string>(serve)),
                                           new std::string("startYear")));
            propExp.set_prop(Expression::encode(exp));
            edgeProp.props.emplace_back(std::move(propExp));
            edgeProps.emplace_back(std::move(edgeProp));
            req.set_edge_props(std::move(edgeProps));
        }

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // vId, stat, player, serve
        nebula::DataSet expected;
        expected.colNames = {"_vid",
                             "_stats",
                             "_tag:1:year",
                             "_edge:101:team_year"};
        nebula::Row row({"Tracy McGrady",
                         NullType::__NULL__,
                         nebula::List({15}),
                         nebula::List({nebula::List({3}),
                                       nebula::List({4}),
                                       nebula::List({6}),
                                       nebula::List({0}),
                                       nebula::List({1}),
                                       nebula::List({1})})});
        expected.rows.emplace_back(std::move(row));
        ASSERT_EQ(expected, resp.vertices);
    }
    */
}

}  // namespace storage
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
