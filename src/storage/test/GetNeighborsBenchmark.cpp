/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "base/Base.h"
#include <folly/Benchmark.h>
#include <gtest/gtest.h>
#include "fs/TempDir.h"
#include "storage/query/GetNeighborsProcessor.h"
#include "storage/test/QueryTestUtils.h"

nebula::storage::StorageEnv* gStorageEnv = nullptr;
int32_t gTotalParts = 6;

void go(const nebula::storage::cpp2::GetNeighborsRequest& req, int32_t iters) {
    for (int32_t i = 0; i < iters; i++) {
        auto* processor = nebula::storage::GetNeighborsProcessor::instance(
                                           gStorageEnv, nullptr, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
    }
}

/*
BENCHMARK(one_player_over_serve, iters) {
    nebula::storage::cpp2::GetNeighborsRequest req;
    BENCHMARK_SUSPEND {
        std::vector<nebula::VertexID> vertices = {"Tim Duncan"};
        std::vector<nebula::EdgeType> over = {101};
        std::vector<std::pair<nebula::TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<nebula::EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(1, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(101, std::vector<std::string>{"teamName", "startYear", "endYear"});
        req = nebula::storage::QueryTestUtils::buildRequest(gTotalParts, vertices, over,
                                                            tags, edges);
    }
    go(req, iters);
}

BENCHMARK(ten_vertex_over_serve, iters) {
    nebula::storage::cpp2::GetNeighborsRequest req;
    BENCHMARK_SUSPEND {
        std::vector<nebula::VertexID> vertices = {"Tim Duncan", "Tony Parker", "LaMarcus Aldridge",
                                                  "Rudy Gay", "Marco Belinelli", "Danny Green",
                                                  "Kyle Anderson", "Aron Baynes", "Boris Diaw",
                                                  "Tiago Splitter"};
        std::vector<nebula::EdgeType> over = {101};
        std::vector<std::pair<nebula::TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<nebula::EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(1, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(101, std::vector<std::string>{"teamName", "startYear", "endYear"});
        req = nebula::storage::QueryTestUtils::buildRequest(gTotalParts, vertices, over,
                                                            tags, edges);
    }
    go(req, iters);
}

BENCHMARK(one_team_over_serve, iters) {
    nebula::storage::cpp2::GetNeighborsRequest req;
    BENCHMARK_SUSPEND {
        std::vector<nebula::VertexID> vertices = {"Spurs"};
        std::vector<nebula::EdgeType> over = {-101};
        std::vector<std::pair<nebula::TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<nebula::EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(2, std::vector<std::string>{"name"});
        edges.emplace_back(-101, std::vector<std::string>{"teamName", "startYear", "endYear"});
        req = nebula::storage::QueryTestUtils::buildRequest(gTotalParts, vertices, over,
                                                            tags, edges);
    }
    go(req, iters);
}

BENCHMARK(ten_team_over_serve, iters) {
    nebula::storage::cpp2::GetNeighborsRequest req;
    BENCHMARK_SUSPEND {
        std::vector<nebula::VertexID> vertices = {"Warriors", "Nuggets", "Rockets", "Trail Blazers",
                                                  "Spurs", "Thunders", "Jazz", "Clippers", "Kings",
                                                  "Timberwolves"};
        std::vector<nebula::EdgeType> over = {-101};
        std::vector<std::pair<nebula::TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<nebula::EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(2, std::vector<std::string>{"name"});
        edges.emplace_back(-101, std::vector<std::string>{"teamName", "startYear", "endYear"});
        req = nebula::storage::QueryTestUtils::buildRequest(gTotalParts, vertices, over,
                                                            tags, edges);
    }
    go(req, iters);
}
*/

int main(int argc, char** argv) {
    folly::init(&argc, &argv, true);
    nebula::fs::TempDir rootPath("/tmp/GetNeighborsBenchTest.XXXXXX");
    nebula::mock::MockCluster cluster;
    cluster.startStorage({0, 0}, rootPath.path());
    gStorageEnv = cluster.storageEnv_.get();
    gTotalParts = cluster.getTotalParts();
    nebula::storage::QueryTestUtils::mockVertexData(gStorageEnv, gTotalParts);
    nebula::storage::QueryTestUtils::mockEdgeData(gStorageEnv, gTotalParts);
    folly::runBenchmarks();
    return 0;
}

/*
40 processors, Intel(R) Xeon(R) CPU E5-2690 v2 @ 3.00GHz.
todo: nba dataset is quite small, need to be check on real data

============================================================================
GetNeighborsBenchmark.cpprelative                         time/iter  iters/s
============================================================================
one_player_over_serve                                       78.02us   12.82K
ten_vertex_over_serve                                      686.69us    1.46K
one_team_over_serve                                        184.27us    5.43K
ten_team_over_serve                                        782.26us    1.28K
============================================================================
*/
