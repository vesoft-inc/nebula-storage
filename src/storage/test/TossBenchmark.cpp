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

#include "TossEnvironment.h"
#include "TossTestExecutor.h"

#define EDGES_PER_REQ_1      1
#define EDGES_PER_REQ_10     0
#define EDGES_PER_REQ_100    0
#define EDGES_PER_REQ_1000   0


namespace nebula {
namespace storage {

const std::string kMetaName = "127.0.0.1";  // NOLINT
constexpr int32_t kMetaPort = 6500;
const std::string kSpaceName = "test";   // NOLINT
const std::string kEdgeName = "test_edge";   // NOLINT
constexpr int32_t kPart = 5;
constexpr int32_t kReplica = 3;

TossEnvironment* gEnv{nullptr};
std::unique_ptr<TestSpace> spaceToss;
std::unique_ptr<TestSpace> spaceNoToss;
std::vector<cpp2::NewEdge> edgesToss;
std::vector<cpp2::NewEdge> edgesNoToss;

void setupEnvironment() {
    gEnv = TossEnvironment::getInstance();
    CHECK(gEnv->connectToMeta(kMetaName, kMetaPort));

    std::vector<meta::cpp2::PropertyType> colTypes{meta::cpp2::PropertyType::INT64,
                                                   meta::cpp2::PropertyType::STRING};

    auto intVid = meta::cpp2::PropertyType::INT64;
    bool useToss = true;

    spaceToss = std::make_unique<TestSpace>(
        gEnv, "bm_toss", kPart, kReplica, intVid, kEdgeName, colTypes, useToss);

    spaceNoToss = std::make_unique<TestSpace>(
        gEnv, "bm_toss_no", kPart, kReplica, intVid, kEdgeName, colTypes, !useToss);

    auto num = 10000U;
    auto base = 10000;
    for (auto i = 0U; i != num; ++i) {
        edgesToss.emplace_back(TossTestUtils::makeEdge(base + i, spaceToss->edgeType_));
    }

    for (auto i = 0U; i != num; ++i) {
        edgesNoToss.emplace_back(TossTestUtils::makeEdge(base + i, spaceNoToss->edgeType_));
    }
}

#if EDGES_PER_REQ_1
BENCHMARK(bm_no_toss) {
    AddEdgeExecutor exec(spaceNoToss.get(), edgesNoToss);
}

BENCHMARK_RELATIVE(bm_toss) {
    AddEdgeExecutor exec(spaceToss.get(), edgesToss);
}


BENCHMARK_DRAW_LINE();
#endif

}  // namespace storage
}  // namespace nebula

// using namespace nebula::storage;  // NOLINT

int main(int argc, char** argv) {
    FLAGS_heartbeat_interval_secs = 1;
    FLAGS_logtostderr = 1;

    folly::init(&argc, &argv, false);
    nebula::storage::setupEnvironment();
    folly::runBenchmarks();

    LOG(INFO) << "exit main";
}

// ============================================================================
// bm_no_toss                                                  36.66ms    27.28
// bm_toss                                           54.64%    67.09ms    14.91
// ----------------------------------------------------------------------------
