 /* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include "TossEnvironment.h"
#include "folly/String.h"
#define LOG_FMT(...) LOG(INFO) << folly::sformat(__VA_ARGS__)

namespace nebula {
namespace storage {

constexpr bool kUseToss = true;
constexpr bool kNotUseToss = false;

// const std::string kMetaName = "hp-server";  // NOLINT
const std::string kMetaName = "127.0.0.1";  // NOLINT
constexpr int32_t kMetaPort = 6500;
const std::string kSpaceName = "test";   // NOLINT
const std::string kColName = "test_edge";   // NOLINT
constexpr int32_t kPart = 5;
constexpr int32_t kReplica = 3;

static int32_t b_ = 9527;
static int32_t gap = 1000;

class TossTest : public ::testing::Test {
public:
    static void SetUpTestCase() {
        env = TossEnvironment::getInstance();
        ASSERT_TRUE(env->connectToMeta(kMetaName, kMetaPort));
        env->createSpace(kSpaceName, kPart, kReplica);
        std::vector<meta::cpp2::PropertyType> types;
        types.emplace_back(meta::cpp2::PropertyType::INT64);
        types.emplace_back(meta::cpp2::PropertyType::STRING);
        auto colDefs = TossTestUtils::makeColDefs(types);
        env->setupEdgeSchema(kColName, colDefs);
        env->waitLeaderCollection();
    }

    static void TearDownTestCase() {}

    void SetUp() override {
        b_ /= gap;
        ++b_;
        b_ *= gap;

        edgeType_ = env->getEdgeType();
    }

    void TearDown() override {}

protected:
    static TossEnvironment* env;
    EdgeType edgeType_;
};

TossEnvironment* TossTest::env = nullptr;

/**
 * test case naming rules
 * e  => edge
 * glk => valid lock
 * blk => invalid lock
 */

TEST_F(TossTest, utils_test) {
    auto edges = TossTestUtils::makeNeighborEdges(b_, edgeType_, 1);
    LOG(INFO) << "edges.size() = " << edges.size();
    for (auto& e : edges) {
        LOG(INFO) << "in-edge key: " << folly::hexlify(TossTest::env->strInEdgeKey(edges[0].key));
        LOG(INFO) << "out-edge key: " << folly::hexlify(TossTest::env->strOutEdgeKey(edges[0].key));
        LOG(INFO) << "lock key: " << folly::hexlify(TossTest::env->strLockKey(edges[0].key));
        LOG(INFO) << "e.props.size() = " << e.props.size();
        for (auto& prop : e.props) {
            LOG(INFO) << prop.toString();
        }
    }
    auto lockKey = TossTest::env->strLockKey(edges[0].key);

    EXPECT_TRUE(NebulaKeyUtils::isEdge(TossTest::env->vIdLen_, lockKey));
    EXPECT_EQ(lockKey.back(), 0);
    EXPECT_TRUE(NebulaKeyUtils::isLock(TossTest::env->vIdLen_, lockKey));
}

TEST_F(TossTest, empty_db) {
    auto edges = TossTestUtils::makeNeighborEdges(b_, edgeType_, 1);
    GetNeighborsExecutor exec(edges);

    EXPECT_FALSE(env->lockExist(edges[0]));
    EXPECT_FALSE(env->outEdgeExist(edges[0]));
    EXPECT_FALSE(env->inEdgeExist(edges[0]));
    // EXPECT_EQ(edges[0].props, exec.data());
    // EXPECT_TRUE(exec.data().empty());
}

/**
 * @brief add edge
 */
TEST_F(TossTest, test0_add_eg) {
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, edgeType_, 1);

    AddEdgeExecutor exec(edges[0]);
    EXPECT_FALSE(env->lockExist(edges[0]));
    EXPECT_TRUE(env->outEdgeExist(edges[0]));
    EXPECT_TRUE(env->inEdgeExist(edges[0]));
}

/**
 * @brief add edge
 */
TEST_F(TossTest, test0_add_eg_then_getProp) {
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, edgeType_, 1);

    AddEdgeExecutor addExec(edges[0]);
    EXPECT_FALSE(env->lockExist(edges[0]));
    EXPECT_TRUE(env->outEdgeExist(edges[0]));
    EXPECT_TRUE(env->inEdgeExist(edges[0]));

    GetPropsExecutor exec(edges[0]);
    EXPECT_FALSE(env->lockExist(edges[0]));
    EXPECT_TRUE(env->outEdgeExist(edges[0]));
    EXPECT_TRUE(env->inEdgeExist(edges[0]));
    EXPECT_EQ(exec.data(), edges[0].props);
}


/**
 * @brief normal edge
 */
TEST_F(TossTest, test1_eg) {
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, edgeType_, 1);

    env->insertBiEdge(edges[0]);
    EXPECT_TRUE(env->inEdgeExist(edges[0]));
    EXPECT_TRUE(env->outEdgeExist(edges[0]));
    EXPECT_FALSE(env->lockExist(edges[0]));

    GetPropsExecutor exec(edges[0]);
    EXPECT_TRUE(env->inEdgeExist(edges[0]));
    EXPECT_TRUE(env->outEdgeExist(edges[0]));
    EXPECT_FALSE(env->lockExist(edges[0]));
    EXPECT_EQ(edges[0].props, exec.data());
}

TEST_F(TossTest, test2_glk_getNei) {
    LOG(INFO) << "b_=" << b_;
    auto lk = TossTestUtils::makeEdge(b_, edgeType_);

    auto lockKey = env->insertValidLock(lk);
    EXPECT_FALSE(env->inEdgeExist(lk));
    EXPECT_TRUE(env->outEdgeExist(lk));
    EXPECT_TRUE(env->lockExist(lk));

    GetNeighborsExecutor exec(lk);

    EXPECT_TRUE(env->inEdgeExist(lk));
    EXPECT_TRUE(env->outEdgeExist(lk));
    EXPECT_FALSE(env->lockExist(lk));

    UNUSED(exec);
}

TEST_F(TossTest, test2_glk_getProp) {
    LOG(INFO) << "b_=" << b_;
    auto lk = TossTestUtils::makeEdge(b_, edgeType_);

    auto lockKey = env->insertValidLock(lk);
    EXPECT_FALSE(env->inEdgeExist(lk));
    EXPECT_TRUE(env->outEdgeExist(lk));
    EXPECT_TRUE(env->lockExist(lk));

    GetPropsExecutor exec(lk);
    EXPECT_TRUE(env->inEdgeExist(lk));
    EXPECT_TRUE(env->outEdgeExist(lk));
    EXPECT_FALSE(env->lockExist(lk));
    EXPECT_EQ(lk.props, exec.data());

    UNUSED(exec);
}

/**
 * @brief good lock + edge
 */
TEST_F(TossTest, test3_glk_eg) {
    LOG(INFO) << "b_=" << b_;

    auto eg = TossTestUtils::makeEdge(b_, edgeType_);
    env->insertBiEdge(eg);
    EXPECT_FALSE(env->lockExist(eg));
    EXPECT_TRUE(env->outEdgeExist(eg));
    EXPECT_TRUE(env->inEdgeExist(eg));

    auto lk = TossTestUtils::makeTwinEdge(eg);
    env->insertValidLock(lk);
    EXPECT_TRUE(env->lockExist(lk));
    EXPECT_TRUE(env->outEdgeExist(lk));
    EXPECT_TRUE(env->inEdgeExist(lk));

    GetPropsExecutor exec(lk);
    EXPECT_FALSE(env->lockExist(lk));
    EXPECT_TRUE(env->outEdgeExist(lk));
    EXPECT_TRUE(env->inEdgeExist(lk));

    EXPECT_EQ(lk.props, exec.data());
}

/**
 * @brief bad lock
 */
TEST_F(TossTest, test4_blk) {
    LOG(INFO) << "b_=" << b_;

    auto eg = TossTestUtils::makeEdge(b_, edgeType_);

    env->insertInvalidLock(eg);
    EXPECT_TRUE(env->lockExist(eg));
    EXPECT_FALSE(env->outEdgeExist(eg));
    EXPECT_FALSE(env->inEdgeExist(eg));

    GetPropsExecutor exec(eg);
    EXPECT_FALSE(env->lockExist(eg));
    EXPECT_FALSE(env->outEdgeExist(eg));
    EXPECT_FALSE(env->inEdgeExist(eg));

    UNUSED(exec);
}

/**
 * @brief bad lock + edge
 */
TEST_F(TossTest, test5_blk_eg) {
    LOG(INFO) << "b_=" << b_;

    auto eg = TossTestUtils::makeEdge(b_, edgeType_);
    env->insertBiEdge(eg);
    EXPECT_FALSE(env->lockExist(eg));
    EXPECT_TRUE(env->outEdgeExist(eg));
    EXPECT_TRUE(env->inEdgeExist(eg));

    auto lk = TossTestUtils::makeTwinEdge(eg);
    env->insertInvalidLock(lk);
    EXPECT_TRUE(env->lockExist(lk));
    EXPECT_TRUE(env->outEdgeExist(lk));
    EXPECT_TRUE(env->inEdgeExist(lk));

    GetPropsExecutor exec(eg);
    EXPECT_FALSE(env->lockExist(eg));
    EXPECT_TRUE(env->outEdgeExist(eg));
    EXPECT_TRUE(env->inEdgeExist(eg));

    EXPECT_EQ(eg.props, exec.data());
}

/**
 * @brief edge1 + edge2
 */
TEST_F(TossTest, test6_e1_e2) {
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, edgeType_, 2);

    env->insertBiEdge(edges[0]);
    env->insertBiEdge(edges[1]);

    EXPECT_FALSE(env->lockExist(edges[0]));
    EXPECT_TRUE(env->outEdgeExist(edges[0]));
    EXPECT_TRUE(env->inEdgeExist(edges[0]));

    EXPECT_FALSE(env->lockExist(edges[1]));
    EXPECT_TRUE(env->outEdgeExist(edges[1]));
    EXPECT_TRUE(env->inEdgeExist(edges[1]));

    GetPropsExecutor exec0(edges[0]);
    GetPropsExecutor exec1(edges[1]);

    EXPECT_FALSE(env->lockExist(edges[0]));
    EXPECT_TRUE(env->outEdgeExist(edges[0]));
    EXPECT_TRUE(env->inEdgeExist(edges[0]));
    EXPECT_EQ(edges[0].props, exec0.data());

    EXPECT_FALSE(env->lockExist(edges[1]));
    EXPECT_TRUE(env->outEdgeExist(edges[1]));
    EXPECT_TRUE(env->inEdgeExist(edges[1]));
    EXPECT_EQ(edges[1].props, exec1.data());
}

/**
 * @brief edge1 + good lock2
 */
TEST_F(TossTest, test7_e1_glk2) {
    LOG(INFO) << "b_=" << b_;

    auto edges = TossTestUtils::makeNeighborEdges(b_, edgeType_, 2);
    auto& eg = edges[0];
    auto& lk = edges[1];

    env->insertBiEdge(eg);
    EXPECT_FALSE(env->lockExist(eg));
    EXPECT_TRUE(env->outEdgeExist(eg));
    EXPECT_TRUE(env->inEdgeExist(eg));

    env->insertValidLock(lk);
    EXPECT_TRUE(env->lockExist(lk));
    EXPECT_TRUE(env->outEdgeExist(lk));
    EXPECT_FALSE(env->inEdgeExist(lk));

    GetPropsExecutor exec1(eg);
    GetPropsExecutor exec2(lk);

    EXPECT_FALSE(env->lockExist(eg));
    EXPECT_TRUE(env->outEdgeExist(eg));
    EXPECT_TRUE(env->inEdgeExist(eg));
    EXPECT_EQ(eg.props, exec1.data());

    EXPECT_FALSE(env->lockExist(lk));
    EXPECT_TRUE(env->outEdgeExist(lk));
    EXPECT_TRUE(env->inEdgeExist(lk));
    EXPECT_EQ(lk.props, exec2.data());
}

/**
 * @brief edge1 + glk2 + edge2
 */
TEST_F(TossTest, test8_e1_glk2_eg2) {
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, edgeType_, 2);
    auto& e1 = edges[0];
    auto& e2 = edges[1];
    auto lk = TossTestUtils::makeTwinEdge(e2);

    env->insertBiEdge(e1);
    EXPECT_FALSE(env->lockExist(e1));
    EXPECT_TRUE(env->outEdgeExist(e1));
    EXPECT_TRUE(env->inEdgeExist(e1));

    env->insertBiEdge(e2);
    env->insertValidLock(lk);
    EXPECT_TRUE(env->lockExist(e2));
    EXPECT_TRUE(env->outEdgeExist(e2));
    EXPECT_TRUE(env->inEdgeExist(e2));

    GetNeighborsExecutor neiExec(e1);

    EXPECT_FALSE(env->lockExist(e1));
    EXPECT_TRUE(env->outEdgeExist(e1));
    EXPECT_TRUE(env->inEdgeExist(e1));

    EXPECT_FALSE(env->lockExist(e2));
    EXPECT_TRUE(env->outEdgeExist(e2));
    EXPECT_TRUE(env->inEdgeExist(e2));

    GetPropsExecutor exec1(e1);
    EXPECT_EQ(e1.props, exec1.data());

    GetPropsExecutor exec2(e2);
    EXPECT_EQ(lk.props, exec2.data());
}

// /**
//  * @brief edge1 + bad lock2
//  */
TEST_F(TossTest, test9_e1_blk2) {
    LOG(INFO) << "b_=" << b_;

    auto edges = TossTestUtils::makeNeighborEdges(b_, edgeType_, 2);
    auto& e1 = edges[0];
    auto& e2 = edges[1];

    env->insertBiEdge(e1);
    EXPECT_FALSE(env->lockExist(e1));
    EXPECT_TRUE(env->outEdgeExist(e1));
    EXPECT_TRUE(env->inEdgeExist(e1));

    env->insertInvalidLock(e2);
    EXPECT_TRUE(env->lockExist(e2));
    EXPECT_FALSE(env->outEdgeExist(e2));
    EXPECT_FALSE(env->inEdgeExist(e2));

    GetNeighborsExecutor exec(e1);

    EXPECT_FALSE(env->lockExist(e1));
    EXPECT_TRUE(env->outEdgeExist(e1));
    EXPECT_TRUE(env->inEdgeExist(e1));

    EXPECT_FALSE(env->lockExist(e2));
    EXPECT_FALSE(env->outEdgeExist(e2));
    EXPECT_FALSE(env->inEdgeExist(e2));

    GetPropsExecutor exec1(e1);
    EXPECT_EQ(e1.props, exec1.data());
}

/**
 * @brief edge(1) + bad lock(1) + edge(2)
 */
TEST_F(TossTest, test10_eg1_blk2_eg2) {
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, edgeType_, 2);
    auto& e1 = edges[0];
    auto& e2 = edges[1];
    auto lk = TossTestUtils::makeTwinEdge(e2);

    env->insertBiEdge(e1);
    EXPECT_FALSE(env->lockExist(e1));
    EXPECT_TRUE(env->outEdgeExist(e1));
    EXPECT_TRUE(env->inEdgeExist(e1));

    env->insertBiEdge(e2);
    env->insertInvalidLock(lk);
    EXPECT_TRUE(env->lockExist(e2));
    EXPECT_TRUE(env->outEdgeExist(e2));
    EXPECT_TRUE(env->inEdgeExist(e2));

    GetNeighborsExecutor neiExec(e1);

    EXPECT_FALSE(env->lockExist(e1));
    EXPECT_TRUE(env->outEdgeExist(e1));
    EXPECT_TRUE(env->inEdgeExist(e1));

    EXPECT_FALSE(env->lockExist(e2));
    EXPECT_TRUE(env->outEdgeExist(e2));
    EXPECT_TRUE(env->inEdgeExist(e2));

    GetPropsExecutor exec1(e1);
    EXPECT_EQ(e1.props, exec1.data());

    GetPropsExecutor exec2(e2);
    EXPECT_EQ(e2.props, exec2.data());
}

/**
 * @brief good lock(1) + edge(2)
 */
TEST_F(TossTest, test11_glk1_eg2) {
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, edgeType_, 2);
    auto& e1 = edges[0];
    auto& e2 = edges[1];
    // auto lk = TossTestUtils::makeTwinEdge(e2);

    env->insertValidLock(e1);
    EXPECT_TRUE(env->lockExist(e1));
    EXPECT_TRUE(env->outEdgeExist(e1));
    EXPECT_FALSE(env->inEdgeExist(e1));

    env->insertBiEdge(e2);
    // env->insertInvalidLock(lk);
    EXPECT_FALSE(env->lockExist(e2));
    EXPECT_TRUE(env->outEdgeExist(e2));
    EXPECT_TRUE(env->inEdgeExist(e2));

    GetNeighborsExecutor neiExec(e1);

    EXPECT_FALSE(env->lockExist(e1));
    EXPECT_TRUE(env->outEdgeExist(e1));
    EXPECT_TRUE(env->inEdgeExist(e1));

    EXPECT_FALSE(env->lockExist(e2));
    EXPECT_TRUE(env->outEdgeExist(e2));
    EXPECT_TRUE(env->inEdgeExist(e2));

    GetPropsExecutor exec1(e1);
    EXPECT_EQ(e1.props, exec1.data());

    GetPropsExecutor exec2(e2);
    EXPECT_EQ(e2.props, exec2.data());
}

/**
 * @brief bad lock(1) + edge(2)
 */
TEST_F(TossTest, test11_blk1_eg2) {
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, edgeType_, 2);
    auto& e1 = edges[0];
    auto& e2 = edges[1];

    env->insertInvalidLock(e1);
    EXPECT_TRUE(env->lockExist(e1));
    EXPECT_FALSE(env->outEdgeExist(e1));
    EXPECT_FALSE(env->inEdgeExist(e1));

    env->insertBiEdge(e2);
    EXPECT_FALSE(env->lockExist(e2));
    EXPECT_TRUE(env->outEdgeExist(e2));
    EXPECT_TRUE(env->inEdgeExist(e2));

    GetNeighborsExecutor neiExec(e1);

    EXPECT_FALSE(env->lockExist(e1));
    EXPECT_FALSE(env->outEdgeExist(e1));
    EXPECT_FALSE(env->inEdgeExist(e1));

    EXPECT_FALSE(env->lockExist(e2));
    EXPECT_TRUE(env->outEdgeExist(e2));
    EXPECT_TRUE(env->inEdgeExist(e2));

    // GetPropsExecutor exec1(e1);
    // EXPECT_EQ(e1.props, exec1.data());

    GetPropsExecutor exec2(e2);
    EXPECT_EQ(e2.props, exec2.data());
}

/**
 * @brief good lock(1) + good lock(2)
 */
TEST_F(TossTest, test12_glk1_glk2) {
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, edgeType_, 2);
    auto& e1 = edges[0];
    auto& e2 = edges[1];
    // auto lk = TossTestUtils::makeTwinEdge(e2);

    env->insertValidLock(e1);
    EXPECT_TRUE(env->lockExist(e1));
    EXPECT_TRUE(env->outEdgeExist(e1));
    EXPECT_FALSE(env->inEdgeExist(e1));

    env->insertValidLock(e2);
    // env->insertInvalidLock(lk);
    EXPECT_TRUE(env->lockExist(e2));
    EXPECT_TRUE(env->outEdgeExist(e2));
    EXPECT_FALSE(env->inEdgeExist(e2));

    GetNeighborsExecutor neiExec(e1);

    EXPECT_FALSE(env->lockExist(e1));
    EXPECT_TRUE(env->outEdgeExist(e1));
    EXPECT_TRUE(env->inEdgeExist(e1));

    EXPECT_FALSE(env->lockExist(e2));
    EXPECT_TRUE(env->outEdgeExist(e2));
    EXPECT_TRUE(env->inEdgeExist(e2));

    GetPropsExecutor exec1(e1);
    EXPECT_EQ(e1.props, exec1.data());

    GetPropsExecutor exec2(e2);
    EXPECT_EQ(e2.props, exec2.data());
}

/**
 * @brief good lock(1) + bad lock(2)
 */
TEST_F(TossTest, test12_glk1_blk2) {
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, edgeType_, 2);
    auto& e1 = edges[0];
    auto& e2 = edges[1];

    env->insertValidLock(e1);
    EXPECT_TRUE(env->lockExist(e1));
    EXPECT_TRUE(env->outEdgeExist(e1));
    EXPECT_FALSE(env->inEdgeExist(e1));

    env->insertInvalidLock(e2);
    EXPECT_TRUE(env->lockExist(e2));
    EXPECT_FALSE(env->outEdgeExist(e2));
    EXPECT_FALSE(env->inEdgeExist(e2));

    GetNeighborsExecutor neiExec(e1);

    EXPECT_FALSE(env->lockExist(e1));
    EXPECT_TRUE(env->outEdgeExist(e1));
    EXPECT_TRUE(env->inEdgeExist(e1));

    EXPECT_FALSE(env->lockExist(e2));
    EXPECT_FALSE(env->outEdgeExist(e2));
    EXPECT_FALSE(env->inEdgeExist(e2));

    GetPropsExecutor exec1(e1);
    EXPECT_EQ(e1.props, exec1.data());
}

/**
 * @brief bad lock(1) + bad lock(2)
 */
TEST_F(TossTest, test12_blk1_blk2) {
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, edgeType_, 2);
    auto& e1 = edges[0];
    auto& e2 = edges[1];

    env->insertInvalidLock(e1);
    EXPECT_TRUE(env->lockExist(e1));
    EXPECT_FALSE(env->outEdgeExist(e1));
    EXPECT_FALSE(env->inEdgeExist(e1));

    env->insertInvalidLock(e2);
    EXPECT_TRUE(env->lockExist(e2));
    EXPECT_FALSE(env->outEdgeExist(e2));
    EXPECT_FALSE(env->inEdgeExist(e2));

    GetNeighborsExecutor neiExec(e1);

    EXPECT_FALSE(env->lockExist(e1));
    EXPECT_FALSE(env->outEdgeExist(e1));
    EXPECT_FALSE(env->inEdgeExist(e1));

    EXPECT_FALSE(env->lockExist(e2));
    EXPECT_FALSE(env->outEdgeExist(e2));
    EXPECT_FALSE(env->inEdgeExist(e2));
}

/**
 * @brief good lock(1) + good lock(2) + edge(2)
 */
TEST_F(TossTest, test14_glk1_glk2_eg2) {
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, edgeType_, 2);
    auto& e1 = edges[0];
    auto& e2 = edges[1];
    auto glk2 = TossTestUtils::makeTwinEdge(e2);

    env->insertValidLock(e1);
    EXPECT_TRUE(env->lockExist(e1));
    EXPECT_TRUE(env->outEdgeExist(e1));
    EXPECT_FALSE(env->inEdgeExist(e1));

    env->insertBiEdge(e2);
    env->insertValidLock(glk2);
    EXPECT_TRUE(env->lockExist(e2));
    EXPECT_TRUE(env->outEdgeExist(e2));
    EXPECT_TRUE(env->inEdgeExist(e2));

    GetNeighborsExecutor neiExec(e1);

    EXPECT_FALSE(env->lockExist(e1));
    EXPECT_TRUE(env->outEdgeExist(e1));
    EXPECT_TRUE(env->inEdgeExist(e1));

    EXPECT_FALSE(env->lockExist(e2));
    EXPECT_TRUE(env->outEdgeExist(e2));
    EXPECT_TRUE(env->inEdgeExist(e2));

    GetPropsExecutor exec1(e1);
    EXPECT_EQ(e1.props, exec1.data());

    GetPropsExecutor exec2(e2);
    EXPECT_EQ(glk2.props, exec2.data());
}

/**
 * @brief good lock(1) + bad lock(2) + edge(2)
 */
TEST_F(TossTest, test14_glk1_blk2_eg2) {
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, edgeType_, 2);
    auto& e1 = edges[0];
    auto& e2 = edges[1];
    auto glk2 = TossTestUtils::makeTwinEdge(e2);

    env->insertValidLock(e1);
    EXPECT_TRUE(env->lockExist(e1));
    EXPECT_TRUE(env->outEdgeExist(e1));
    EXPECT_FALSE(env->inEdgeExist(e1));

    env->insertBiEdge(e2);
    env->insertInvalidLock(glk2);
    EXPECT_TRUE(env->lockExist(e2));
    EXPECT_TRUE(env->outEdgeExist(e2));
    EXPECT_TRUE(env->inEdgeExist(e2));

    GetNeighborsExecutor neiExec(e1);

    EXPECT_FALSE(env->lockExist(e1));
    EXPECT_TRUE(env->outEdgeExist(e1));
    EXPECT_TRUE(env->inEdgeExist(e1));

    EXPECT_FALSE(env->lockExist(e2));
    EXPECT_TRUE(env->outEdgeExist(e2));
    EXPECT_TRUE(env->inEdgeExist(e2));

    GetPropsExecutor exec1(e1);
    EXPECT_EQ(e1.props, exec1.data());

    GetPropsExecutor exec2(e2);
    EXPECT_EQ(e2.props, exec2.data());
}

/**
 * @brief bad lock(1) + bad lock(2) + edge(2)
 */
TEST_F(TossTest, test14_blk1_blk2_eg2) {
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, edgeType_, 2);
    auto& e1 = edges[0];
    auto& e2 = edges[1];
    auto glk2 = TossTestUtils::makeTwinEdge(e2);

    env->insertInvalidLock(e1);
    EXPECT_TRUE(env->lockExist(e1));
    EXPECT_FALSE(env->outEdgeExist(e1));
    EXPECT_FALSE(env->inEdgeExist(e1));

    env->insertBiEdge(e2);
    env->insertInvalidLock(glk2);
    EXPECT_TRUE(env->lockExist(e2));
    EXPECT_TRUE(env->outEdgeExist(e2));
    EXPECT_TRUE(env->inEdgeExist(e2));

    GetNeighborsExecutor neiExec(e1);

    EXPECT_FALSE(env->lockExist(e1));
    EXPECT_FALSE(env->outEdgeExist(e1));
    EXPECT_FALSE(env->inEdgeExist(e1));

    EXPECT_FALSE(env->lockExist(e2));
    EXPECT_TRUE(env->outEdgeExist(e2));
    EXPECT_TRUE(env->inEdgeExist(e2));

    GetPropsExecutor exec2(e2);
    EXPECT_EQ(e2.props, exec2.data());
}

/**
 * @brief  good lock(1) + edge(1) + good lock(2) + edge(2)
 */
TEST_F(TossTest, test16_glk1_eg1_glk2_eg2) {
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, edgeType_, 2);
    auto& e1 = edges[0];
    auto& e2 = edges[1];
    auto lk1 = TossTestUtils::makeTwinEdge(e1);
    auto lk2 = TossTestUtils::makeTwinEdge(e2);


    env->insertBiEdge(e1);
    env->insertValidLock(lk1);
    EXPECT_TRUE(env->lockExist(e1));
    EXPECT_TRUE(env->outEdgeExist(e1));
    EXPECT_TRUE(env->inEdgeExist(e1));

    env->insertBiEdge(e2);
    env->insertValidLock(lk2);
    EXPECT_TRUE(env->lockExist(e2));
    EXPECT_TRUE(env->outEdgeExist(e2));
    EXPECT_TRUE(env->inEdgeExist(e2));

    GetNeighborsExecutor neiExec(e1);

    EXPECT_FALSE(env->lockExist(e1));
    EXPECT_TRUE(env->outEdgeExist(e1));
    EXPECT_TRUE(env->inEdgeExist(e1));

    EXPECT_FALSE(env->lockExist(e2));
    EXPECT_TRUE(env->outEdgeExist(e2));
    EXPECT_TRUE(env->inEdgeExist(e2));

    GetPropsExecutor exec1(e1);
    EXPECT_EQ(lk1.props, exec1.data());

    GetPropsExecutor exec2(e2);
    EXPECT_EQ(lk2.props, exec2.data());
}

/**
 * @brief  good lock(1) + edge(1) + bad lock(2) + edge(2)
 */
TEST_F(TossTest, test16_glk1_eg1_blk2_eg2) {
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, edgeType_, 2);
    auto& e1 = edges[0];
    auto& e2 = edges[1];
    auto lk1 = TossTestUtils::makeTwinEdge(e1);
    auto lk2 = TossTestUtils::makeTwinEdge(e2);


    env->insertBiEdge(e1);
    env->insertValidLock(lk1);
    EXPECT_TRUE(env->lockExist(e1));
    EXPECT_TRUE(env->outEdgeExist(e1));
    EXPECT_TRUE(env->inEdgeExist(e1));

    env->insertBiEdge(e2);
    env->insertInvalidLock(lk2);
    EXPECT_TRUE(env->lockExist(e2));
    EXPECT_TRUE(env->outEdgeExist(e2));
    EXPECT_TRUE(env->inEdgeExist(e2));

    GetNeighborsExecutor neiExec(e1);

    EXPECT_FALSE(env->lockExist(e1));
    EXPECT_TRUE(env->outEdgeExist(e1));
    EXPECT_TRUE(env->inEdgeExist(e1));

    EXPECT_FALSE(env->lockExist(e2));
    EXPECT_TRUE(env->outEdgeExist(e2));
    EXPECT_TRUE(env->inEdgeExist(e2));

    GetPropsExecutor exec1(e1);
    EXPECT_EQ(lk1.props, exec1.data());

    GetPropsExecutor exec2(e2);
    EXPECT_EQ(e2.props, exec2.data());
}

/**
 * @brief bad lock(1) + edge(1) + good lock(2) + edge(2)
 */
TEST_F(TossTest, test16_blk1_eg1_glk2_eg2) {
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, edgeType_, 2);
    auto& e1 = edges[0];
    auto& e2 = edges[1];
    auto lk1 = TossTestUtils::makeTwinEdge(e1);
    auto lk2 = TossTestUtils::makeTwinEdge(e2);


    env->insertBiEdge(e1);
    env->insertInvalidLock(lk1);
    EXPECT_TRUE(env->lockExist(e1));
    EXPECT_TRUE(env->outEdgeExist(e1));
    EXPECT_TRUE(env->inEdgeExist(e1));

    env->insertBiEdge(e2);
    env->insertValidLock(lk2);
    EXPECT_TRUE(env->lockExist(e2));
    EXPECT_TRUE(env->outEdgeExist(e2));
    EXPECT_TRUE(env->inEdgeExist(e2));

    GetNeighborsExecutor neiExec(e1);

    EXPECT_FALSE(env->lockExist(e1));
    EXPECT_TRUE(env->outEdgeExist(e1));
    EXPECT_TRUE(env->inEdgeExist(e1));

    EXPECT_FALSE(env->lockExist(e2));
    EXPECT_TRUE(env->outEdgeExist(e2));
    EXPECT_TRUE(env->inEdgeExist(e2));

    GetPropsExecutor exec1(e1);
    EXPECT_EQ(e1.props, exec1.data());

    GetPropsExecutor exec2(e2);
    EXPECT_EQ(lk2.props, exec2.data());
}

/**
 * @brief bad lock(1) + edge(1) + bad lock(2) + edge(2)
 */
TEST_F(TossTest, test16_blk1_eg1_blk2_eg2) {
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, edgeType_, 2);
    auto& e1 = edges[0];
    auto& e2 = edges[1];
    auto lk1 = TossTestUtils::makeTwinEdge(e1);
    auto lk2 = TossTestUtils::makeTwinEdge(e2);


    env->insertBiEdge(e1);
    env->insertInvalidLock(lk1);
    EXPECT_TRUE(env->lockExist(e1));
    EXPECT_TRUE(env->outEdgeExist(e1));
    EXPECT_TRUE(env->inEdgeExist(e1));

    env->insertBiEdge(e2);
    env->insertInvalidLock(lk2);
    EXPECT_TRUE(env->lockExist(e2));
    EXPECT_TRUE(env->outEdgeExist(e2));
    EXPECT_TRUE(env->inEdgeExist(e2));

    GetNeighborsExecutor neiExec(e1);

    EXPECT_FALSE(env->lockExist(e1));
    EXPECT_TRUE(env->outEdgeExist(e1));
    EXPECT_TRUE(env->inEdgeExist(e1));

    EXPECT_FALSE(env->lockExist(e2));
    EXPECT_TRUE(env->outEdgeExist(e2));
    EXPECT_TRUE(env->inEdgeExist(e2));

    GetPropsExecutor exec1(e1);
    EXPECT_EQ(e1.props, exec1.data());

    GetPropsExecutor exec2(e2);
    EXPECT_EQ(e2.props, exec2.data());
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
    FLAGS_heartbeat_interval_secs = 1;

    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, false);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
