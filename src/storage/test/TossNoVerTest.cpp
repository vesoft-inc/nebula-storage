 /* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include "TossEnvironment.h"
#include "TossTestExecutor.h"
#include "folly/String.h"
#define LOG_FMT(...) LOG(INFO) << folly::sformat(__VA_ARGS__)

namespace nebula {
namespace storage {

constexpr bool kUseToss = true;
constexpr bool kNotUseToss = false;

const std::string kMetaName = "127.0.0.1";  // NOLINT
constexpr int32_t kMetaPort = 6500;
const std::string kSpaceName = "test";   // NOLINT
const std::string kEdgeName = "test_edge";   // NOLINT
constexpr int32_t kPart = 5;
constexpr int32_t kReplica = 3;

static int32_t b_ = 9527;
static int32_t gap = 1000;

class TossTest : public ::testing::Test {
public:
    static void SetUpTestCase() {
        env = TossEnvironment::getInstance();
        ASSERT_TRUE(env->connectToMeta(kMetaName, kMetaPort));

        std::vector<meta::cpp2::PropertyType> colTypes{meta::cpp2::PropertyType::INT64,
                                                       meta::cpp2::PropertyType::STRING};

        std::string name1("test1");
        auto intVid = meta::cpp2::PropertyType::INT64;
        TossTest::space1 = std::make_unique<TestSpace>(
            env, name1, kPart, kReplica, intVid, kEdgeName, colTypes);

        auto stringVid = meta::cpp2::PropertyType::FIXED_STRING;
        std::string name2("test2");
        TossTest::space2 = std::make_unique<TestSpace>(
            env, name2, kPart, kReplica, stringVid, kEdgeName, colTypes);
    }

    static void TearDownTestCase() {}

    void SetUp() override {
        b_ /= gap;
        ++b_;
        b_ *= gap;

        s1 = TossTest::space1.get();
        s2 = TossTest::space2.get();
    }

    void TearDown() override {}

protected:
    static TossEnvironment* env;
    TestSpace* s1{nullptr};
    TestSpace* s2{nullptr};
    static std::unique_ptr<TestSpace> space1;
    static std::unique_ptr<TestSpace> space2;
};

TossEnvironment* TossTest::env = nullptr;
std::unique_ptr<TestSpace> TossTest::space1;
std::unique_ptr<TestSpace> TossTest::space2;

/**
 * test case naming rules
 * eg  => edge
 * glk => valid lock
 * blk => invalid lock
 */
TEST_F(TossTest, utils_test) {
    auto edges = TossTestUtils::makeNeighborEdges(b_, s1->getEdgeType(), 1);
    LOG(INFO) << "edges.size() = " << edges.size();
    for (auto& e : edges) {
        LOG(INFO) << "in-edge key: " << folly::hexlify(s1->strInEdgeKey(edges[0].key));
        LOG(INFO) << "out-edge key: " << folly::hexlify(s1->strOutEdgeKey(edges[0].key));
        LOG(INFO) << "lock key: " << folly::hexlify(s1->strLockKey(edges[0].key));
        LOG(INFO) << "e.props.size() = " << e.props.size();
        for (auto& prop : e.props) {
            LOG(INFO) << prop.toString();
        }
    }
    auto lockKey = s1->strLockKey(edges[0].key);

    EXPECT_FALSE(NebulaKeyUtils::isEdge(s1->vIdLen_, lockKey));
    EXPECT_EQ(lockKey.back(), 0);
    EXPECT_TRUE(NebulaKeyUtils::isLock(s1->vIdLen_, lockKey));
}

TEST_F(TossTest, empty_db) {
    auto eg = TossTestUtils::makeEdge(b_, s1->edgeType_);
    GetNeighborsExecutor exec(s1, eg);

    EXPECT_FALSE(s1->lockExist(eg));
    EXPECT_FALSE(s1->outEdgeExist(eg));
    EXPECT_FALSE(s1->inEdgeExist(eg));
}

/**
 * @brief add edge
 */
TEST_F(TossTest, test0_add_eg) {
    LOG(INFO) << "b_=" << b_;
    auto eg = TossTestUtils::makeEdge(b_, s1->edgeType_);

    AddEdgeExecutor exec(s1, eg);
    EXPECT_FALSE(s1->lockExist(eg));
    EXPECT_TRUE(s1->outEdgeExist(eg));
    EXPECT_TRUE(s1->inEdgeExist(eg));
}

/**
 * @brief add edge
 */
TEST_F(TossTest, test0_add_eg_twice) {
    LOG(INFO) << "b_=" << b_;
    auto eg1 = TossTestUtils::makeEdge(b_, s1->edgeType_);

    AddEdgeExecutor exec1(s1, eg1);
    EXPECT_FALSE(s1->lockExist(eg1));
    EXPECT_TRUE(s1->outEdgeExist(eg1));
    EXPECT_TRUE(s1->inEdgeExist(eg1));

    auto eg2 = TossTestUtils::makeTwinEdge(eg1);
    AddEdgeExecutor exec2(s1, eg2);
    EXPECT_TRUE(exec2.ok());
}

/**
 * @brief add edge
 */
TEST_F(TossTest, test0_add_eg_then_getProp) {
    LOG(INFO) << "b_=" << b_;
    auto eg = TossTestUtils::makeEdge(b_, s1->edgeType_);

    AddEdgeExecutor addExec(s1, eg);
    EXPECT_FALSE(s1->lockExist(eg));
    EXPECT_TRUE(s1->outEdgeExist(eg));
    EXPECT_TRUE(s1->inEdgeExist(eg));

    GetPropsExecutor exec(s1, eg);
    EXPECT_FALSE(s1->lockExist(eg));
    EXPECT_TRUE(s1->outEdgeExist(eg));
    EXPECT_TRUE(s1->inEdgeExist(eg));
    EXPECT_EQ(exec.data(), eg.props);
}


/**
 * @brief normal edge
 */
TEST_F(TossTest, test1_eg) {
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, s1->edgeType_, 1);

    s1->insertBiEdge(edges[0]);
    EXPECT_TRUE(s1->inEdgeExist(edges[0]));
    EXPECT_TRUE(s1->outEdgeExist(edges[0]));
    EXPECT_FALSE(s1->lockExist(edges[0]));

    GetPropsExecutor exec(s1, edges[0]);
    EXPECT_TRUE(s1->inEdgeExist(edges[0]));
    EXPECT_TRUE(s1->outEdgeExist(edges[0]));
    EXPECT_FALSE(s1->lockExist(edges[0]));
    EXPECT_EQ(edges[0].props, exec.data());
}

TEST_F(TossTest, test2_glk_getNei) {
    LOG(INFO) << "b_=" << b_;
    auto lk = TossTestUtils::makeEdge(b_, s1->edgeType_);

    auto lockKey = s1->insertValidLock(lk);
    EXPECT_FALSE(s1->inEdgeExist(lk));
    EXPECT_TRUE(s1->outEdgeExist(lk));
    EXPECT_TRUE(s1->lockExist(lk));

    GetNeighborsExecutor exec(s1, lk);

    EXPECT_TRUE(s1->inEdgeExist(lk));
    EXPECT_TRUE(s1->outEdgeExist(lk));
    EXPECT_FALSE(s1->lockExist(lk));

    UNUSED(exec);
}

TEST_F(TossTest, test2_glk_getProp) {
    LOG(INFO) << "b_=" << b_;
    auto lk = TossTestUtils::makeEdge(b_, s1->edgeType_);

    auto lockKey = s1->insertValidLock(lk);
    EXPECT_FALSE(s1->inEdgeExist(lk));
    EXPECT_TRUE(s1->outEdgeExist(lk));
    EXPECT_TRUE(s1->lockExist(lk));

    GetPropsExecutor exec(s1, lk);
    EXPECT_TRUE(s1->inEdgeExist(lk));
    EXPECT_TRUE(s1->outEdgeExist(lk));
    EXPECT_FALSE(s1->lockExist(lk));
    EXPECT_EQ(lk.props, exec.data());

    UNUSED(exec);
}

/**
 * @brief good lock + edge
 */
TEST_F(TossTest, test3_glk_eg) {
    LOG(INFO) << "b_=" << b_;

    auto eg = TossTestUtils::makeEdge(b_, s1->edgeType_);
    s1->insertBiEdge(eg);
    EXPECT_FALSE(s1->lockExist(eg));
    EXPECT_TRUE(s1->outEdgeExist(eg));
    EXPECT_TRUE(s1->inEdgeExist(eg));

    auto lk = TossTestUtils::makeTwinEdge(eg);
    s1->insertValidLock(lk);
    EXPECT_TRUE(s1->lockExist(lk));
    EXPECT_TRUE(s1->outEdgeExist(lk));
    EXPECT_TRUE(s1->inEdgeExist(lk));

    GetPropsExecutor exec(s1, lk);
    EXPECT_FALSE(s1->lockExist(lk));
    EXPECT_TRUE(s1->outEdgeExist(lk));
    EXPECT_TRUE(s1->inEdgeExist(lk));

    EXPECT_EQ(lk.props, exec.data());
}

/**
 * @brief bad lock
 */
TEST_F(TossTest, test4_blk) {
    LOG(INFO) << "b_=" << b_;

    auto eg = TossTestUtils::makeEdge(b_, s1->edgeType_);

    s1->insertInvalidLock(eg);
    EXPECT_TRUE(s1->lockExist(eg));
    EXPECT_FALSE(s1->outEdgeExist(eg));
    EXPECT_FALSE(s1->inEdgeExist(eg));

    GetPropsExecutor exec(s1, eg);
    EXPECT_FALSE(s1->lockExist(eg));
    EXPECT_FALSE(s1->outEdgeExist(eg));
    EXPECT_FALSE(s1->inEdgeExist(eg));

    UNUSED(exec);
}

/**
 * @brief bad lock + edge
 */
TEST_F(TossTest, test5_blk_eg) {
    LOG(INFO) << "b_=" << b_;

    auto eg = TossTestUtils::makeEdge(b_, s1->edgeType_);
    s1->insertBiEdge(eg);
    EXPECT_FALSE(s1->lockExist(eg));
    EXPECT_TRUE(s1->outEdgeExist(eg));
    EXPECT_TRUE(s1->inEdgeExist(eg));

    auto lk = TossTestUtils::makeTwinEdge(eg);
    s1->insertInvalidLock(lk);
    EXPECT_TRUE(s1->lockExist(lk));
    EXPECT_TRUE(s1->outEdgeExist(lk));
    EXPECT_TRUE(s1->inEdgeExist(lk));

    GetPropsExecutor exec(s1, eg);
    EXPECT_FALSE(s1->lockExist(eg));
    EXPECT_TRUE(s1->outEdgeExist(eg));
    EXPECT_TRUE(s1->inEdgeExist(eg));

    EXPECT_EQ(eg.props, exec.data());
}

/**
 * @brief edge1 + edge2
 */
TEST_F(TossTest, test6_e1_e2) {
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, s1->edgeType_, 2);

    s1->insertBiEdge(edges[0]);
    s1->insertBiEdge(edges[1]);

    EXPECT_FALSE(s1->lockExist(edges[0]));
    EXPECT_TRUE(s1->outEdgeExist(edges[0]));
    EXPECT_TRUE(s1->inEdgeExist(edges[0]));

    EXPECT_FALSE(s1->lockExist(edges[1]));
    EXPECT_TRUE(s1->outEdgeExist(edges[1]));
    EXPECT_TRUE(s1->inEdgeExist(edges[1]));

    GetPropsExecutor exec0(s1, edges[0]);
    GetPropsExecutor exec1(s1, edges[1]);

    EXPECT_FALSE(s1->lockExist(edges[0]));
    EXPECT_TRUE(s1->outEdgeExist(edges[0]));
    EXPECT_TRUE(s1->inEdgeExist(edges[0]));
    EXPECT_EQ(edges[0].props, exec0.data());

    EXPECT_FALSE(s1->lockExist(edges[1]));
    EXPECT_TRUE(s1->outEdgeExist(edges[1]));
    EXPECT_TRUE(s1->inEdgeExist(edges[1]));
    EXPECT_EQ(edges[1].props, exec1.data());
}

/**
 * @brief edge1 + good lock2
 */
TEST_F(TossTest, test7_e1_glk2) {
    LOG(INFO) << "b_=" << b_;

    auto edges = TossTestUtils::makeNeighborEdges(b_, s1->edgeType_, 2);
    auto& eg = edges[0];
    auto& lk = edges[1];

    s1->insertBiEdge(eg);
    EXPECT_FALSE(s1->lockExist(eg));
    EXPECT_TRUE(s1->outEdgeExist(eg));
    EXPECT_TRUE(s1->inEdgeExist(eg));

    s1->insertValidLock(lk);
    EXPECT_TRUE(s1->lockExist(lk));
    EXPECT_TRUE(s1->outEdgeExist(lk));
    EXPECT_FALSE(s1->inEdgeExist(lk));

    GetPropsExecutor exec1(s1, eg);
    GetPropsExecutor exec2(s1, lk);

    EXPECT_FALSE(s1->lockExist(eg));
    EXPECT_TRUE(s1->outEdgeExist(eg));
    EXPECT_TRUE(s1->inEdgeExist(eg));
    EXPECT_EQ(eg.props, exec1.data());

    EXPECT_FALSE(s1->lockExist(lk));
    EXPECT_TRUE(s1->outEdgeExist(lk));
    EXPECT_TRUE(s1->inEdgeExist(lk));
    EXPECT_EQ(lk.props, exec2.data());
}

/**
 * @brief edge1 + glk2 + edge2
 */
TEST_F(TossTest, test8_e1_glk2_eg2) {
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, s1->edgeType_, 2);
    auto& e1 = edges[0];
    auto& e2 = edges[1];
    auto lk = TossTestUtils::makeTwinEdge(e2);

    s1->insertBiEdge(e1);
    EXPECT_FALSE(s1->lockExist(e1));
    EXPECT_TRUE(s1->outEdgeExist(e1));
    EXPECT_TRUE(s1->inEdgeExist(e1));

    s1->insertBiEdge(e2);
    s1->insertValidLock(lk);
    EXPECT_TRUE(s1->lockExist(e2));
    EXPECT_TRUE(s1->outEdgeExist(e2));
    EXPECT_TRUE(s1->inEdgeExist(e2));

    GetNeighborsExecutor neiExec(s1, e1);

    EXPECT_FALSE(s1->lockExist(e1));
    EXPECT_TRUE(s1->outEdgeExist(e1));
    EXPECT_TRUE(s1->inEdgeExist(e1));

    EXPECT_FALSE(s1->lockExist(e2));
    EXPECT_TRUE(s1->outEdgeExist(e2));
    EXPECT_TRUE(s1->inEdgeExist(e2));

    GetPropsExecutor exec1(s1, e1);
    EXPECT_EQ(e1.props, exec1.data());

    GetPropsExecutor exec2(s1, e2);
    EXPECT_EQ(lk.props, exec2.data());
}

// /**
//  * @brief edge1 + bad lock2
//  */
TEST_F(TossTest, test9_e1_blk2) {
    LOG(INFO) << "b_=" << b_;

    auto edges = TossTestUtils::makeNeighborEdges(b_, s1->edgeType_, 2);
    auto& e1 = edges[0];
    auto& e2 = edges[1];

    s1->insertBiEdge(e1);
    EXPECT_FALSE(s1->lockExist(e1));
    EXPECT_TRUE(s1->outEdgeExist(e1));
    EXPECT_TRUE(s1->inEdgeExist(e1));

    s1->insertInvalidLock(e2);
    EXPECT_TRUE(s1->lockExist(e2));
    EXPECT_FALSE(s1->outEdgeExist(e2));
    EXPECT_FALSE(s1->inEdgeExist(e2));

    GetNeighborsExecutor exec(s1, e1);

    EXPECT_FALSE(s1->lockExist(e1));
    EXPECT_TRUE(s1->outEdgeExist(e1));
    EXPECT_TRUE(s1->inEdgeExist(e1));

    EXPECT_FALSE(s1->lockExist(e2));
    EXPECT_FALSE(s1->outEdgeExist(e2));
    EXPECT_FALSE(s1->inEdgeExist(e2));

    GetPropsExecutor exec1(s1, e1);
    EXPECT_EQ(e1.props, exec1.data());
}

/**
 * @brief edge(1) + bad lock(1) + edge(2)
 */
TEST_F(TossTest, test10_eg1_blk2_eg2) {
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, s1->edgeType_, 2);
    auto& e1 = edges[0];
    auto& e2 = edges[1];
    auto lk = TossTestUtils::makeTwinEdge(e2);

    s1->insertBiEdge(e1);
    EXPECT_FALSE(s1->lockExist(e1));
    EXPECT_TRUE(s1->outEdgeExist(e1));
    EXPECT_TRUE(s1->inEdgeExist(e1));

    s1->insertBiEdge(e2);
    s1->insertInvalidLock(lk);
    EXPECT_TRUE(s1->lockExist(e2));
    EXPECT_TRUE(s1->outEdgeExist(e2));
    EXPECT_TRUE(s1->inEdgeExist(e2));

    GetNeighborsExecutor neiExec(s1, e1);

    EXPECT_FALSE(s1->lockExist(e1));
    EXPECT_TRUE(s1->outEdgeExist(e1));
    EXPECT_TRUE(s1->inEdgeExist(e1));

    EXPECT_FALSE(s1->lockExist(e2));
    EXPECT_TRUE(s1->outEdgeExist(e2));
    EXPECT_TRUE(s1->inEdgeExist(e2));

    GetPropsExecutor exec1(s1, e1);
    EXPECT_EQ(e1.props, exec1.data());

    GetPropsExecutor exec2(s1, e2);
    EXPECT_EQ(e2.props, exec2.data());
}

/**
 * @brief good lock(1) + edge(2)
 */
TEST_F(TossTest, test11_glk1_eg2) {
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, s1->edgeType_, 2);
    auto& e1 = edges[0];
    auto& e2 = edges[1];
    // auto lk = TossTestUtils::makeTwinEdge(e2);

    s1->insertValidLock(e1);
    EXPECT_TRUE(s1->lockExist(e1));
    EXPECT_TRUE(s1->outEdgeExist(e1));
    EXPECT_FALSE(s1->inEdgeExist(e1));

    s1->insertBiEdge(e2);
    // s1->insertInvalidLock(lk);
    EXPECT_FALSE(s1->lockExist(e2));
    EXPECT_TRUE(s1->outEdgeExist(e2));
    EXPECT_TRUE(s1->inEdgeExist(e2));

    GetNeighborsExecutor neiExec(s1, e1);
    EXPECT_FALSE(s1->lockExist(e1));
    EXPECT_TRUE(s1->outEdgeExist(e1));
    EXPECT_TRUE(s1->inEdgeExist(e1));

    EXPECT_FALSE(s1->lockExist(e2));
    EXPECT_TRUE(s1->outEdgeExist(e2));
    EXPECT_TRUE(s1->inEdgeExist(e2));

    GetPropsExecutor exec1(s1, e1);
    EXPECT_EQ(e1.props, exec1.data());

    GetPropsExecutor exec2(s1, e2);
    EXPECT_EQ(e2.props, exec2.data());
}

/**
 * @brief bad lock(1) + edge(2)
 */
TEST_F(TossTest, test11_blk1_eg2) {
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, s1->edgeType_, 2);
    auto& e1 = edges[0];
    auto& e2 = edges[1];

    s1->insertInvalidLock(e1);
    EXPECT_TRUE(s1->lockExist(e1));
    EXPECT_FALSE(s1->outEdgeExist(e1));
    EXPECT_FALSE(s1->inEdgeExist(e1));

    s1->insertBiEdge(e2);
    EXPECT_FALSE(s1->lockExist(e2));
    EXPECT_TRUE(s1->outEdgeExist(e2));
    EXPECT_TRUE(s1->inEdgeExist(e2));

    GetNeighborsExecutor neiExec(s1, e1);

    EXPECT_FALSE(s1->lockExist(e1));
    EXPECT_FALSE(s1->outEdgeExist(e1));
    EXPECT_FALSE(s1->inEdgeExist(e1));

    EXPECT_FALSE(s1->lockExist(e2));
    EXPECT_TRUE(s1->outEdgeExist(e2));
    EXPECT_TRUE(s1->inEdgeExist(e2));

    GetPropsExecutor exec2(s1, e2);
    EXPECT_EQ(e2.props, exec2.data());
}

/**
 * @brief good lock(1) + good lock(2)
 */
TEST_F(TossTest, test12_glk1_glk2) {
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, s1->edgeType_, 2);
    auto& e1 = edges[0];
    auto& e2 = edges[1];
    // auto lk = TossTestUtils::makeTwinEdge(e2);

    s1->insertValidLock(e1);
    EXPECT_TRUE(s1->lockExist(e1));
    EXPECT_TRUE(s1->outEdgeExist(e1));
    EXPECT_FALSE(s1->inEdgeExist(e1));

    s1->insertValidLock(e2);
    // s1->insertInvalidLock(lk);
    EXPECT_TRUE(s1->lockExist(e2));
    EXPECT_TRUE(s1->outEdgeExist(e2));
    EXPECT_FALSE(s1->inEdgeExist(e2));

    GetNeighborsExecutor neiExec(s1, e1);

    EXPECT_FALSE(s1->lockExist(e1));
    EXPECT_TRUE(s1->outEdgeExist(e1));
    EXPECT_TRUE(s1->inEdgeExist(e1));

    EXPECT_FALSE(s1->lockExist(e2));
    EXPECT_TRUE(s1->outEdgeExist(e2));
    EXPECT_TRUE(s1->inEdgeExist(e2));

    GetPropsExecutor exec1(s1, e1);
    EXPECT_EQ(e1.props, exec1.data());

    GetPropsExecutor exec2(s1, e2);
    EXPECT_EQ(e2.props, exec2.data());
}

/**
 * @brief good lock(1) + bad lock(2)
 */
TEST_F(TossTest, test12_glk1_blk2) {
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, s1->edgeType_, 2);
    auto& e1 = edges[0];
    auto& e2 = edges[1];

    s1->insertValidLock(e1);
    EXPECT_TRUE(s1->lockExist(e1));
    EXPECT_TRUE(s1->outEdgeExist(e1));
    EXPECT_FALSE(s1->inEdgeExist(e1));

    s1->insertInvalidLock(e2);
    EXPECT_TRUE(s1->lockExist(e2));
    EXPECT_FALSE(s1->outEdgeExist(e2));
    EXPECT_FALSE(s1->inEdgeExist(e2));

    GetNeighborsExecutor neiExec(s1, e1);

    EXPECT_FALSE(s1->lockExist(e1));
    EXPECT_TRUE(s1->outEdgeExist(e1));
    EXPECT_TRUE(s1->inEdgeExist(e1));

    EXPECT_FALSE(s1->lockExist(e2));
    EXPECT_FALSE(s1->outEdgeExist(e2));
    EXPECT_FALSE(s1->inEdgeExist(e2));

    GetPropsExecutor exec1(s1, e1);
    EXPECT_EQ(e1.props, exec1.data());
}

/**
 * @brief bad lock(1) + bad lock(2)
 */
TEST_F(TossTest, test12_blk1_blk2) {
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, s1->edgeType_, 2);
    auto& e1 = edges[0];
    auto& e2 = edges[1];

    s1->insertInvalidLock(e1);
    EXPECT_TRUE(s1->lockExist(e1));
    EXPECT_FALSE(s1->outEdgeExist(e1));
    EXPECT_FALSE(s1->inEdgeExist(e1));

    s1->insertInvalidLock(e2);
    EXPECT_TRUE(s1->lockExist(e2));
    EXPECT_FALSE(s1->outEdgeExist(e2));
    EXPECT_FALSE(s1->inEdgeExist(e2));

    GetNeighborsExecutor neiExec(s1, e1);

    EXPECT_FALSE(s1->lockExist(e1));
    EXPECT_FALSE(s1->outEdgeExist(e1));
    EXPECT_FALSE(s1->inEdgeExist(e1));

    EXPECT_FALSE(s1->lockExist(e2));
    EXPECT_FALSE(s1->outEdgeExist(e2));
    EXPECT_FALSE(s1->inEdgeExist(e2));
}

/**
 * @brief good lock(1) + good lock(2) + edge(2)
 */
TEST_F(TossTest, test14_glk1_glk2_eg2) {
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, s1->edgeType_, 2);
    auto& e1 = edges[0];
    auto& e2 = edges[1];
    auto glk2 = TossTestUtils::makeTwinEdge(e2);

    s1->insertValidLock(e1);
    EXPECT_TRUE(s1->lockExist(e1));
    EXPECT_TRUE(s1->outEdgeExist(e1));
    EXPECT_FALSE(s1->inEdgeExist(e1));

    s1->insertBiEdge(e2);
    s1->insertValidLock(glk2);
    EXPECT_TRUE(s1->lockExist(e2));
    EXPECT_TRUE(s1->outEdgeExist(e2));
    EXPECT_TRUE(s1->inEdgeExist(e2));

    GetNeighborsExecutor neiExec(s1, e1);

    EXPECT_FALSE(s1->lockExist(e1));
    EXPECT_TRUE(s1->outEdgeExist(e1));
    EXPECT_TRUE(s1->inEdgeExist(e1));

    EXPECT_FALSE(s1->lockExist(e2));
    EXPECT_TRUE(s1->outEdgeExist(e2));
    EXPECT_TRUE(s1->inEdgeExist(e2));

    GetPropsExecutor exec1(s1, e1);
    EXPECT_EQ(e1.props, exec1.data());

    GetPropsExecutor exec2(s1, e2);
    EXPECT_EQ(glk2.props, exec2.data());
}

/**
 * @brief good lock(1) + bad lock(2) + edge(2)
 */
TEST_F(TossTest, test14_glk1_blk2_eg2) {
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, s1->edgeType_, 2);
    auto& e1 = edges[0];
    auto& e2 = edges[1];
    auto glk2 = TossTestUtils::makeTwinEdge(e2);

    s1->insertValidLock(e1);
    EXPECT_TRUE(s1->lockExist(e1));
    EXPECT_TRUE(s1->outEdgeExist(e1));
    EXPECT_FALSE(s1->inEdgeExist(e1));

    s1->insertBiEdge(e2);
    s1->insertInvalidLock(glk2);
    EXPECT_TRUE(s1->lockExist(e2));
    EXPECT_TRUE(s1->outEdgeExist(e2));
    EXPECT_TRUE(s1->inEdgeExist(e2));

    GetNeighborsExecutor neiExec(s1, e1);

    EXPECT_FALSE(s1->lockExist(e1));
    EXPECT_TRUE(s1->outEdgeExist(e1));
    EXPECT_TRUE(s1->inEdgeExist(e1));

    EXPECT_FALSE(s1->lockExist(e2));
    EXPECT_TRUE(s1->outEdgeExist(e2));
    EXPECT_TRUE(s1->inEdgeExist(e2));

    GetPropsExecutor exec1(s1, e1);
    EXPECT_EQ(e1.props, exec1.data());

    GetPropsExecutor exec2(s1, e2);
    EXPECT_EQ(e2.props, exec2.data());
}

/**
 * @brief bad lock(1) + bad lock(2) + edge(2)
 */
TEST_F(TossTest, test14_blk1_blk2_eg2) {
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, s1->edgeType_, 2);
    auto& e1 = edges[0];
    auto& e2 = edges[1];
    auto glk2 = TossTestUtils::makeTwinEdge(e2);

    s1->insertInvalidLock(e1);
    EXPECT_TRUE(s1->lockExist(e1));
    EXPECT_FALSE(s1->outEdgeExist(e1));
    EXPECT_FALSE(s1->inEdgeExist(e1));

    s1->insertBiEdge(e2);
    s1->insertInvalidLock(glk2);
    EXPECT_TRUE(s1->lockExist(e2));
    EXPECT_TRUE(s1->outEdgeExist(e2));
    EXPECT_TRUE(s1->inEdgeExist(e2));

    GetNeighborsExecutor neiExec(s1, e1);

    EXPECT_FALSE(s1->lockExist(e1));
    EXPECT_FALSE(s1->outEdgeExist(e1));
    EXPECT_FALSE(s1->inEdgeExist(e1));

    EXPECT_FALSE(s1->lockExist(e2));
    EXPECT_TRUE(s1->outEdgeExist(e2));
    EXPECT_TRUE(s1->inEdgeExist(e2));

    GetPropsExecutor exec2(s1, e2);
    EXPECT_EQ(e2.props, exec2.data());
}

/**
 * @brief  good lock(1) + edge(1) + good lock(2) + edge(2)
 */
TEST_F(TossTest, test24_glk1_eg1_glk2_eg2) {
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, s1->edgeType_, 2);
    auto& e1 = edges[0];
    auto& e2 = edges[1];
    auto lk1 = TossTestUtils::makeTwinEdge(e1);
    auto lk2 = TossTestUtils::makeTwinEdge(e2);


    s1->insertBiEdge(e1);
    s1->insertValidLock(lk1);
    EXPECT_TRUE(s1->lockExist(e1));
    EXPECT_TRUE(s1->outEdgeExist(e1));
    EXPECT_TRUE(s1->inEdgeExist(e1));

    s1->insertBiEdge(e2);
    s1->insertValidLock(lk2);
    EXPECT_TRUE(s1->lockExist(e2));
    EXPECT_TRUE(s1->outEdgeExist(e2));
    EXPECT_TRUE(s1->inEdgeExist(e2));

    GetNeighborsExecutor neiExec(s1, e1);

    EXPECT_FALSE(s1->lockExist(e1));
    EXPECT_TRUE(s1->outEdgeExist(e1));
    EXPECT_TRUE(s1->inEdgeExist(e1));

    EXPECT_FALSE(s1->lockExist(e2));
    EXPECT_TRUE(s1->outEdgeExist(e2));
    EXPECT_TRUE(s1->inEdgeExist(e2));

    GetPropsExecutor exec1(s1, e1);
    EXPECT_EQ(lk1.props, exec1.data());

    GetPropsExecutor exec2(s1, e2);
    EXPECT_EQ(lk2.props, exec2.data());
}

/**
 * @brief  good lock(1) + edge(1) + bad lock(2) + edge(2)
 */
TEST_F(TossTest, test24_glk1_eg1_blk2_eg2) {
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, s1->edgeType_, 2);
    auto& e1 = edges[0];
    auto& e2 = edges[1];
    auto lk1 = TossTestUtils::makeTwinEdge(e1);
    auto lk2 = TossTestUtils::makeTwinEdge(e2);


    s1->insertBiEdge(e1);
    s1->insertValidLock(lk1);
    EXPECT_TRUE(s1->lockExist(e1));
    EXPECT_TRUE(s1->outEdgeExist(e1));
    EXPECT_TRUE(s1->inEdgeExist(e1));

    s1->insertBiEdge(e2);
    s1->insertInvalidLock(lk2);
    EXPECT_TRUE(s1->lockExist(e2));
    EXPECT_TRUE(s1->outEdgeExist(e2));
    EXPECT_TRUE(s1->inEdgeExist(e2));

    GetNeighborsExecutor neiExec(s1, e1);

    EXPECT_FALSE(s1->lockExist(e1));
    EXPECT_TRUE(s1->outEdgeExist(e1));
    EXPECT_TRUE(s1->inEdgeExist(e1));

    EXPECT_FALSE(s1->lockExist(e2));
    EXPECT_TRUE(s1->outEdgeExist(e2));
    EXPECT_TRUE(s1->inEdgeExist(e2));

    GetPropsExecutor exec1(s1, e1);
    EXPECT_EQ(lk1.props, exec1.data());

    GetPropsExecutor exec2(s1, e2);
    EXPECT_EQ(e2.props, exec2.data());
}

/**
 * @brief bad lock(1) + edge(1) + good lock(2) + edge(2)
 */
TEST_F(TossTest, test24_blk1_eg1_glk2_eg2) {
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, s1->edgeType_, 2);
    auto& e1 = edges[0];
    auto& e2 = edges[1];
    auto lk1 = TossTestUtils::makeTwinEdge(e1);
    auto lk2 = TossTestUtils::makeTwinEdge(e2);

    s1->insertBiEdge(e1);
    s1->insertInvalidLock(lk1);
    EXPECT_TRUE(s1->lockExist(e1));
    EXPECT_TRUE(s1->outEdgeExist(e1));
    EXPECT_TRUE(s1->inEdgeExist(e1));

    s1->insertBiEdge(e2);
    s1->insertValidLock(lk2);
    EXPECT_TRUE(s1->lockExist(e2));
    EXPECT_TRUE(s1->outEdgeExist(e2));
    EXPECT_TRUE(s1->inEdgeExist(e2));

    GetNeighborsExecutor neiExec(s1, e1);

    EXPECT_FALSE(s1->lockExist(e1));
    EXPECT_TRUE(s1->outEdgeExist(e1));
    EXPECT_TRUE(s1->inEdgeExist(e1));

    EXPECT_FALSE(s1->lockExist(e2));
    EXPECT_TRUE(s1->outEdgeExist(e2));
    EXPECT_TRUE(s1->inEdgeExist(e2));

    GetPropsExecutor exec1(s1, e1);
    EXPECT_EQ(e1.props, exec1.data());

    GetPropsExecutor exec2(s1, e2);
    EXPECT_EQ(lk2.props, exec2.data());
}

/**
 * @brief bad lock(1) + edge(1) + bad lock(2) + edge(2)
 */
TEST_F(TossTest, test24_blk1_eg1_blk2_eg2) {
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, s1->edgeType_, 2);
    auto& e1 = edges[0];
    auto& e2 = edges[1];
    auto lk1 = TossTestUtils::makeTwinEdge(e1);
    auto lk2 = TossTestUtils::makeTwinEdge(e2);

    s1->insertBiEdge(e1);
    s1->insertInvalidLock(lk1);
    EXPECT_TRUE(s1->lockExist(e1));
    EXPECT_TRUE(s1->outEdgeExist(e1));
    EXPECT_TRUE(s1->inEdgeExist(e1));

    s1->insertBiEdge(e2);
    s1->insertInvalidLock(lk2);
    EXPECT_TRUE(s1->lockExist(e2));
    EXPECT_TRUE(s1->outEdgeExist(e2));
    EXPECT_TRUE(s1->inEdgeExist(e2));

    GetNeighborsExecutor neiExec(s1, e1);

    EXPECT_FALSE(s1->lockExist(e1));
    EXPECT_TRUE(s1->outEdgeExist(e1));
    EXPECT_TRUE(s1->inEdgeExist(e1));

    EXPECT_FALSE(s1->lockExist(e2));
    EXPECT_TRUE(s1->outEdgeExist(e2));
    EXPECT_TRUE(s1->inEdgeExist(e2));

    GetPropsExecutor exec1(s1, e1);
    EXPECT_EQ(e1.props, exec1.data());

    GetPropsExecutor exec2(s1, e2);
    EXPECT_EQ(e2.props, exec2.data());
}

/**
 * @brief update an edge
 */
TEST_F(TossTest, test30_update_eg) {
    LOG(INFO) << "b_=" << b_;

    auto e1 = TossTestUtils::makeEdge(b_, s1->edgeType_);
    s1->insertBiEdge(e1);
    EXPECT_FALSE(s1->lockExist(e1));
    EXPECT_TRUE(s1->outEdgeExist(e1));
    EXPECT_TRUE(s1->inEdgeExist(e1));

    auto e2 = TossTestUtils::makeTwinEdge(e1);
    UpdateExecutor upd(s1, e2);
    EXPECT_FALSE(s1->lockExist(e1));
    EXPECT_TRUE(s1->outEdgeExist(e1));
    EXPECT_TRUE(s1->inEdgeExist(e1));

    GetPropsExecutor exec1(s1, e1);
    EXPECT_EQ(e2.props, exec1.data());
}

/**
 * @brief update non-exist edge
 */
TEST_F(TossTest, test30_update_non_exist_eg) {
    LOG(INFO) << "b_=" << b_;

    auto e1 = TossTestUtils::makeEdge(b_, s1->edgeType_);
    EXPECT_FALSE(s1->lockExist(e1));
    EXPECT_FALSE(s1->outEdgeExist(e1));
    EXPECT_FALSE(s1->inEdgeExist(e1));

    auto e2 = TossTestUtils::makeTwinEdge(e1);
    UpdateExecutor upd(s1, e2);
}

/**
 * @brief update good lock
 */
TEST_F(TossTest, test30_update_glk) {
    LOG(INFO) << "b_=" << b_;

    auto e1 = TossTestUtils::makeEdge(b_, s1->edgeType_);
    s1->insertValidLock(e1);
    EXPECT_TRUE(s1->lockExist(e1));
    EXPECT_TRUE(s1->outEdgeExist(e1));
    EXPECT_FALSE(s1->inEdgeExist(e1));

    auto e2 = TossTestUtils::makeTwinEdge(e1);
    UpdateExecutor upd(s1, e2);
    EXPECT_FALSE(s1->lockExist(e1));
    EXPECT_TRUE(s1->outEdgeExist(e1));
    EXPECT_TRUE(s1->inEdgeExist(e1));

    GetPropsExecutor exec1(s1, e1);
    EXPECT_EQ(e2.props, exec1.data());

    AddEdgeExecutor addEdge(s1, e1);
    EXPECT_EQ(addEdge.code(), cpp2::ErrorCode::SUCCEEDED);
}

/**
 * @brief update bad lock
 */
TEST_F(TossTest, test30_update_blk) {
    LOG(INFO) << "b_=" << b_;

    auto e1 = TossTestUtils::makeEdge(b_, s1->edgeType_);
    s1->insertInvalidLock(e1);
    EXPECT_TRUE(s1->lockExist(e1));
    EXPECT_FALSE(s1->outEdgeExist(e1));
    EXPECT_FALSE(s1->inEdgeExist(e1));

    auto e2 = TossTestUtils::makeTwinEdge(e1);
    UpdateExecutor upd(s1, e2);

    EXPECT_FALSE(s1->lockExist(e1));
    EXPECT_FALSE(s1->outEdgeExist(e1));
    EXPECT_FALSE(s1->inEdgeExist(e1));

    AddEdgeExecutor addEdge(s1, e1);
    EXPECT_EQ(addEdge.code(), cpp2::ErrorCode::SUCCEEDED);
}

/**
 * @brief update an edge then insert same
 */
TEST_F(TossTest, test30_update_eg_then_add) {
    LOG(INFO) << "b_=" << b_;

    auto e1 = TossTestUtils::makeEdge(b_, s1->edgeType_);
    s1->insertBiEdge(e1);
    EXPECT_FALSE(s1->lockExist(e1));
    EXPECT_TRUE(s1->outEdgeExist(e1));
    EXPECT_TRUE(s1->inEdgeExist(e1));

    auto e2 = TossTestUtils::makeTwinEdge(e1);
    UpdateExecutor upd(s1, e2);
    EXPECT_FALSE(s1->lockExist(e1));
    EXPECT_TRUE(s1->outEdgeExist(e1));
    EXPECT_TRUE(s1->inEdgeExist(e1));

    GetPropsExecutor exec1(s1, e1);
    EXPECT_EQ(e2.props, exec1.data());

    AddEdgeExecutor addEdge(s1, e1);
    EXPECT_EQ(addEdge.code(), cpp2::ErrorCode::SUCCEEDED);
}

/**
 * @brief add string edge in an string vid space
 */
TEST_F(TossTest, test40_string_vid_add_eg) {
    auto eg = TossTestUtils::makeEdgeS(b_, s2->getEdgeType());
    LOG(INFO) << "b_=" << b_ << ", eg hex: " << folly::hexlify(s2->strEdgeKey(eg.key));

    AddEdgeExecutor exec(s2, eg);
    EXPECT_TRUE(exec.ok());
    EXPECT_FALSE(s2->lockExist(eg));
    EXPECT_TRUE(s2->outEdgeExist(eg));
    EXPECT_TRUE(s2->inEdgeExist(eg));
}

// /**
//  * @brief add string edge in an string vid space
//  */
// TEST_F(TossTest, test50_multi_add_eg) {
//     auto eg = TossTestUtils::makeEdgeS(b_, TossTest::edgeTypeS_);
//     LOG(INFO) << "b_=" << b_ << ", eg hex: " << folly::hexlify(env->strEdgeKey(eg.key));

//     AddEdgeExecutor exec(s1, eg);
//     EXPECT_TRUE(exec.ok());
//     EXPECT_FALSE(s1->lockExist(eg));
//     EXPECT_TRUE(s1->outEdgeExist(eg));
//     EXPECT_TRUE(s1->inEdgeExist(eg));
// }


// /**
//  * @brief update an edge in an string vid space
//  */
// TEST_F(TossTest, test40_string_vid_update_eg) {
//     auto e1 = TossTestUtils::makeEdgeS(b_, TossTest::edgeTypeS_);
//     LOG(INFO) << "b_=" << b_ << ", eg hex: " << folly::hexlify(env->strEdgeKey(e1.key));

//     s1->insertBiEdge(e1);
//     EXPECT_FALSE(s1->lockExist(e1));
//     EXPECT_TRUE(s1->outEdgeExist(e1));
//     EXPECT_TRUE(s1->inEdgeExist(e1));

//     auto e2 = TossTestUtils::makeTwinEdge(e1);

//     UpdateExecutor upd(s1, e2);
//     EXPECT_FALSE(s1->lockExist(e1));
//     EXPECT_TRUE(s1->outEdgeExist(e1));
//     EXPECT_TRUE(s1->inEdgeExist(e1));

//     GetPropsExecutor exec1(s1, e1);
//     EXPECT_EQ(e2.props, exec1.data());
// }

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
    FLAGS_heartbeat_interval_secs = 1;

    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, false);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
