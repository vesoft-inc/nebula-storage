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
    auto expect = 0;
    auto edges = TossTestUtils::makeNeighborEdges(b_, edgeType_, 1);
    GetNeighborsExecutor exec(edges);
    auto props = exec.data();
    auto svec = TossTestUtils::splitNeiResults(props);

    EXPECT_TRUE(svec.empty());
    EXPECT_FALSE(env->inEdgeExist(edges[0]));
    EXPECT_FALSE(env->outEdgeExist(edges[0]));
    EXPECT_FALSE(env->lockExist(edges[0]));
    EXPECT_EQ(svec.size(), expect);
}

/**
 * @brief add edge
 */
TEST_F(TossTest, test0_add_eg) {
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, edgeType_, 1);

    AddEdgeExecutor exec(edges[0]);

    EXPECT_TRUE(env->inEdgeExist(edges[0]));
    EXPECT_TRUE(env->outEdgeExist(edges[0]));
    EXPECT_FALSE(env->lockExist(edges[0]));
}

/**
 * @brief add edge
 */
TEST_F(TossTest, test0_add_eg_then_getProp) {
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, edgeType_, 1);

    AddEdgeExecutor addExec(edges[0]);

    EXPECT_TRUE(env->inEdgeExist(edges[0]));
    EXPECT_TRUE(env->outEdgeExist(edges[0]));
    EXPECT_FALSE(env->lockExist(edges[0]));

    GetPropsExecutor exec(edges[0]);
    auto props = exec.data();
    std::vector<std::string> values;
    for (auto& prop : props) {
        values.emplace_back(prop.toString());
    }
    auto svec = TossTestUtils::splitNeiResults(values);
    LOG(INFO) << "svec.size() = " << svec.size();
    for (auto& s : svec) {
        LOG(INFO) << "s: " << s;
    }

    EXPECT_TRUE(env->inEdgeExist(edges[0]));
    EXPECT_TRUE(env->outEdgeExist(edges[0]));
    EXPECT_FALSE(env->lockExist(edges[0]));
    EXPECT_EQ(svec.size(), 1U);
}


/**
 * @brief normal edge
 */
TEST_F(TossTest, test1_e) {
    auto expect = 1;
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, edgeType_, 1);

    auto lockKey = env->insertBiEdge(edges[0]);
    EXPECT_TRUE(env->inEdgeExist(edges[0]));
    EXPECT_TRUE(env->outEdgeExist(edges[0]));
    EXPECT_FALSE(env->lockExist(edges[0]));

    // GetNeighborsExecutor exec(edges);
    // auto props = exec.data();
    GetPropsExecutor exec(edges[0]);
    auto props = exec.data();
    std::vector<std::string> values;
    for (auto& prop : props) {
        values.emplace_back(prop.toString());
    }
    auto svec = TossTestUtils::splitNeiResults(values);
    LOG(INFO) << "svec.size() = " << svec.size();
    for (auto& s : svec) {
        LOG(INFO) << "s: " << s;
    }

    EXPECT_TRUE(env->inEdgeExist(edges[0]));
    EXPECT_TRUE(env->outEdgeExist(edges[0]));
    EXPECT_FALSE(env->lockExist(edges[0]));
    EXPECT_EQ(svec.size(), expect);
}

/**
 * @brief good lock
 */
TEST_F(TossTest, test2_glk) {
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, edgeType_, 1);

    auto lockKey = env->insertValidLock(edges[0]);
    EXPECT_FALSE(env->inEdgeExist(edges[0]));
    EXPECT_TRUE(env->outEdgeExist(edges[0]));
    EXPECT_TRUE(env->lockExist(edges[0]));

    GetNeighborsExecutor exec(edges);
    auto props = exec.data();
    UNUSED(props);
    // auto svec = TossTestUtils::splitNeiResults(props);

    EXPECT_TRUE(env->inEdgeExist(edges[0]));
    EXPECT_TRUE(env->outEdgeExist(edges[0]));
    EXPECT_FALSE(env->lockExist(edges[0]));
    // EXPECT_EQ(svec.size(), expect);
}

/**
 * @brief good lock + edge
 */
TEST_F(TossTest, test3_glk_eg) {
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, edgeType_, 1);

    auto lockKey = env->insertValidLock(edges[0]);
    LOG(INFO) << "lock_test_1 lock hexlify = " << folly::hexlify(lockKey);

    EXPECT_FALSE(env->inEdgeExist(edges[0]));
    EXPECT_TRUE(env->outEdgeExist(edges[0]));
    EXPECT_TRUE(env->lockExist(edges[0]));

    GetNeighborsExecutor exec(edges);
    auto props = exec.data();
    UNUSED(props);
    // auto svec = TossTestUtils::splitNeiResults(props);

    EXPECT_TRUE(env->inEdgeExist(edges[0]));
    EXPECT_TRUE(env->outEdgeExist(edges[0]));
    EXPECT_FALSE(env->lockExist(edges[0]));
}

/**
 * @brief bad lock
 */
TEST_F(TossTest, test4_blk) {
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, edgeType_, 1);

    auto lockKey = env->insertInvalidLock(edges[0]);
    LOG(INFO) << "lock_test_1 lock hexlify = " << folly::hexlify(lockKey);

    EXPECT_FALSE(env->inEdgeExist(edges[0]));
    EXPECT_FALSE(env->outEdgeExist(edges[0]));
    EXPECT_TRUE(env->lockExist(edges[0]));

    GetNeighborsExecutor exec(edges);
    auto props = exec.data();
    UNUSED(props);

    EXPECT_FALSE(env->inEdgeExist(edges[0]));
    EXPECT_FALSE(env->outEdgeExist(edges[0]));
    EXPECT_FALSE(env->lockExist(edges[0]));
}

/**
 * @brief bad lock + edge
 */
TEST_F(TossTest, test5_blk_eg) {
    LOG(INFO) << "b_=" << b_;
    auto edges = TossTestUtils::makeNeighborEdges(b_, edgeType_, 1);

    auto lockKey = env->insertInvalidLock(edges[0]);
    LOG(INFO) << "lock_test_1 lock hexlify = " << folly::hexlify(lockKey);

    EXPECT_FALSE(env->inEdgeExist(edges[0]));
    EXPECT_FALSE(env->outEdgeExist(edges[0]));
    EXPECT_TRUE(env->lockExist(edges[0]));

    GetNeighborsExecutor exec(edges);
    auto props = exec.data();
    UNUSED(props);

    EXPECT_FALSE(env->inEdgeExist(edges[0]));
    EXPECT_FALSE(env->outEdgeExist(edges[0]));
    EXPECT_FALSE(env->lockExist(edges[0]));
}

// /**
//  * @brief neighbor edge + edge
//  *        check normal data(without lock) can be read without err
//  */
// TEST_F(TossTest, neighbors_test_1) {
//     LOG(INFO) << "b_=" << b_;
//     auto num = 2;
//     std::vector<cpp2::NewEdge> edges = env->generateMultiEdges(num, b_);
//     ASSERT_EQ(edges.size(), 2);

//     auto rawKey1 = env->insertEdge(edges[0]);
//     auto rawKey2 = env->insertEdge(edges[1]);

//     auto props = env->getNeighbors(edges);
//     auto svec = TossTestUtils::splitNeiResults(props);
//     ASSERT_TRUE(TossTestUtils::compareSize(svec, num));
// }

// /**
//  * @brief neighbor edge + good lock
//  */
// TEST_F(TossTest, neighbors_test_2) {
//     LOG(INFO) << "b_=" << b_;
//     auto num = 2;
//     std::vector<cpp2::NewEdge> edges = env->generateMultiEdges(num, b_);
//     ASSERT_EQ(edges.size(), 2);

//     auto rawKey1 = env->insertEdge(edges[0]);

//     auto lockKey = env->insertLock(edges[1], true);
//     LOG(INFO) << "lock_test_1 lock hexlify = " << folly::hexlify(lockKey);

//     auto props = env->getNeighbors(edges);
//     auto svec = TossTestUtils::splitNeiResults(props);
//     ASSERT_TRUE(TossTestUtils::compareSize(svec, num));

//     // step 1st: lock key not exist
//     ASSERT_FALSE(env->keyExist(lockKey));

//     // step 2nd: edge key exist
//     auto rawKey = NebulaKeyUtils::toEdgeKey(lockKey);
//     ASSERT_TRUE(env->keyExist(rawKey));

//     // step 3rd: reverse edge key exist
//     auto reversedEdgeKey = env->reverseEdgeKey(edges[1].key);
//     auto reversedRawKey = env->makeRawKey(reversedEdgeKey);
//     ASSERT_TRUE(env->keyExist(reversedRawKey.first));
// }

// /**
//  * @brief neighbor edge + good lock + edge
//  */
// TEST_F(TossTest, neighbors_test_3) {
//     LOG(INFO) << "b_=" << b_;
//     auto num = 2;
//     std::vector<cpp2::NewEdge> edges = env->generateMultiEdges(num, b_);
//     ASSERT_EQ(edges.size(), 2);
//     auto lockEdge = env->dupEdge(edges[1]);

//     auto rawKey1 = env->insertEdge(edges[0]);
//     auto rawKey2 = env->insertEdge(edges[1]);
//     auto lockKey = env->insertLock(lockEdge, true);
//     LOG(INFO) << "lock_test_1 lock hexlify = " << folly::hexlify(lockKey);

//     auto props = env->getNeighbors(edges);
//     auto svec = TossTestUtils::splitNeiResults(props);
//     ASSERT_TRUE(TossTestUtils::compareSize(svec, num));

//     // step 1st: lock key not exist
//     ASSERT_FALSE(env->keyExist(lockKey));

//     // step 2nd: edge key exist
//     ASSERT_TRUE(env->keyExist(rawKey1));
//     ASSERT_TRUE(env->keyExist(rawKey2));

//     // step 3rd: reverse edge key exist
//     auto reversedEdgeKey = env->reverseEdgeKey(edges[1].key);
//     auto reversedRawKey = env->makeRawKey(reversedEdgeKey);
//     ASSERT_TRUE(env->keyExist(reversedRawKey.first));

//     // TossTestUtils::print_svec(svec);
//     LOG(INFO) << "edges[0]=" << edges[0].props[1].toString();
//     LOG(INFO) << "edges[1]=" << edges[1].props[1].toString();
//     LOG(INFO) << "lockEdge=" << lockEdge.props[1].toString();

//     LOG(INFO) << "lockEdge.size()=" << lockEdge.props[1].toString().size();

//     // step 4th: the get neighbors result is from lock
//     auto strProps = env->extractStrVals(svec);
//     decltype(strProps) expect{edges[0].props[1].toString(), lockEdge.props[1].toString()};
//     ASSERT_EQ(strProps, expect);
// }

// /**
//  * @brief neighbor edge + bad lock
//  */
// TEST_F(TossTest, neighbors_test_4) {
//     LOG(INFO) << "b_=" << b_;
//     auto num = 2;
//     std::vector<cpp2::NewEdge> edges = env->generateMultiEdges(num, b_);
//     ASSERT_EQ(edges.size(), 2);

//     auto rawKey1 = env->insertEdge(edges[0]);
//     auto lockEdge = env->dupEdge(edges[1]);

//     auto lockKey = env->insertLock(lockEdge, false);
//     LOG(INFO) << "lock_test_1 lock hexlify = " << folly::hexlify(lockKey);

//     auto props = env->getNeighbors(edges);
//     auto svec = TossTestUtils::splitNeiResults(props);
//     ASSERT_TRUE(TossTestUtils::compareSize(svec, num-1));

//     // step 1st: lock key not exist
//     ASSERT_FALSE(env->keyExist(lockKey));

//     // step 2nd: edge key exist
//     ASSERT_TRUE(env->keyExist(rawKey1));

//     // step 3rd: reverse edge key exist
//     auto reversedEdgeKey = env->reverseEdgeKey(lockEdge.key);
//     auto reversedRawKey = env->makeRawKey(reversedEdgeKey);
//     ASSERT_FALSE(env->keyExist(reversedRawKey.first));

//     // TossTestUtils::print_svec(svec);
//     LOG(INFO) << "edges[0]=" << edges[0].props[1].toString();

//     // step 4th: the get neighbors result is from lock
//     auto strProps = env->extractStrVals(svec);
//     decltype(strProps) expect{edges[0].props[1].toString()};
//     ASSERT_EQ(strProps, expect);
// }

// /**
//  * @brief neighbor edge + bad lock + edge
//  */
// TEST_F(TossTest, neighbors_test_5) {
//     LOG(INFO) << "b_=" << b_;
//     auto num = 2;
//     std::vector<cpp2::NewEdge> edges = env->generateMultiEdges(num, b_);
//     ASSERT_EQ(edges.size(), 2);

//     auto lock1 = env->dupEdge(edges[1]);

//     auto lockKey1 = env->insertLock(lock1, false);
//     LOG(INFO) << "lock_test_1 lock hexlify = " << folly::hexlify(lockKey1);

//     auto rawKey0 = env->insertEdge(edges[0]);
//     auto rawKey1 = env->insertEdge(edges[1]);

//     auto props = env->getNeighbors(edges);
//     auto svec = TossTestUtils::splitNeiResults(props);

//     // step 1st: lock key not exist
//     ASSERT_FALSE(env->keyExist(lockKey1));

//     // step 2nd: edge key exist
//     ASSERT_TRUE(env->keyExist(rawKey1));

//     // step 3rd: reverse edge key exist
//     auto reversedEdgeKey = env->reverseEdgeKey(lock1.key);
//     auto reversedRawKey = env->makeRawKey(reversedEdgeKey);
//     ASSERT_FALSE(env->keyExist(reversedRawKey.first));

//     LOG(INFO) << "edges[0]=" << edges[0].props[1].toString();

//     // step 4th: the get neighbors result is from lock
//     auto actualProps = env->extractStrVals(svec);
//     decltype(actualProps) expect{edges[0].props[1].toString(), edges[1].props[1].toString()};
//     ASSERT_EQ(actualProps, expect);

//     ASSERT_TRUE(TossTestUtils::compareSize(svec, num));
// }

// /**
//  * @brief neighbor good lock + edge
//  */
// TEST_F(TossTest, neighbors_test_6) {
//     LOG(INFO) << "b_=" << b_;
//     auto num = 2;
//     std::vector<cpp2::NewEdge> edges = env->generateMultiEdges(num, b_);
//     ASSERT_EQ(edges.size(), 2);

//     auto lockEdge = env->dupEdge(edges[0]);

//     auto lockKey = env->insertLock(lockEdge, true);
//     LOG(INFO) << "lock_test_1 lock hexlify = " << folly::hexlify(lockKey);

//     // auto rawKey0 = env->insertEdge(edges[0]);
//     auto rawKey1 = env->insertEdge(edges[1]);

//     auto props = env->getNeighbors(edges);
//     auto svec = TossTestUtils::splitNeiResults(props);

//     // step 1st: lock key not exist
//     ASSERT_FALSE(env->keyExist(lockKey));

//     // step 2nd: edge key exist
//     ASSERT_TRUE(env->keyExist(rawKey1));

//     // step 3rd: reverse edge key exist
//     auto reversedEdgeKey = env->reverseEdgeKey(lockEdge.key);
//     auto reversedRawKey = env->makeRawKey(reversedEdgeKey);
//     ASSERT_TRUE(env->keyExist(reversedRawKey.first));

//     // step 4th: the get neighbors result is from lock
//     auto actualProps = env->extractStrVals(svec);
//     decltype(actualProps) expect{lockEdge.props[1].toString(), edges[1].props[1].toString()};
//     ASSERT_EQ(actualProps, expect);

//     ASSERT_TRUE(TossTestUtils::compareSize(svec, num));
// }

// /**
//  * @brief neighbor good lock + good lock
//  */
// TEST_F(TossTest, neighbors_test_7) {
//     LOG(INFO) << "b_=" << b_;
//     auto num = 2;
//     std::vector<cpp2::NewEdge> edges = env->generateMultiEdges(num, b_);
//     ASSERT_EQ(edges.size(), 2);

//     auto lock0 = env->dupEdge(edges[0]);
//     auto lock1 = env->dupEdge(edges[1]);

//     auto lockKey0 = env->insertLock(lock0, true);
//     auto lockKey1 = env->insertLock(lock1, true);
//     LOG(INFO) << "lockKey0 hexlify = " << folly::hexlify(lockKey0);
//     LOG(INFO) << "lockKey1 hexlify = " << folly::hexlify(lockKey1);

//     // auto rawKey0 = env->insertEdge(edges[0]);
//     // auto rawKey1 = env->insertEdge(edges[1]);
//     auto rawKey0 = env->makeRawKey(edges[0].key);
//     auto rawKey1 = env->makeRawKey(edges[1].key);

//     auto props = env->getNeighbors(edges);
//     auto svec = TossTestUtils::splitNeiResults(props);

//     // step 1st: lock key not exist
//     ASSERT_FALSE(env->keyExist(lockKey0));
//     ASSERT_FALSE(env->keyExist(lockKey1));

//     // step 2nd: edge key exist
//     ASSERT_TRUE(env->keyExist(rawKey0.first));
//     ASSERT_TRUE(env->keyExist(rawKey1.first));

//     // step 3rd: reverse edge key exist
//     auto rRawKey0 = env->makeRawKey(env->reverseEdgeKey(lock0.key));
//     auto rRawKey1 = env->makeRawKey(env->reverseEdgeKey(lock1.key));

//     ASSERT_TRUE(env->keyExist(rRawKey0.first));
//     ASSERT_TRUE(env->keyExist(rRawKey1.first));

//     // step 4th: the get neighbors result is from lock
//     auto actualProps = env->extractStrVals(svec);
//     decltype(actualProps) expect{lock0.props[1].toString(), lock1.props[1].toString()};
//     ASSERT_EQ(actualProps, expect);

//     ASSERT_TRUE(TossTestUtils::compareSize(svec, num));
// }

// /**
//  * @brief neighbor good lock + good lock + edge
//  */
// TEST_F(TossTest, neighbors_test_8) {
//     LOG(INFO) << "b_=" << b_;
//     auto num = 2;
//     std::vector<cpp2::NewEdge> edges = env->generateMultiEdges(num, b_);
//     ASSERT_EQ(edges.size(), 2);

//     auto lock0 = env->dupEdge(edges[0]);
//     auto lock1 = env->dupEdge(edges[1]);

//     auto lockKey0 = env->insertLock(lock0, true);
//     auto lockKey1 = env->insertLock(lock1, true);
//     LOG(INFO) << "lockKey0 hexlify = " << folly::hexlify(lockKey0);
//     LOG(INFO) << "lockKey1 hexlify = " << folly::hexlify(lockKey1);

//     auto rawKey0 = env->makeRawKey(edges[0].key).first;
//     // auto rawKey1 = env->makeRawKey(edges[1].key).first;

//     // auto rawKey0 = env->insertEdge(edges[0]);
//     auto rawKey1 = env->insertEdge(edges[1]);

//     auto props = env->getNeighbors(edges);
//     auto svec = TossTestUtils::splitNeiResults(props);

//     // step 1st: lock key not exist
//     ASSERT_FALSE(env->keyExist(lockKey0));
//     ASSERT_FALSE(env->keyExist(lockKey1));

//     // step 2nd: edge key exist
//     ASSERT_TRUE(env->keyExist(rawKey0));
//     ASSERT_TRUE(env->keyExist(rawKey1));

//     // step 3rd: reverse edge key exist
//     auto rRawKey0 = env->makeRawKey(env->reverseEdgeKey(lock0.key));
//     auto rRawKey1 = env->makeRawKey(env->reverseEdgeKey(lock1.key));

//     ASSERT_TRUE(env->keyExist(rRawKey0.first));
//     ASSERT_TRUE(env->keyExist(rRawKey1.first));

//     // step 4th: the get neighbors result is from lock
//     auto actualProps = env->extractStrVals(svec);
//     decltype(actualProps) expect{lock0.props[1].toString(), lock1.props[1].toString()};
//     ASSERT_EQ(actualProps, expect);

//     ASSERT_TRUE(TossTestUtils::compareSize(svec, num));
// }

// /**
//  * @brief neighbor good lock + bad lock
//  */
// TEST_F(TossTest, neighbors_test_9) {
//     LOG(INFO) << "b_=" << b_;
//     auto num = 2;
//     std::vector<cpp2::NewEdge> edges = env->generateMultiEdges(num, b_);
//     ASSERT_EQ(edges.size(), 2);

//     auto lock0 = env->dupEdge(edges[0]);
//     auto lock1 = env->dupEdge(edges[1]);

//     auto lockKey0 = env->insertLock(lock0, true);
//     auto lockKey1 = env->insertLock(lock1, false);
//     LOG(INFO) << "lockKey0 hexlify = " << folly::hexlify(lockKey0);
//     LOG(INFO) << "lockKey1 hexlify = " << folly::hexlify(lockKey1);

//     auto rawKey0 = env->makeRawKey(edges[0].key).first;
//     auto rawKey1 = env->makeRawKey(edges[1].key).first;

//     // auto rawKey0 = env->insertEdge(edges[0]);
//     // auto rawKey1 = env->insertEdge(edges[1]);

//     auto props = env->getNeighbors(edges);
//     auto svec = TossTestUtils::splitNeiResults(props);

//     // step 1st: lock key not exist
//     ASSERT_FALSE(env->keyExist(lockKey0));
//     ASSERT_FALSE(env->keyExist(lockKey1));

//     // step 2nd: edge key exist
//     ASSERT_TRUE(env->keyExist(rawKey0));
//     ASSERT_FALSE(env->keyExist(rawKey1));

//     // step 3rd: reverse edge key exist
//     auto rRawKey0 = env->makeRawKey(env->reverseEdgeKey(lock0.key));
//     auto rRawKey1 = env->makeRawKey(env->reverseEdgeKey(lock1.key));

//     ASSERT_TRUE(env->keyExist(rRawKey0.first));
//     ASSERT_FALSE(env->keyExist(rRawKey1.first));

//     // step 4th: the get neighbors result is from lock
//     auto actualProps = env->extractStrVals(svec);
//     decltype(actualProps) expect{lock0.props[1].toString()};
//     ASSERT_EQ(actualProps, expect);

//     // ASSERT_TRUE(TossTestUtils::compareSize(svec, num));
// }

// /**
//  * @brief neighbor good lock + bad lock + edge
//  */
// TEST_F(TossTest, neighbors_test_10) {
//     LOG(INFO) << "b_=" << b_;
//     auto num = 2;
//     std::vector<cpp2::NewEdge> edges = env->generateMultiEdges(num, b_);
//     ASSERT_EQ(edges.size(), 2);

//     auto lock0 = env->dupEdge(edges[0]);
//     auto lock1 = env->dupEdge(edges[1]);

//     auto lockKey0 = env->insertLock(lock0, true);
//     auto lockKey1 = env->insertLock(lock1, false);
//     LOG(INFO) << "lockKey0 hexlify = " << folly::hexlify(lockKey0);
//     LOG(INFO) << "lockKey1 hexlify = " << folly::hexlify(lockKey1);

//     auto rawKey0 = env->makeRawKey(edges[0].key).first;
//     auto rawKey1 = env->makeRawKey(edges[1].key).first;

//     // auto rawKey0 = env->insertEdge(edges[0]);
//     env->syncAddEdge(edges[1]);

//     auto props = env->getNeighbors(edges);
//     auto svec = TossTestUtils::splitNeiResults(props);

//     // step 1st: lock key not exist
//     ASSERT_FALSE(env->keyExist(lockKey0));
//     ASSERT_FALSE(env->keyExist(lockKey1));

//     // step 2nd: edge key exist
//     ASSERT_TRUE(env->keyExist(rawKey0));
//     ASSERT_TRUE(env->keyExist(rawKey1));

//     // step 3rd: reverse edge key exist
//     auto rRawKey0 = env->makeRawKey(env->reverseEdgeKey(lock0.key));
//     auto rRawKey1 = env->makeRawKey(env->reverseEdgeKey(lock1.key));

//     ASSERT_TRUE(env->keyExist(rRawKey0.first));
//     ASSERT_TRUE(env->keyExist(rRawKey1.first));

//     // step 4th: the get neighbors result is from lock
//     auto actualProps = env->extractStrVals(svec);
//     decltype(actualProps) expect{lock0.props[1].toString(), edges[1].props[1].toString()};
//     ASSERT_EQ(actualProps, expect);

//     ASSERT_TRUE(TossTestUtils::compareSize(svec, num));
// }

// /**
//  * @brief neighbor bad lock + edge
//  */
// TEST_F(TossTest, neighbors_test_11) {
//     LOG(INFO) << "b_=" << b_;
//     auto num = 2;
//     std::vector<cpp2::NewEdge> edges = env->generateMultiEdges(num, b_);
//     ASSERT_EQ(edges.size(), 2);

//     auto lock0 = env->dupEdge(edges[0]);
//     auto lock1 = env->dupEdge(edges[1]);

//     auto lockKey0 = env->insertLock(lock0, false);
//     // auto lockKey1 = env->insertLock(lock1, false);
//     LOG(INFO) << "lockKey0 hexlify = " << folly::hexlify(lockKey0);
//     // LOG(INFO) << "lockKey1 hexlify = " << folly::hexlify(lockKey1);

//     auto rawKey0 = env->makeRawKey(edges[0].key).first;
//     auto rawKey1 = env->makeRawKey(edges[1].key).first;

//     env->syncAddEdge(edges[1]);

//     auto props = env->getNeighbors(edges);

//     // step 1st: lock key not exist
//     ASSERT_FALSE(env->keyExist(lockKey0));
//     // ASSERT_FALSE(env->keyExist(lockKey1));

//     // step 2nd: edge key exist
//     ASSERT_FALSE(env->keyExist(rawKey0));
//     ASSERT_TRUE(env->keyExist(rawKey1));

//     // step 3rd: reverse edge key exist
//     auto rRawKey0 = env->makeRawKey(env->reverseEdgeKey(lock0.key));
//     auto rRawKey1 = env->makeRawKey(env->reverseEdgeKey(lock1.key));

//     ASSERT_FALSE(env->keyExist(rRawKey0.first));
//     ASSERT_TRUE(env->keyExist(rRawKey1.first));

//     // step 4th: the get neighbors result is from lock
//     auto svec = TossTestUtils::splitNeiResults(props);
//     auto actualProps = env->extractStrVals(svec);
//     decltype(actualProps) expect{edges[1].props[1].toString()};
//     ASSERT_EQ(actualProps, expect);

//     ASSERT_TRUE(TossTestUtils::compareSize(svec, num-1));
// }

// /**
//  * @brief neighbor bad lock + good lock
//  */
// TEST_F(TossTest, neighbors_test_12) {
//     LOG(INFO) << "b_=" << b_;
//     auto num = 2;
//     std::vector<cpp2::NewEdge> edges = env->generateMultiEdges(num, b_);
//     ASSERT_EQ(edges.size(), 2);

//     auto lock0 = env->dupEdge(edges[0]);
//     auto lock1 = env->dupEdge(edges[1]);


//     auto rawKey0 = env->makeRawKey(edges[0].key).first;
//     auto rawKey1 = env->makeRawKey(edges[1].key).first;

//     // env->syncAddEdge(edges[1]);
//     auto lockKey0 = env->insertLock(lock0, false);
//     auto lockKey1 = env->insertLock(lock1, true);
//     LOG(INFO) << "lockKey0 hexlify = " << folly::hexlify(lockKey0);
//     LOG(INFO) << "lockKey1 hexlify = " << folly::hexlify(lockKey1);

//     auto props = env->getNeighbors(edges);

//     // step 1st: lock key not exist
//     ASSERT_FALSE(env->keyExist(lockKey0));
//     ASSERT_FALSE(env->keyExist(lockKey1));

//     // step 2nd: edge key exist
//     ASSERT_FALSE(env->keyExist(rawKey0));
//     ASSERT_TRUE(env->keyExist(rawKey1));

//     // step 3rd: reverse edge key exist
//     auto rRawKey0 = env->makeRawKey(env->reverseEdgeKey(lock0.key));
//     auto rRawKey1 = env->makeRawKey(env->reverseEdgeKey(lock1.key));

//     ASSERT_FALSE(env->keyExist(rRawKey0.first));
//     ASSERT_TRUE(env->keyExist(rRawKey1.first));

//     // step 4th: the get neighbors result is from lock
//     auto svec = TossTestUtils::splitNeiResults(props);
//     auto actualProps = env->extractStrVals(svec);
//     decltype(actualProps) expect{lock1.props[1].toString()};
//     ASSERT_EQ(actualProps, expect);

//     ASSERT_TRUE(TossTestUtils::compareSize(svec, num-1));
// }

// /**
//  * @brief neighbor bad lock + good lock + edge
//  */
// TEST_F(TossTest, neighbors_test_13) {
//     LOG(INFO) << "b_=" << b_;
//     auto num = 2;
//     std::vector<cpp2::NewEdge> edges = env->generateMultiEdges(num, b_);
//     ASSERT_EQ(edges.size(), 2);

//     auto lock0 = env->dupEdge(edges[0]);
//     auto lock1 = env->dupEdge(edges[1]);


//     auto rawKey0 = env->makeRawKey(edges[0].key).first;
//     auto rawKey1 = env->makeRawKey(edges[1].key).first;

//     env->syncAddEdge(edges[1]);
//     auto lockKey0 = env->insertLock(lock0, false);
//     auto lockKey1 = env->insertLock(lock1, true);
//     LOG(INFO) << "lockKey0 hexlify = " << folly::hexlify(lockKey0);
//     LOG(INFO) << "lockKey1 hexlify = " << folly::hexlify(lockKey1);

//     auto props = env->getNeighbors(edges);

//     // step 1st: lock key not exist
//     ASSERT_FALSE(env->keyExist(lockKey0));
//     ASSERT_FALSE(env->keyExist(lockKey1));

//     // step 2nd: edge key exist
//     ASSERT_FALSE(env->keyExist(rawKey0));
//     ASSERT_TRUE(env->keyExist(rawKey1));

//     // step 3rd: reverse edge key exist
//     auto rRawKey0 = env->makeRawKey(env->reverseEdgeKey(lock0.key));
//     auto rRawKey1 = env->makeRawKey(env->reverseEdgeKey(lock1.key));

//     ASSERT_FALSE(env->keyExist(rRawKey0.first));
//     ASSERT_TRUE(env->keyExist(rRawKey1.first));

//     // step 4th: the get neighbors result is from lock
//     auto svec = TossTestUtils::splitNeiResults(props);
//     auto actualProps = env->extractStrVals(svec);
//     decltype(actualProps) expect{lock1.props[1].toString()};
//     ASSERT_EQ(actualProps, expect);

//     ASSERT_TRUE(TossTestUtils::compareSize(svec, num-1));
// }

// /**
//  * @brief neighbor bad lock + bad lock
//  */
// TEST_F(TossTest, neighbors_test_14) {
//     LOG(INFO) << "b_=" << b_;
//     auto num = 2;
//     std::vector<cpp2::NewEdge> edges = env->generateMultiEdges(num, b_);
//     ASSERT_EQ(edges.size(), 2);

//     auto lock0 = env->dupEdge(edges[0]);
//     auto lock1 = env->dupEdge(edges[1]);

//     auto rawKey0 = env->makeRawKey(edges[0].key).first;
//     auto rawKey1 = env->makeRawKey(edges[1].key).first;

//     // env->syncAddEdge(edges[1]);

//     auto lockKey0 = env->insertLock(lock0, false);
//     auto lockKey1 = env->insertLock(lock1, false);
//     LOG(INFO) << "lockKey0 hexlify = " << folly::hexlify(lockKey0);
//     LOG(INFO) << "lockKey1 hexlify = " << folly::hexlify(lockKey1);

//     auto props = env->getNeighbors(edges);

//     // step 1st: lock key not exist
//     ASSERT_FALSE(env->keyExist(lockKey0));
//     ASSERT_FALSE(env->keyExist(lockKey1));

//     // step 2nd: edge key exist
//     ASSERT_FALSE(env->keyExist(rawKey0));
//     ASSERT_FALSE(env->keyExist(rawKey1));

//     // step 3rd: reverse edge key exist
//     auto rRawKey0 = env->makeRawKey(env->reverseEdgeKey(lock0.key));
//     auto rRawKey1 = env->makeRawKey(env->reverseEdgeKey(lock1.key));

//     ASSERT_FALSE(env->keyExist(rRawKey0.first));
//     ASSERT_FALSE(env->keyExist(rRawKey1.first));

//     // step 4th: the get neighbors result is from lock
//     auto svec = TossTestUtils::splitNeiResults(props);
//     auto actualProps = env->extractStrVals(svec);
//     decltype(actualProps) expect{};
//     ASSERT_EQ(actualProps, expect);

//     ASSERT_TRUE(TossTestUtils::compareSize(svec, 0));
// }

// /**
//  * @brief neighbor bad lock + bad lock + edge
//  */
// TEST_F(TossTest, neighbors_test_15) {
//     LOG(INFO) << "b_=" << b_;
//     auto num = 2;
//     std::vector<cpp2::NewEdge> edges = env->generateMultiEdges(num, b_);
//     ASSERT_EQ(edges.size(), 2);

//     auto lock0 = env->dupEdge(edges[0]);
//     auto lock1 = env->dupEdge(edges[1]);

//     auto rawKey0 = env->makeRawKey(edges[0].key).first;
//     auto rawKey1 = env->makeRawKey(edges[1].key).first;

//     env->syncAddEdge(edges[1]);

//     auto lockKey0 = env->insertLock(lock0, false);
//     auto lockKey1 = env->insertLock(lock1, false);
//     LOG(INFO) << "lockKey0 hexlify = " << folly::hexlify(lockKey0);
//     LOG(INFO) << "lockKey1 hexlify = " << folly::hexlify(lockKey1);

//     auto props = env->getNeighbors(edges);

//     // step 1st: lock key not exist
//     ASSERT_FALSE(env->keyExist(lockKey0));
//     ASSERT_FALSE(env->keyExist(lockKey1));

//     // step 2nd: edge key exist
//     ASSERT_FALSE(env->keyExist(rawKey0));
//     ASSERT_TRUE(env->keyExist(rawKey1));

//     // step 3rd: reverse edge key exist
//     auto rRawKey0 = env->makeRawKey(env->reverseEdgeKey(lock0.key));
//     auto rRawKey1 = env->makeRawKey(env->reverseEdgeKey(lock1.key));

//     ASSERT_FALSE(env->keyExist(rRawKey0.first));
//     ASSERT_TRUE(env->keyExist(rRawKey1.first));

//     // step 4th: the get neighbors result is from lock
//     auto svec = TossTestUtils::splitNeiResults(props);
//     auto actualProps = env->extractStrVals(svec);
//     decltype(actualProps) expect{edges[1].props[1].toString()};
//     ASSERT_EQ(actualProps, expect);

//     ASSERT_TRUE(TossTestUtils::compareSize(svec, 1));
// }

// /**
//  * @brief neighbor good lock + neighbor edge + good lock + edge
//  */
// TEST_F(TossTest, neighbors_test_16) {
//     LOG(INFO) << "b_=" << b_;
//     auto num = 2;
//     std::vector<cpp2::NewEdge> edges = env-> (num, b_);
//     ASSERT_EQ(edges.size(), 2);

//     auto lock0 = env->dupEdge(edges[0]);
//     auto lock1 = env->dupEdge(edges[1]);

//     auto rawKey0 = env->makeRawKey(edges[0].key).first;
//     auto rawKey1 = env->makeRawKey(edges[1].key).first;

//     env->syncAddEdge(edges[1]);

//     auto lockKey0 = env->insertLock(lock0, true);
//     auto lockKey1 = env->insertLock(lock1, true);
//     LOG(INFO) << "lockKey0 hexlify = " << folly::hexlify(lockKey0);
//     LOG(INFO) << "lockKey1 hexlify = " << folly::hexlify(lockKey1);

//     auto props = env->getNeighbors(edges);

//     // step 1st: lock key not exist
//     ASSERT_FALSE(env->keyExist(lockKey0));
//     ASSERT_FALSE(env->keyExist(lockKey1));

//     // step 2nd: edge key exist
//     ASSERT_TRUE(env->keyExist(rawKey0));
//     ASSERT_TRUE(env->keyExist(rawKey1));

//     // step 3rd: reverse edge key exist
//     auto rRawKey0 = env->makeRawKey(env->reverseEdgeKey(lock0.key));
//     auto rRawKey1 = env->makeRawKey(env->reverseEdgeKey(lock1.key));

//     ASSERT_TRUE(env->keyExist(rRawKey0.first));
//     ASSERT_TRUE(env->keyExist(rRawKey1.first));

//     // step 4th: the get neighbors result is from lock
//     auto svec = TossTestUtils::splitNeiResults(props);
//     auto actualProps = env->extractStrVals(svec);
//     decltype(actualProps) expect{lock0.props[1].toString(), lock1.props[1].toString()};
//     ASSERT_EQ(actualProps, expect);

//     ASSERT_TRUE(TossTestUtils::compareSize(svec, 2));
// }

// /**
//  * @brief  neighbor good lock + neighbor edge + bad lock + edge
//  */
// TEST_F(TossTest, neighbors_test_17) {
//     LOG(INFO) << "b_=" << b_;
//     auto num = 2;
//     std::vector<cpp2::NewEdge> edges = env->generateMultiEdges(num, b_);
//     ASSERT_EQ(edges.size(), 2);

//     auto lock0 = env->dupEdge(edges[0]);
//     auto lock1 = env->dupEdge(edges[1]);

//     auto rawKey0 = env->makeRawKey(edges[0].key).first;
//     auto rawKey1 = env->makeRawKey(edges[1].key).first;

//     env->syncAddEdge(edges[1]);

//     auto lockKey0 = env->insertLock(lock0, true);
//     auto lockKey1 = env->insertLock(lock1, false);
//     LOG(INFO) << "lockKey0 hexlify = " << folly::hexlify(lockKey0);
//     LOG(INFO) << "lockKey1 hexlify = " << folly::hexlify(lockKey1);

//     auto props = env->getNeighbors(edges);

//     // step 1st: lock key not exist
//     ASSERT_FALSE(env->keyExist(lockKey0));
//     ASSERT_FALSE(env->keyExist(lockKey1));

//     // step 2nd: edge key exist
//     ASSERT_TRUE(env->keyExist(rawKey0));
//     ASSERT_TRUE(env->keyExist(rawKey1));

//     // step 3rd: reverse edge key exist
//     auto rRawKey0 = env->makeRawKey(env->reverseEdgeKey(lock0.key));
//     auto rRawKey1 = env->makeRawKey(env->reverseEdgeKey(lock1.key));

//     ASSERT_TRUE(env->keyExist(rRawKey0.first));
//     ASSERT_TRUE(env->keyExist(rRawKey1.first));

//     // step 4th: the get neighbors result is from lock
//     auto svec = TossTestUtils::splitNeiResults(props);
//     auto actualProps = env->extractStrVals(svec);
//     decltype(actualProps) expect{lock0.props[1].toString(), edges[1].props[1].toString()};
//     ASSERT_EQ(actualProps, expect);

//     ASSERT_TRUE(TossTestUtils::compareSize(svec, 2));
// }

// /**
//  * @brief  neighbor bad lock + neighbor edge + good lock + edge
//  */
// TEST_F(TossTest, neighbors_test_18) {
//     LOG(INFO) << "b_=" << b_;
//     auto num = 2;
//     std::vector<cpp2::NewEdge> edges = env->generateMultiEdges(num, b_);
//     ASSERT_EQ(edges.size(), 2);

//     auto lock0 = env->dupEdge(edges[0]);
//     auto lock1 = env->dupEdge(edges[1]);

//     auto rawKey0 = env->makeRawKey(edges[0].key).first;
//     auto rawKey1 = env->makeRawKey(edges[1].key).first;

//     env->syncAddEdge(edges[0]);
//     env->syncAddEdge(edges[1]);

//     auto lockKey0 = env->insertLock(lock0, false);
//     auto lockKey1 = env->insertLock(lock1, true);
//     LOG(INFO) << "lockKey0 hexlify = " << folly::hexlify(lockKey0);
//     LOG(INFO) << "lockKey1 hexlify = " << folly::hexlify(lockKey1);

//     auto props = env->getNeighbors(edges);

//     // step 1st: lock key not exist
//     ASSERT_FALSE(env->keyExist(lockKey0));
//     ASSERT_FALSE(env->keyExist(lockKey1));

//     // step 2nd: edge key exist
//     ASSERT_TRUE(env->keyExist(rawKey0));
//     ASSERT_TRUE(env->keyExist(rawKey1));

//     // step 3rd: reverse edge key exist
//     auto rRawKey0 = env->makeRawKey(env->reverseEdgeKey(lock0.key));
//     auto rRawKey1 = env->makeRawKey(env->reverseEdgeKey(lock1.key));

//     ASSERT_TRUE(env->keyExist(rRawKey0.first));
//     ASSERT_TRUE(env->keyExist(rRawKey1.first));

//     // step 4th: the get neighbors result is from lock
//     auto svec = TossTestUtils::splitNeiResults(props);
//     auto actualProps = env->extractStrVals(svec);
//     decltype(actualProps) expect{edges[0].props[1].toString(), lock1.props[1].toString()};
//     ASSERT_EQ(actualProps, expect);

//     ASSERT_TRUE(TossTestUtils::compareSize(svec, 2));
// }

// /**
//  * @brief neighbor bad lock + neighbor edge + bad lock + edge
//  */
// TEST_F(TossTest, neighbors_test_19) {
//     LOG(INFO) << "b_=" << b_;
//     auto num = 2;
//     std::vector<cpp2::NewEdge> edges = env->generateMultiEdges(num, b_);
//     ASSERT_EQ(edges.size(), 2);

//     auto lock0 = env->dupEdge(edges[0]);
//     auto lock1 = env->dupEdge(edges[1]);

//     auto rawKey0 = env->makeRawKey(edges[0].key).first;
//     auto rawKey1 = env->makeRawKey(edges[1].key).first;

//     env->syncAddEdge(edges[0]);
//     env->syncAddEdge(edges[1]);

//     auto lockKey0 = env->insertLock(lock0, false);
//     auto lockKey1 = env->insertLock(lock1, false);
//     LOG(INFO) << "lockKey0 hexlify = " << folly::hexlify(lockKey0);
//     LOG(INFO) << "lockKey1 hexlify = " << folly::hexlify(lockKey1);

//     auto props = env->getNeighbors(edges);

//     // step 1st: lock key not exist
//     ASSERT_FALSE(env->keyExist(lockKey0));
//     ASSERT_FALSE(env->keyExist(lockKey1));

//     // step 2nd: edge key exist
//     ASSERT_TRUE(env->keyExist(rawKey0));
//     ASSERT_TRUE(env->keyExist(rawKey1));

//     // step 3rd: reverse edge key exist
//     auto rRawKey0 = env->makeRawKey(env->reverseEdgeKey(lock0.key));
//     auto rRawKey1 = env->makeRawKey(env->reverseEdgeKey(lock1.key));

//     ASSERT_TRUE(env->keyExist(rRawKey0.first));
//     ASSERT_TRUE(env->keyExist(rRawKey1.first));

//     // step 4th: the get neighbors result is from lock
//     auto svec = TossTestUtils::splitNeiResults(props);
//     auto actualProps = env->extractStrVals(svec);
//     decltype(actualProps) expect{edges[0].props[1].toString(), edges[1].props[1].toString()};
//     ASSERT_EQ(actualProps, expect);

//     ASSERT_TRUE(TossTestUtils::compareSize(svec, 2));
// }

// /**
//  * @brief
//  */
// TEST_F(TossTest, get_props_test_0) {
//     LOG(INFO) << "getProps_test_0 b_=" << b_;
//     auto num = 1;
//     std::vector<cpp2::NewEdge> edges = env->generateMultiEdges(num, b_);

//     LOG(INFO) << "going to add edge:" << edges.back().props.back();
//     auto code = env->syncAddMultiEdges(edges, kNotUseToss);
//     ASSERT_EQ(code, cpp2::ErrorCode::SUCCEEDED);

//     auto vvec = env->getProps(edges[0]);
//     LOG(INFO) << "vvec.size()=" << vvec.size();
//     for (auto& val : vvec) {
//         LOG(INFO) << "val.toString()=" << val.toString();
//     }
// }


// /**
//  * @brief
//  */
// TEST_F(TossTest, get_props_test_1) {
//     LOG(INFO) << __func__ << " b_=" << b_;
//     auto num = 1;
//     std::vector<cpp2::NewEdge> edges = env->generateMultiEdges(num, b_);

//     LOG(INFO) << "going to add edge:" << edges.back().props.back();
//     auto code = env->syncAddMultiEdges(edges, kUseToss);
//     ASSERT_EQ(code, cpp2::ErrorCode::SUCCEEDED);

//     auto vvec = env->getProps(edges[0]);
//     LOG(INFO) << "vvec.size()=" << vvec.size();
//     for (auto& val : vvec) {
//         LOG(INFO) << "val.toString()=" << val.toString();
//     }
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
