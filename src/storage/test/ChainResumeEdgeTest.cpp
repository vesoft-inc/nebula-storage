 /* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include <folly/Benchmark.h>
#include <folly/Format.h>
#include <folly/container/Enumerate.h>
#include <folly/init/Init.h>
#include <folly/String.h>
#include <thrift/lib/cpp/util/EnumUtils.h>

#include "common/fs/TempDir.h"

#include "storage/CommonUtils.h"
#include "storage/transaction/ConsistUtil.h"
#include "storage/transaction/ChainAddEdgesProcessorLocal.h"
#include "storage/transaction/ChainAddEdgesGroupProcessor.h"
#include "storage/transaction/ChainResumeProcessor.h"

#include "mock/MockCluster.h"
#include "mock/MockData.h"
#include "storage/test/TestUtils.h"
#include "storage/test/ChainTestUtils.h"

namespace nebula {
namespace storage {

constexpr int32_t mockSpaceId = 1;
constexpr int32_t mockPartNum = 6;

ChainTestUtils gTestUtil;

/**
 * @brief resumeChainTest1
 *                  insert      resume
 *  prepareLocal    succeed     succeed
 *  processRemote   skip        succeed
 *  processLocal    skip        succeed
 *  expect: edge/prime/double : 334/0/0
 *          keyOfRequest: false
 */
TEST(ChainResumeEdgesTest, resumeChainTest1) {
    fs::TempDir rootPath("/tmp/AddEdgesTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto mClient = MetaClientTestUpdater::makeDefaultMetaClient();

    env->metaClient_ = mClient.get();
    auto* proc = new FakeChainAddEdgesProcessorLocal(env);

    proc->rcProcessRemote = nebula::cpp2::ErrorCode::SUCCEEDED;
    proc->rcProcessLocal = nebula::cpp2::ErrorCode::SUCCEEDED;

    LOG(INFO) << "Build AddEdgesRequest...";
    cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq(false, 1);

    LOG(INFO) << "Test AddEdgesProcessor...";
    auto fut = proc->getFuture();
    proc->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());

    EXPECT_EQ(0, numOfKey(req, gTestUtil.genKey, env));
    EXPECT_EQ(334, numOfKey(req, gTestUtil.genPrime, env));
    EXPECT_EQ(0, numOfKey(req, gTestUtil.genDoublePrime, env));

    auto* proc2 = new FakeChainAddEdgesProcessorLocal(env);
    proc2->rcProcessRemote = nebula::cpp2::ErrorCode::SUCCEEDED;

    ChainResumeProcessor resumeProc(env);
    ChainResumeProcessorTestHelper testHelper(&resumeProc);
    testHelper.setAddEdgeProc(proc2);
    resumeProc.process();

    EXPECT_EQ(334, numOfKey(req, gTestUtil.genKey, env));
    EXPECT_EQ(0, numOfKey(req, gTestUtil.genPrime, env));
    EXPECT_EQ(0, numOfKey(req, gTestUtil.genDoublePrime, env));

    auto txnId = testHelper.getTxnId();
    auto keyOfRequest = ConsistUtil::tempRequestTable() + txnId;
    auto [spaceId, partId] = extractSpaceAndPart(req);
    EXPECT_FALSE(keyExist(env, spaceId, partId, keyOfRequest));
    EXPECT_FALSE(keyExist(env, spaceId, partId, "1234567"));
}

/**
 * @brief resumeChainTest2
 *                  previous    resume
 *  prepareLocal    succeed     succeed
 *  processRemote   skip        failed
 *  processLocal    skip        failed
 *  expect: edge/prime/double : 0/0/0
 *          keyOfRequest: false
 */
TEST(ChainResumeEdgesTest, resumeChainTest2) {
    fs::TempDir rootPath("/tmp/AddEdgesTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto mClient = MetaClientTestUpdater::makeDefaultMetaClient();

    env->metaClient_ = mClient.get();
    auto* proc = new FakeChainAddEdgesProcessorLocal(env);

    proc->rcProcessRemote = nebula::cpp2::ErrorCode::SUCCEEDED;
    proc->rcProcessLocal = nebula::cpp2::ErrorCode::SUCCEEDED;

    LOG(INFO) << "Build AddEdgesRequest...";
    cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq(false, 1);

    LOG(INFO) << "Test AddEdgesProcessor...";
    auto fut = proc->getFuture();
    proc->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());

    ChainTestUtils util;
    EXPECT_EQ(0, numOfKey(req, util.genKey, env));
    EXPECT_EQ(334, numOfKey(req, util.genPrime, env));
    EXPECT_EQ(0, numOfKey(req, util.genDoublePrime, env));

    auto* proc2 = new FakeChainAddEdgesProcessorLocal(env);
    proc2->rcProcessRemote = nebula::cpp2::ErrorCode::E_UNKNOWN;

    ChainResumeProcessor resumeProc(env);
    ChainResumeProcessorTestHelper testHelper(&resumeProc);
    testHelper.setAddEdgeProc(proc2);
    resumeProc.process();

    EXPECT_EQ(0, numOfKey(req, util.genKey, env));
    EXPECT_EQ(0, numOfKey(req, util.genPrime, env));
    EXPECT_EQ(0, numOfKey(req, util.genDoublePrime, env));

    auto txnId = testHelper.getTxnId();
    auto keyOfRequest = ConsistUtil::tempRequestTable() + txnId;
    auto [spaceId, partId] = extractSpaceAndPart(req);
    EXPECT_FALSE(keyExist(env, spaceId, partId, keyOfRequest));
    EXPECT_FALSE(keyExist(env, spaceId, partId, "1234567"));
}

/**
 * @brief resumeChainTest3
 *                  insert      resume
 *  prepareLocal    succeed     succeed
 *  processRemote   skip        outdate
 *  processLocal    skip        succeed
 *  expect: edge/prime/double : 334/0/334
 *          keyOfRequest: true
 */
TEST(ChainResumeEdgesTest, resumeChainTest3) {
    fs::TempDir rootPath("/tmp/AddEdgesTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto mClient = MetaClientTestUpdater::makeDefaultMetaClient();

    env->metaClient_ = mClient.get();
    auto* proc = new FakeChainAddEdgesProcessorLocal(env);

    proc->rcProcessRemote = nebula::cpp2::ErrorCode::SUCCEEDED;
    proc->rcProcessLocal = nebula::cpp2::ErrorCode::SUCCEEDED;

    LOG(INFO) << "Build AddEdgesRequest...";
    cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq(false, 1);

    LOG(INFO) << "Test AddEdgesProcessor...";
    auto fut = proc->getFuture();
    proc->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());

    ChainTestUtils util;
    EXPECT_EQ(0, numOfKey(req, util.genKey, env));
    EXPECT_EQ(334, numOfKey(req, util.genPrime, env));
    EXPECT_EQ(0, numOfKey(req, util.genDoublePrime, env));

    auto* proc2 = new FakeChainAddEdgesProcessorLocal(env);
    proc2->rcProcessRemote = nebula::cpp2::ErrorCode::E_RPC_FAILURE;

    ChainResumeProcessor resumeProc(env);
    ChainResumeProcessorTestHelper testHelper(&resumeProc);
    testHelper.setAddEdgeProc(proc2);
    resumeProc.process();

    // none of really edge key should be inserted
    EXPECT_EQ(334, numOfKey(req, util.genKey, env));
    EXPECT_EQ(0, numOfKey(req, util.genPrime, env));
    EXPECT_EQ(334, numOfKey(req, util.genDoublePrime, env));

    auto txnId = testHelper.getTxnId();
    auto keyOfRequest = ConsistUtil::tempRequestTable() + txnId;
    auto [spaceId, partId] = extractSpaceAndPart(req);
    EXPECT_TRUE(keyExist(env, spaceId, partId, keyOfRequest));
}

/**
 * @brief resumeRemoteTest1
 *                  insert      resume
 *  prepareLocal    succeed     succeed
 *  processRemote   outdate     failed
 *  processLocal    succeed     succeed
 *  expect: edge/prime/double : 334/0/334
 *          keyOfRequest: true
 */
TEST(ChainResumeEdgesTest, resumeRemoteTest1) {
    fs::TempDir rootPath("/tmp/AddEdgesTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto mClient = MetaClientTestUpdater::makeDefaultMetaClient();

    env->metaClient_ = mClient.get();
    auto* proc = new FakeChainAddEdgesProcessorLocal(env);

    proc->rcProcessRemote = nebula::cpp2::ErrorCode::E_RPC_FAILURE;

    LOG(INFO) << "Build AddEdgesRequest...";
    cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq(false, 1);

    LOG(INFO) << "Test AddEdgesProcessor...";
    auto fut = proc->getFuture();
    proc->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());

    ChainTestUtils util;
    EXPECT_EQ(334, numOfKey(req, util.genKey, env));
    EXPECT_EQ(0, numOfKey(req, util.genPrime, env));
    EXPECT_EQ(334, numOfKey(req, util.genDoublePrime, env));

    auto* proc2 = new FakeChainAddEdgesProcessorLocal(env);
    proc2->rcProcessRemote = nebula::cpp2::ErrorCode::E_RPC_FAILURE;

    ChainResumeProcessor resumeProc(env);
    ChainResumeProcessorTestHelper testHelper(&resumeProc);
    testHelper.setAddEdgeProc(proc2);
    resumeProc.process();

    EXPECT_EQ(334, numOfKey(req, util.genKey, env));
    EXPECT_EQ(0, numOfKey(req, util.genPrime, env));
    EXPECT_EQ(334, numOfKey(req, util.genDoublePrime, env));

    auto txnId = testHelper.getTxnId();
    auto keyOfRequest = ConsistUtil::tempRequestTable() + txnId;
    auto [spaceId, partId] = extractSpaceAndPart(req);
    EXPECT_TRUE(keyExist(env, spaceId, partId, keyOfRequest));
}

/**
 * @brief resumeRemoteTest2
 *                  insert      resume
 *  prepareLocal    succeed     succeed
 *  processRemote   outdate     outdate
 *  processLocal    succeed     succeed
 *  expect: edge/prime/double : 334/0/334
 *          keyOfRequest: true
 */
TEST(ChainResumeEdgesTest, resumeRemoteTest2) {
    fs::TempDir rootPath("/tmp/AddEdgesTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto mClient = MetaClientTestUpdater::makeDefaultMetaClient();

    env->metaClient_ = mClient.get();
    auto* proc = new FakeChainAddEdgesProcessorLocal(env);

    proc->rcProcessRemote = nebula::cpp2::ErrorCode::E_RPC_FAILURE;

    LOG(INFO) << "Build AddEdgesRequest...";
    cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq(false, 1);

    LOG(INFO) << "Test AddEdgesProcessor...";
    auto fut = proc->getFuture();
    proc->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());

    ChainTestUtils util;
    EXPECT_EQ(334, numOfKey(req, util.genKey, env));
    EXPECT_EQ(0, numOfKey(req, util.genPrime, env));
    EXPECT_EQ(334, numOfKey(req, util.genDoublePrime, env));

    auto* proc2 = new FakeChainAddEdgesProcessorLocal(env);
    proc2->rcProcessRemote = nebula::cpp2::ErrorCode::E_RPC_FAILURE;

    ChainResumeProcessor resumeProc(env);
    ChainResumeProcessorTestHelper testHelper(&resumeProc);
    testHelper.setAddEdgeProc(proc2);
    resumeProc.process();

    EXPECT_EQ(334, numOfKey(req, util.genKey, env));
    EXPECT_EQ(0, numOfKey(req, util.genPrime, env));
    EXPECT_EQ(334, numOfKey(req, util.genDoublePrime, env));

    auto txnId = testHelper.getTxnId();
    auto keyOfRequest = ConsistUtil::tempRequestTable() + txnId;
    auto [spaceId, partId] = extractSpaceAndPart(req);
    EXPECT_TRUE(keyExist(env, spaceId, partId, keyOfRequest));
}

/**
 * @brief resumeRemoteTest3
 *                  insert      resume
 *  prepareLocal    succeed     succeed
 *  processRemote   outdate     succeed
 *  processLocal    succeed     succeed
 *  expect: edge/prime/double : 334/0/0
 *          keyOfRequest: false
 */
TEST(ChainResumeEdgesTest, resumeRemoteTest3) {
    fs::TempDir rootPath("/tmp/AddEdgesTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto mClient = MetaClientTestUpdater::makeDefaultMetaClient();

    env->metaClient_ = mClient.get();
    auto* proc = new FakeChainAddEdgesProcessorLocal(env);

    proc->rcProcessRemote = nebula::cpp2::ErrorCode::E_RPC_FAILURE;

    LOG(INFO) << "Build AddEdgesRequest...";
    cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq(false, 1);

    LOG(INFO) << "Test AddEdgesProcessor...";
    auto fut = proc->getFuture();
    proc->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());

    ChainTestUtils util;
    EXPECT_EQ(334, numOfKey(req, util.genKey, env));
    EXPECT_EQ(0, numOfKey(req, util.genPrime, env));
    EXPECT_EQ(334, numOfKey(req, util.genDoublePrime, env));

    auto* proc2 = new FakeChainAddEdgesProcessorLocal(env);
    proc2->rcProcessRemote = nebula::cpp2::ErrorCode::SUCCEEDED;

    ChainResumeProcessor resumeProc(env);
    ChainResumeProcessorTestHelper testHelper(&resumeProc);
    testHelper.setAddEdgeProc(proc2);
    resumeProc.process();

    EXPECT_EQ(334, numOfKey(req, util.genKey, env));
    EXPECT_EQ(0, numOfKey(req, util.genPrime, env));
    EXPECT_EQ(0, numOfKey(req, util.genDoublePrime, env));

    auto txnId = testHelper.getTxnId();
    auto keyOfRequest = ConsistUtil::tempRequestTable() + txnId;
    auto [spaceId, partId] = extractSpaceAndPart(req);
    EXPECT_FALSE(keyExist(env, spaceId, partId, keyOfRequest));
}
}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, false);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}


/**
 * @brief resumeChainTest1
 *                  insert      resume
 *  prepareLocal    succeed     succeed
 *  processRemote   skip        succeed
 *  processLocal    skip        succeed
 *  expect: edge/prime/double : 334/0/0
 *          keyOfRequest: false
 */

/**
 * @brief resumeChainTest2
 *                  insert      resume
 *  prepareLocal    succeed     succeed
 *  processRemote   skip        failed
 *  processLocal    skip        failed
 *  expect: edge/prime/double : 0/0/0
 *          keyOfRequest: false
 */

/**
 * @brief resumeChainTest3
 *                  insert      resume
 *  prepareLocal    succeed     succeed
 *  processRemote   skip        outdate
 *  processLocal    skip        succeed
 *  expect: edge/prime/double : 334/0/334
 *          keyOfRequest: true
 */

/**
 * @brief resumeRemoteTest1
 *                  insert      resume
 *  prepareLocal    succeed     succeed
 *  processRemote   outdate     failed
 *  processLocal    succeed     succeed
 *  expect: edge/prime/double : 334/0/334
 *          keyOfRequest: true
 */

/**
 * @brief resumeRemoteTest2
 *                  insert      resume
 *  prepareLocal    succeed     succeed
 *  processRemote   outdate     outdate
 *  processLocal    succeed     succeed
 *  expect: edge/prime/double : 334/0/334
 *          keyOfRequest: true
 */

/**
 * @brief resumeRemoteTest3
 *                  insert      resume
 *  prepareLocal    succeed     succeed
 *  processRemote   outdate     succeed
 *  processLocal    succeed     succeed
 *  expect: edge/prime/double : 334/0/0
 *          keyOfRequest: false
 */
