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
#include "storage/mutate/UpdateEdgeProcessor.h"
#include "storage/transaction/ConsistUtil.h"
#include "storage/transaction/ChainUpdateEdgeProcessorRemote.h"
#include "storage/transaction/ChainAddEdgesProcessorLocal.h"
#include "storage/transaction/ChainAddEdgesGroupProcessor.h"
#include "storage/transaction/ChainResumeProcessor.h"

#include "mock/MockCluster.h"
#include "mock/MockData.h"
#include "storage/test/QueryTestUtils.h"
#include "storage/test/TestUtils.h"
#include "storage/test/ChainTestUtils.h"

namespace nebula {
namespace storage {

using ECode = ::nebula::cpp2::ErrorCode;

constexpr int32_t mockSpaceId = 1;
constexpr int32_t mockPartNum = 6;
constexpr int32_t mockSpaceVidLen = 32;

ChainTestUtils gTestUtil;

struct ChainUpdateEdgeTestHelper {
    ChainUpdateEdgeTestHelper() {
        sEdgeType = std::to_string(std::abs(edgeType_));
    }

    cpp2::EdgeKey defaultEdgeKey() {
        cpp2::EdgeKey ret;
        ret.set_src(srcId_);
        ret.set_edge_type(edgeType_);
        ret.set_ranking(rank_);
        ret.set_dst(dstId_);
        return ret;
    }

    std::vector<cpp2::UpdatedProp> defaultUpdateProps() {
        ObjectPool objPool;
        LOG(INFO) << "Build updated props...";
        std::vector<cpp2::UpdatedProp> props;
        // int: 101.teamCareer = 20
        cpp2::UpdatedProp uProp1;
        uProp1.set_name("teamCareer");
        // ConstantExpression val1(20);
        const auto& val1 = *ConstantExpression::make(&objPool, 20);
        uProp1.set_value(Expression::encode(val1));
        props.emplace_back(uProp1);

        // bool: 101.type = trade
        cpp2::UpdatedProp uProp2;
        uProp2.set_name("type");
        std::string colnew("trade");
        // ConstantExpression val2(colnew);
        const auto& val2 = *ConstantExpression::make(&objPool, colnew);
        uProp2.set_value(Expression::encode(val2));
        props.emplace_back(uProp2);
        return props;
    }

    std::vector<std::string> defaultRetProps() {
        ObjectPool objPool;
        std::vector<std::string> props;
        std::vector<std::string> cols{
            "playerName", "teamName", "teamCareer", "type", kSrc, kType, kRank, kDst};
        for (auto& colName : cols) {
            const auto& exp = *EdgePropertyExpression::make(&objPool, sEdgeType, colName);
            props.emplace_back(Expression::encode(exp));
        }
        return props;
    }

    cpp2::UpdateEdgeRequest makeDefaultRequest() {
        auto edgeKey = defaultEdgeKey();
        auto updateProps = defaultUpdateProps();
        auto retProps = defaultRetProps();
        return makeRequest(edgeKey, updateProps, retProps);
    }

    cpp2::UpdateEdgeRequest reverseRequest(StorageEnv* env, const cpp2::UpdateEdgeRequest& req) {
        ChainUpdateEdgeProcessorLocal proc(env);
        return proc.reverseRequest(req);
    }

    cpp2::UpdateEdgeRequest makeInvalidRequest() {
        VertexID srcInvalid{"Spurssssssss"};
        auto edgeKey = defaultEdgeKey();
        edgeKey.set_src(srcInvalid);
        auto updateProps = defaultUpdateProps();
        auto retProps = defaultRetProps();
        return makeRequest(edgeKey, updateProps, retProps);
    }

    cpp2::UpdateEdgeRequest makeRequest(const cpp2::EdgeKey& edgeKey,
                                        const std::vector<cpp2::UpdatedProp>& updateProps,
                                        const std::vector<std::string>& retCols) {
        cpp2::UpdateEdgeRequest req;
        auto partId = std::hash<std::string>()(edgeKey.get_src().getStr()) % mockPartNum + 1;
        req.set_space_id(mockSpaceId);
        req.set_part_id(partId);
        req.set_edge_key(edgeKey);
        req.set_updated_props(updateProps);
        req.set_return_props(retCols);
        req.set_insertable(false);
        return req;
    }

    bool checkResp2(cpp2::UpdateResponse& resp) {
        LOG(INFO) << "checkResp2(cpp2::UpdateResponse& resp)";
        if (!resp.props_ref()) {
            LOG(INFO) << "!resp.props_ref()";
            return false;
        } else {
            LOG(INFO) << "resp.props_ref()";
            EXPECT_EQ(9, (*resp.props_ref()).colNames.size());
            EXPECT_EQ("_inserted", (*resp.props_ref()).colNames[0]);
            EXPECT_EQ("101.playerName", (*resp.props_ref()).colNames[1]);
            EXPECT_EQ("101.teamName", (*resp.props_ref()).colNames[2]);
            EXPECT_EQ("101.teamCareer", (*resp.props_ref()).colNames[3]);
            EXPECT_EQ("101.type", (*resp.props_ref()).colNames[4]);
            EXPECT_EQ(std::string("101.").append(kSrc), (*resp.props_ref()).colNames[5]);
            EXPECT_EQ(std::string("101.").append(kType), (*resp.props_ref()).colNames[6]);
            EXPECT_EQ(std::string("101.").append(kRank), (*resp.props_ref()).colNames[7]);
            EXPECT_EQ(std::string("101.").append(kDst), (*resp.props_ref()).colNames[8]);
            EXPECT_EQ(1, (*resp.props_ref()).rows.size());
            EXPECT_EQ(9, (*resp.props_ref()).rows[0].values.size());
            EXPECT_EQ(false, (*resp.props_ref()).rows[0].values[0].getBool());
            EXPECT_EQ("Tim Duncan", (*resp.props_ref()).rows[0].values[1].getStr());
            EXPECT_EQ("Spurs", (*resp.props_ref()).rows[0].values[2].getStr());
            EXPECT_EQ(20, (*resp.props_ref()).rows[0].values[3].getInt());
            EXPECT_EQ("trade", (*resp.props_ref()).rows[0].values[4].getStr());
            EXPECT_EQ("Spurs", (*resp.props_ref()).rows[0].values[5].getStr());
            EXPECT_EQ(-101, (*resp.props_ref()).rows[0].values[6].getInt());
            EXPECT_EQ(1997, (*resp.props_ref()).rows[0].values[7].getInt());
            EXPECT_EQ("Tim Duncan", (*resp.props_ref()).rows[0].values[8].getStr());
        }
        return true;
    }

    bool edgeExist(StorageEnv* env, cpp2::UpdateEdgeRequest& req) {
        auto partId = req.get_part_id();
        auto key = ConsistUtil::edgeKey(mockSpaceVidLen, partId, req.get_edge_key());
        return keyExist(env, mockSpaceId, partId, key);
    }

    bool primeExist(StorageEnv* env, cpp2::UpdateEdgeRequest& req) {
        auto partId = req.get_part_id();
        auto key = ConsistUtil::edgeKeyPrime(mockSpaceVidLen, partId, req.get_edge_key());
        return keyExist(env, mockSpaceId, partId, key);
    }

    bool doublePrimeExist(StorageEnv* env, cpp2::UpdateEdgeRequest& req) {
        auto partId = req.get_part_id();
        auto key = ConsistUtil::doublePrime(mockSpaceVidLen, partId, req.get_edge_key());
        return keyExist(env, mockSpaceId, partId, key);
    }

    bool keyExist(StorageEnv* env,
                  GraphSpaceID spaceId,
                  PartitionID partId,
                  const std::string& key) {
        std::string val;
        auto rc = env->kvstore_->get(spaceId, partId, key, &val);
        return rc == ECode::SUCCEEDED;
    }

    bool checkRequestUpdated(StorageEnv* env, cpp2::UpdateEdgeRequest& req) {
        // get serve from kvstore directly
        bool ret = true;
        auto& key = req.get_edge_key();
        auto partId = req.get_part_id();
        auto prefix = ConsistUtil::edgeKey(mockSpaceVidLen, partId, req.get_edge_key());
        std::unique_ptr<kvstore::KVIterator> iter;
        auto rc = env->kvstore_->prefix(mockSpaceId, partId, prefix, &iter);
        EXPECT_EQ(ECode::SUCCEEDED, rc);
        EXPECT_TRUE(iter && iter->valid());

        auto edgeType = key.get_edge_type();
        auto edgeReader = RowReaderWrapper::getEdgePropReader(
            env->schemaMan_, mockSpaceId, std::abs(edgeType), iter->val());

        LOG(INFO) << "req.get_updated_props().size() = " << req.get_updated_props().size();
        ObjectPool objPool;
        for (auto& prop : req.get_updated_props()) {
            LOG(INFO) << "prop name = " << prop.get_name();
            auto enVal = prop.get_value();
            auto expression = Expression::decode(&objPool, enVal);
            ConstantExpression* cexpr = static_cast<ConstantExpression*>(expression);
            auto val1 = cexpr->value();
            auto val2 = edgeReader->getValueByName(prop.get_name());

            // EXPECT_EQ(val1, val2);
            if (val1 != val2) {
                ret = false;
            }
        }

        return ret;
    }

public:
    VertexID srcId_{"Spurs"};
    VertexID dstId_{"Tim Duncan"};
    EdgeRanking rank_{1997};
    EdgeType edgeType_{-101};
    storage::cpp2::EdgeKey updateKey_;
    std::string sEdgeType;
};

class FakeInternalStorageClient : public InternalStorageClient {
public:
    explicit FakeInternalStorageClient(StorageEnv* env,
                                       std::shared_ptr<folly::IOThreadPoolExecutor> pool,
                                       ECode code = ECode::SUCCEEDED)
        : InternalStorageClient(pool, env->metaClient_), env_(env), code_(code) {}

    void chainUpdateEdge(cpp2::UpdateEdgeRequest& req,
                         TermID termOfSrc,
                         folly::Promise<ECode>&& p,
                         folly::EventBase* evb = nullptr) override {
        cpp2::ChainUpdateEdgeRequest chainReq;
        chainReq.set_update_edge_request(req);
        chainReq.set_term(termOfSrc);

        auto* proc = ChainUpdateEdgeProcessorRemote::instance(env_);
        auto f = proc->getFuture();
        proc->process(chainReq);
        auto resp = std::move(f).get();

        p.setValue(code_);
        UNUSED(evb);
    }

private:
    StorageEnv* env_{nullptr};
    ECode code_{ECode::SUCCEEDED};
};

ChainUpdateEdgeTestHelper helper;
TEST(ChainUpdateEdgeTest, updateTest1) {
    fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto mClient = MetaClientTestUpdater::makeDefaultMetaClient();
    env->metaClient_ = mClient.get();

    auto parts = cluster.getTotalParts();
    EXPECT_TRUE(QueryTestUtils::mockEdgeData(env, parts, mockSpaceVidLen));

    LOG(INFO) << "Test updateTest1...";
    auto req = helper.makeDefaultRequest();

    auto pool = std::make_shared<folly::IOThreadPoolExecutor>(3);
    env->txnMan_->iClient_ = std::make_unique<FakeInternalStorageClient>(env, pool);
    auto reversedRequest = helper.reverseRequest(env, req);

    auto* proc = new FakeChainUpdateProcessor(env);
    auto f = proc->getFuture();
    // proc->rcProcessRemote = ECode::SUCCEEDED;
    proc->process(req);
    auto resp = std::move(f).get();

    EXPECT_TRUE(helper.checkResp2(resp));
    EXPECT_TRUE(helper.checkRequestUpdated(env, req));
    EXPECT_TRUE(helper.checkRequestUpdated(env, reversedRequest));
    EXPECT_TRUE(helper.edgeExist(env, req));
    EXPECT_FALSE(helper.primeExist(env, req));
    EXPECT_FALSE(helper.doublePrimeExist(env, req));
}

TEST(ChainUpdateEdgeTest, updateTest2) {
    fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto mClient = MetaClientTestUpdater::makeDefaultMetaClient();
    env->metaClient_ = mClient.get();

    auto parts = cluster.getTotalParts();
    EXPECT_TRUE(QueryTestUtils::mockEdgeData(env, parts, mockSpaceVidLen));

    LOG(INFO) << "Test UpdateEdgeRequest...";
    auto goodRequest = helper.makeDefaultRequest();
    EXPECT_TRUE(helper.edgeExist(env, goodRequest));
    EXPECT_FALSE(helper.primeExist(env, goodRequest));
    EXPECT_FALSE(helper.doublePrimeExist(env, goodRequest));

    auto badRequest = helper.makeInvalidRequest();

    auto* proc = new FakeChainUpdateProcessor(env);
    auto f = proc->getFuture();
    proc->rcProcessRemote = ECode::E_KEY_NOT_FOUND;
    proc->process(badRequest);
    auto resp = std::move(f).get();

    EXPECT_EQ(1, (*resp.result_ref()).failed_parts.size());
    EXPECT_FALSE(helper.checkResp2(resp));
    EXPECT_FALSE(helper.edgeExist(env, badRequest));
    EXPECT_FALSE(helper.primeExist(env, badRequest));
    EXPECT_FALSE(helper.doublePrimeExist(env, badRequest));
}

TEST(ChainUpdateEdgeTest, updateTest3) {
    fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto mClient = MetaClientTestUpdater::makeDefaultMetaClient();
    env->metaClient_ = mClient.get();

    auto parts = cluster.getTotalParts();
    EXPECT_TRUE(QueryTestUtils::mockEdgeData(env, parts, mockSpaceVidLen));

    LOG(INFO) << "Test UpdateEdgeRequest...";
    auto goodRequest = helper.makeDefaultRequest();
    EXPECT_TRUE(helper.edgeExist(env, goodRequest));
    EXPECT_FALSE(helper.primeExist(env, goodRequest));
    EXPECT_FALSE(helper.doublePrimeExist(env, goodRequest));

    auto* proc = new FakeChainUpdateProcessor(env);
    auto f = proc->getFuture();
    proc->rcProcessRemote = ECode::SUCCEEDED;
    proc->rcProcessLocal = ECode::SUCCEEDED;
    proc->process(goodRequest);
    auto resp = std::move(f).get();

    EXPECT_TRUE(helper.edgeExist(env, goodRequest));
    EXPECT_TRUE(helper.primeExist(env, goodRequest));
    EXPECT_FALSE(helper.doublePrimeExist(env, goodRequest));
}

TEST(ChainUpdateEdgeTest, updateTest4) {
    fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto mClient = MetaClientTestUpdater::makeDefaultMetaClient();
    env->metaClient_ = mClient.get();

    auto parts = cluster.getTotalParts();
    EXPECT_TRUE(QueryTestUtils::mockEdgeData(env, parts, mockSpaceVidLen));

    LOG(INFO) << "Test UpdateEdgeRequest...";
    auto goodRequest = helper.makeDefaultRequest();
    EXPECT_TRUE(helper.edgeExist(env, goodRequest));
    EXPECT_FALSE(helper.primeExist(env, goodRequest));
    EXPECT_FALSE(helper.doublePrimeExist(env, goodRequest));

    auto* proc = new FakeChainUpdateProcessor(env);
    auto f = proc->getFuture();
    proc->rcProcessRemote = ECode::E_RPC_FAILURE;
    proc->process(goodRequest);
    auto resp = std::move(f).get();

    EXPECT_TRUE(helper.edgeExist(env, goodRequest));
    EXPECT_FALSE(helper.primeExist(env, goodRequest));
    EXPECT_TRUE(helper.doublePrimeExist(env, goodRequest));
}

// ChainUpdateEdgeTestHelper helper;
TEST(ChainUpdateEdgeTest, updateTest5) {
    fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto mClient = MetaClientTestUpdater::makeDefaultMetaClient();
    env->metaClient_ = mClient.get();

    auto parts = cluster.getTotalParts();
    EXPECT_TRUE(QueryTestUtils::mockEdgeData(env, parts, mockSpaceVidLen));

    LOG(INFO) << "Test UpdateEdgeRequest...";
    auto req = helper.makeDefaultRequest();

    auto* proc = new FakeChainUpdateProcessor(env);
    auto f = proc->getFuture();
    proc->rcProcessRemote = ECode::SUCCEEDED;
    proc->process(req);
    auto resp = std::move(f).get();

    // EXPECT_TRUE(helper.checkResp(req, resp));
    EXPECT_TRUE(helper.checkRequestUpdated(env, req));
    EXPECT_TRUE(helper.edgeExist(env, req));
    EXPECT_FALSE(helper.primeExist(env, req));
    EXPECT_FALSE(helper.doublePrimeExist(env, req));

    auto* proc2 = new FakeChainUpdateProcessor(env);
    auto f2 = proc2->getFuture();
    proc2->rcProcessRemote = ECode::SUCCEEDED;
    proc2->process(req);
    auto resp2 = std::move(f2).get();

    ChainResumeProcessor resumeProc(env);
    ChainResumeProcessorTestHelper testHelper(&resumeProc);
    testHelper.setUpdProc(proc2);
    resumeProc.process();

    EXPECT_TRUE(helper.edgeExist(env, req));
    EXPECT_FALSE(helper.primeExist(env, req));
    EXPECT_FALSE(helper.doublePrimeExist(env, req));
}

TEST(ChainUpdateEdgeTest, updateTest6) {
    fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto mClient = MetaClientTestUpdater::makeDefaultMetaClient();
    env->metaClient_ = mClient.get();

    auto parts = cluster.getTotalParts();
    EXPECT_TRUE(QueryTestUtils::mockEdgeData(env, parts, mockSpaceVidLen));

    LOG(INFO) << "Test UpdateEdgeRequest...";
    auto goodRequest = helper.makeDefaultRequest();
    EXPECT_TRUE(helper.edgeExist(env, goodRequest));
    EXPECT_FALSE(helper.primeExist(env, goodRequest));
    EXPECT_FALSE(helper.doublePrimeExist(env, goodRequest));

    auto badRequest = helper.makeInvalidRequest();
    auto* proc = new FakeChainUpdateProcessor(env);
    auto f = proc->getFuture();
    proc->rcProcessRemote = ECode::E_KEY_NOT_FOUND;
    proc->process(badRequest);
    auto resp = std::move(f).get();

    EXPECT_EQ(1, (*resp.result_ref()).failed_parts.size());
    EXPECT_FALSE(helper.checkResp2(resp));
    EXPECT_FALSE(helper.edgeExist(env, badRequest));
    EXPECT_FALSE(helper.primeExist(env, badRequest));
    EXPECT_FALSE(helper.doublePrimeExist(env, badRequest));

    ChainResumeProcessor resumeProc(env);
    ChainResumeProcessorTestHelper testHelper(&resumeProc);
    testHelper.setUpdProc(proc);
    resumeProc.process();

    EXPECT_TRUE(helper.edgeExist(env, goodRequest));
    EXPECT_FALSE(helper.primeExist(env, goodRequest));
    EXPECT_FALSE(helper.doublePrimeExist(env, goodRequest));

    EXPECT_FALSE(helper.edgeExist(env, badRequest));
    EXPECT_FALSE(helper.primeExist(env, badRequest));
    EXPECT_FALSE(helper.doublePrimeExist(env, badRequest));
}

TEST(ChainUpdateEdgeTest, updateTest7) {
    fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto mClient = MetaClientTestUpdater::makeDefaultMetaClient();
    env->metaClient_ = mClient.get();

    auto parts = cluster.getTotalParts();
    EXPECT_TRUE(QueryTestUtils::mockEdgeData(env, parts, mockSpaceVidLen));

    LOG(INFO) << "Test UpdateEdgeRequest...";
    auto goodRequest = helper.makeDefaultRequest();
    EXPECT_TRUE(helper.edgeExist(env, goodRequest));
    EXPECT_FALSE(helper.primeExist(env, goodRequest));
    EXPECT_FALSE(helper.doublePrimeExist(env, goodRequest));

    auto* proc = new FakeChainUpdateProcessor(env);
    auto f = proc->getFuture();
    proc->rcProcessRemote = ECode::SUCCEEDED;
    proc->rcProcessLocal = ECode::SUCCEEDED;
    proc->process(goodRequest);
    auto resp = std::move(f).get();

    EXPECT_TRUE(helper.edgeExist(env, goodRequest));
    EXPECT_TRUE(helper.primeExist(env, goodRequest));
    EXPECT_FALSE(helper.doublePrimeExist(env, goodRequest));

    auto* proc2 = new FakeChainUpdateProcessor(env);
    proc2->rcProcessRemote = ECode::SUCCEEDED;

    ChainResumeProcessor resumeProc(env);
    ChainResumeProcessorTestHelper testHelper(&resumeProc);
    testHelper.setUpdProc(proc2);
    resumeProc.process();

    EXPECT_TRUE(helper.edgeExist(env, goodRequest));
    EXPECT_FALSE(helper.primeExist(env, goodRequest));
    EXPECT_FALSE(helper.doublePrimeExist(env, goodRequest));
}

TEST(ChainUpdateEdgeTest, updateTest8) {
    fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto mClient = MetaClientTestUpdater::makeDefaultMetaClient();
    env->metaClient_ = mClient.get();

    auto parts = cluster.getTotalParts();
    EXPECT_TRUE(QueryTestUtils::mockEdgeData(env, parts, mockSpaceVidLen));

    LOG(INFO) << "Test UpdateEdgeRequest...";
    auto goodRequest = helper.makeDefaultRequest();
    EXPECT_TRUE(helper.edgeExist(env, goodRequest));
    EXPECT_FALSE(helper.primeExist(env, goodRequest));
    EXPECT_FALSE(helper.doublePrimeExist(env, goodRequest));

    auto* proc = new FakeChainUpdateProcessor(env);
    auto f = proc->getFuture();
    proc->rcProcessRemote = ECode::E_RPC_FAILURE;

    proc->process(goodRequest);
    auto resp = std::move(f).get();

    EXPECT_TRUE(helper.edgeExist(env, goodRequest));
    EXPECT_FALSE(helper.primeExist(env, goodRequest));
    EXPECT_TRUE(helper.doublePrimeExist(env, goodRequest));

    auto* proc2 = new FakeChainUpdateProcessor(env);
    proc2->rcProcessRemote = ECode::SUCCEEDED;

    ChainResumeProcessor resumeProc(env);
    ChainResumeProcessorTestHelper testHelper(&resumeProc);
    testHelper.setUpdProc(proc2);
    resumeProc.process();

    EXPECT_TRUE(helper.edgeExist(env, goodRequest));
    EXPECT_FALSE(helper.primeExist(env, goodRequest));
    EXPECT_FALSE(helper.doublePrimeExist(env, goodRequest));
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, false);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}



//              ***** Test Plan *****
/**
 * @brief updateTest1
 *                  insert      update
 *  prepareLocal    succeed     succeed
 *  processRemote   skip        succeed
 *  processLocal    succeed     succeed
 *  expect: edge            true
 *          edge prime      false
 *          double prime    false
 *          prop changed    true
 */

/**
 * @brief updateTest2
 *                  insert      update
 *  prepareLocal    failed     succeed
 *  processRemote   skip       succeed
 *  processLocal    failed     succeed
 *  expect: edge            false
 *          edge prime      false
 *          double prime    false
 *          prop changed    true
 */

/**
 * @brief updateTest3
 *                  insert      update
 *  prepareLocal    succeed     succeed
 *  processRemote   skip        failed
 *  processLocal    skip        failed
 *  expect: edge            true
 *          edge prime      true
 *          double prime    false
 *          prop changed    false
 */

/**
 * @brief updateTest4
 *                  insert      update
 *  prepareLocal    succeed     succeed
 *  processRemote   skip        outdate
 *  processLocal    skip        succeed
 *  expect: edge            true
 *          edge prime      false
 *          double prime    true
 *          prop changed    false
 */

/**
 * @brief updateTest5 (update1 + resume)
 *                  insert      update
 *  prepareLocal    succeed     succeed
 *  processRemote   skip        succeed
 *  processLocal    succeed     succeed
 *  expect: edge            true
 *          edge prime      false
 *          double prime    false
 *          prop changed    true
 */

/**
 * @brief updateTest6 (update2 + resume)
 *                  insert      update
 *  prepareLocal    failed     succeed
 *  processRemote   skip       succeed
 *  processLocal    failed     succeed
 *  expect: edge            false
 *          edge prime      false
 *          double prime    false
 *          prop changed    true
 */

/**
 * @brief updateTest7 (updateTest3 + resume)
 *                  update      resume
 *  prepareLocal    succeed     succeed
 *  processRemote   skip        failed
 *  processLocal    skip        failed
 *  expect: edge            true
 *          edge prime      true
 *          double prime    false
 *          prop changed    false
 */

/**
 * @brief updateTest8
 *                  insert      resume
 *  prepareLocal    succeed     succeed
 *  processRemote   skip        outdate
 *  processLocal    skip        succeed
 *  expect: edge            true
 *          edge prime      false
 *          double prime    true
 *          prop changed    false
 */

//              ***** End Test Plan *****
