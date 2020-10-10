/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "common/interface/gen-cpp2/common_types.h"
#include "utils/NebulaKeyUtils.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "storage/mutate/AddEdgesProcessor.h"
#include "storage/test/TestUtils.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"

namespace nebula {
namespace storage {

TEST(AddEdgesTest, SimpleTest) {
    fs::TempDir rootPath("/tmp/AddEdgesTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();

    auto* processor = AddEdgesProcessor::instance(env, nullptr);

    LOG(INFO) << "Build AddEdgesRequest...";
    cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq();

    LOG(INFO) << "Test AddEdgesProcessor...";
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());

    LOG(INFO) << "Check data in kv store...";
    // The number of data in serve is 334
    checkAddEdgesData(req, env, 334, 0);
}

TEST(AddEdgesTest, SpecifyPropertyNameTest) {
    fs::TempDir rootPath("/tmp/AddEdgesTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();

    auto* processor = AddEdgesProcessor::instance(env, nullptr);

    LOG(INFO) << "Build AddEdgesRequest...";
    cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesSpecifiedOrderReq();

    LOG(INFO) << "Test AddEdgesProcessor...";
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());

    LOG(INFO) << "Check data in kv store...";
    // The number of data in serve is 334
    checkAddEdgesData(req, env, 334, 1);
}

TEST(AddEdgesTest, MultiVersionTest) {
    FLAGS_enable_multi_versions = true;
    fs::TempDir rootPath("/tmp/AddEdgesTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();

    LOG(INFO) << "Build AddEdgesRequest...";
    cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq();
    cpp2::AddEdgesRequest specifiedOrderReq = mock::MockData::mockAddEdgesSpecifiedOrderReq();

    {
        LOG(INFO) << "AddEdgesProcessor...";
        auto* processor = AddEdgesProcessor::instance(env, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
    }
    {
        LOG(INFO) << "AddEdgesProcessor...";
        auto* processor = AddEdgesProcessor::instance(env, nullptr);
        auto fut = processor->getFuture();
        processor->process(specifiedOrderReq);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
    }

    LOG(INFO) << "Check data in kv store...";
    // The number of data in serve is 668
    checkAddEdgesData(req, env, 668, 2);
    FLAGS_enable_multi_versions = false;
}

TEST(AddEdgesTest, CheckData) {
    fs::TempDir rootPath("/tmp/AddEdgesTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    GraphSpaceID spaceId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(status.ok());
    auto spaceVidLen = status.value();

    auto* processor = AddEdgesProcessor::instance(env, nullptr);

    LOG(INFO) << "Build AddEdgesRequest...";
    cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq();

    LOG(INFO) << "Test AddEdgesProcessor...";
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());

    // The number of edges is 334, only serves_ count
    // The index data of the count of serves_
    // Normal index data count: 167 (equal to forward edge data count)
    // Vertex index data count: 167 (equal to forward edge data count)
    // Vertex_count index data count: 6 (equal to part count of current space)
    checkEdgesDataAndIndexData(spaceVidLen, spaceId, parts, env, 334, 340);
}

}  // namespace storage
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}


