/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "common/interface/gen-cpp2/common_types.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "storage/mutate/DeleteVerticesProcessor.h"
#include "storage/mutate/AddVerticesProcessor.h"
#include "utils/NebulaKeyUtils.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"
#include "storage/test/TestUtils.h"

namespace nebula {
namespace storage {

TEST(DeleteVerticesTest, SimpleTest) {
    fs::TempDir rootPath("/tmp/DeleteVertexTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();

    // Add vertices
    {
        auto* processor = AddVerticesProcessor::instance(env, nullptr);

        LOG(INFO) << "Build AddVerticesRequest...";
        cpp2::AddVerticesRequest req = mock::MockData::mockAddVerticesReq();

        LOG(INFO) << "Test AddVerticesProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());

        LOG(INFO) << "Check data in kv store...";
        // The number of data in players and teams is 81
        checkAddVerticesData(req, env, 81, 0);
    }

    // Delete vertices
    {
        auto* processor = DeleteVerticesProcessor::instance(env, nullptr);

        LOG(INFO) << "Build DeleteVerticesRequest...";
        cpp2::DeleteVerticesRequest req = mock::MockData::mockDeleteVerticesReq();

        LOG(INFO) << "Test DeleteVerticesProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());

        LOG(INFO) << "Check data in kv store...";
        auto ret = env->schemaMan_->getSpaceVidLen(req.space_id);
        EXPECT_TRUE(ret.ok());
        auto spaceVidLen = ret.value();

        // All the added datas are deleted, the number of vertices is 0
        checkVerticesData(spaceVidLen, req.space_id, req.parts, env, 0);
    }
}

TEST(DeleteVerticesTest, MultiVersionTest) {
    FLAGS_enable_multi_versions = true;
    fs::TempDir rootPath("/tmp/DeleteVertexTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();

    // Add vertices
    {
        LOG(INFO) << "Build AddVerticesRequest...";
        cpp2::AddVerticesRequest req = mock::MockData::mockAddVerticesReq();
        cpp2::AddVerticesRequest specifiedOrderReq =
            mock::MockData::mockAddVerticesSpecifiedOrderReq();

        {
            LOG(INFO) << "AddVerticesProcessor...";
            auto* processor = AddVerticesProcessor::instance(env, nullptr);
            auto fut = processor->getFuture();
            processor->process(req);
            auto resp = std::move(fut).get();
            EXPECT_EQ(0, resp.result.failed_parts.size());
        }
        {
            LOG(INFO) << "AddVerticesProcessor...";
            auto* processor = AddVerticesProcessor::instance(env, nullptr);
            auto fut = processor->getFuture();
            processor->process(specifiedOrderReq);
            auto resp = std::move(fut).get();
            EXPECT_EQ(0, resp.result.failed_parts.size());
        }

        LOG(INFO) << "Check data in kv store...";
        // The number of vertices is 162
        checkAddVerticesData(req, env, 162, 2);
    }

    // Delete vertices
    {
        auto* processor = DeleteVerticesProcessor::instance(env, nullptr);

        LOG(INFO) << "Build DeleteVerticesRequest...";
        cpp2::DeleteVerticesRequest req = mock::MockData::mockDeleteVerticesReq();

        LOG(INFO) << "Test DeleteVerticesProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());

        LOG(INFO) << "Check data in kv store...";
        auto ret = env->schemaMan_->getSpaceVidLen(req.space_id);
        EXPECT_TRUE(ret.ok());
        auto spaceVidLen = ret.value();

        // All the added datas are deleted, the number of vertices is 0
        checkVerticesData(spaceVidLen, req.space_id, req.parts, env, 0);
    }
    FLAGS_enable_multi_versions = false;
}

// Check data and index data after delete vertex data
TEST(DeleteVerticesTest, CheckData) {
    fs::TempDir rootPath("/tmp/DeleteVertexTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    GraphSpaceID spaceId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(status.ok());
    auto spaceVidLen = status.value();

    // Add vertices
    {
        auto* processor = AddVerticesProcessor::instance(env, nullptr);

        LOG(INFO) << "Build AddVerticesRequest...";
        cpp2::AddVerticesRequest req = mock::MockData::mockAddVerticesReq();

        LOG(INFO) << "Test AddVerticesProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());

        // The number of vertices is 81, the count of players_ and teams_
        // The index data of the count of players_ and teams_
        // Normal index data count: 81 (equal to data count)
        // Vertex index data count: 81 (equal to data count)
        // Vertex_count index data count: 6 (equal to part count of current space)
        checkVerticesDataAndIndexData(spaceVidLen, spaceId, parts, env, 81, 168);
    }

    // Delete vertices
    {
        auto* processor = DeleteVerticesProcessor::instance(env, nullptr);

        LOG(INFO) << "Build DeleteVerticesRequest...";
        cpp2::DeleteVerticesRequest req = mock::MockData::mockDeleteVerticesReq();

        LOG(INFO) << "Test DeleteVerticesProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());

        // The number of vertices is 0, the count of players_ and teams_
        // The index data of the count of players_ and teams_
        // Normal index data count: 0 (equal to data count)
        // Vertex index data count: 0 (equal to data count)
        // Vertex_count index data count: 0 (equal to part count of current space)
        checkVerticesDataAndIndexData(spaceVidLen, spaceId, parts, env, 0, 0);
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

