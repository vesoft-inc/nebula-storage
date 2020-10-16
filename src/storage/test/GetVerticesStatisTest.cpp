/* Copyright (c) 2020 vesoft inc. All rights reserved.
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
#include "storage/mutate/AddVerticesProcessor.h"
#include "storage/mutate/UpdateVertexProcessor.h"
#include "storage/mutate/DeleteVerticesProcessor.h"
#include "storage/query/GetVerticesStatisProcessor.h"
#include "storage/test/TestUtils.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"
#include "common/expression/ConstantExpression.h"

namespace nebula {
namespace storage {

cpp2::GetVerticesStatisRequest
buildGetVerticesStatisRequest(int32_t parts, IndexID indexId) {
    cpp2::GetVerticesStatisRequest req;
    req.space_id = 1;
    for (int32_t i = 1; i <= parts; i++) {
        req.parts.emplace_back(i);
    }
    req.index = indexId;
    return req;
}

TEST(GetVerticesStatisTest, SimpleTest) {
    fs::TempDir rootPath("/tmp/AddVerticesTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    // Empty data test
    {
        // Get all vertex count in tag 1
        auto* processor = GetVerticesStatisProcessor::instance(env, nullptr);
        LOG(INFO) << "Build GetVerticesStatisRequest...";
        cpp2::GetVerticesStatisRequest req = buildGetVerticesStatisRequest(parts, 5);

        LOG(INFO) << "Test GetVerticesStatisProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        EXPECT_EQ(0, resp.count);
    }
    {
        // Get all vertex count in tag 2
        auto* processor = GetVerticesStatisProcessor::instance(env, nullptr);
        LOG(INFO) << "Build GetVerticesStatisRequest...";
        cpp2::GetVerticesStatisRequest req = buildGetVerticesStatisRequest(parts, 6);

        LOG(INFO) << "Test GetVerticesStatisProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        EXPECT_EQ(0, resp.count);
    }
    {
        // Get all vertex count in space
        auto* processor = GetVerticesStatisProcessor::instance(env, nullptr);
        LOG(INFO) << "Build GetVerticesStatisRequest...";
        cpp2::GetVerticesStatisRequest req = buildGetVerticesStatisRequest(parts, 4);

        LOG(INFO) << "Test GetVerticesStatisProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        EXPECT_EQ(0, resp.count);
    }
    // Add vertex data
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
        // The number of vertices is 81
        checkAddVerticesData(req, env, 81, 0);
    }
    {
        // Get all vertex count in tag 1
        auto* processor = GetVerticesStatisProcessor::instance(env, nullptr);
        LOG(INFO) << "Build GetVerticesStatisRequest...";
        cpp2::GetVerticesStatisRequest req = buildGetVerticesStatisRequest(parts, 5);

        LOG(INFO) << "Test GetVerticesStatisProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        EXPECT_EQ(51, resp.count);
    }
    {
        // Get all vertex count in tag 2
        auto* processor = GetVerticesStatisProcessor::instance(env, nullptr);
        LOG(INFO) << "Build GetVerticesStatisRequest...";
        cpp2::GetVerticesStatisRequest req = buildGetVerticesStatisRequest(parts, 6);

        LOG(INFO) << "Test GetVerticesStatisProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        EXPECT_EQ(30, resp.count);
    }
    {
        // Get all vertex count in space
        auto* processor = GetVerticesStatisProcessor::instance(env, nullptr);
        LOG(INFO) << "Build GetVerticesStatisRequest...";
        cpp2::GetVerticesStatisRequest req = buildGetVerticesStatisRequest(parts, 4);

        LOG(INFO) << "Test GetVerticesStatisProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        EXPECT_EQ(81, resp.count);
    }
    // update vertex data
    {
        GraphSpaceID spaceId = 1;
        TagID tagId = 1;

        LOG(INFO) << "Build UpdateVertexRequest...";
        cpp2::UpdateVertexRequest req;

        req.set_space_id(spaceId);
        auto partId = std::hash<std::string>()("Tim Duncan") % parts + 1;
        VertexID vertexId("Tim Duncan");
        req.set_part_id(partId);
        req.set_vertex_id(vertexId);
        req.set_tag_id(tagId);

        LOG(INFO) << "Build updated props...";
        std::vector<cpp2::UpdatedProp> updatedProps;
        // int: player.age = 45
        cpp2::UpdatedProp uProp1;
        uProp1.set_name("age");
        ConstantExpression val1(45L);
        uProp1.set_value(Expression::encode(val1));
        updatedProps.emplace_back(uProp1);

        // string: player.country= China
        cpp2::UpdatedProp uProp2;
        uProp2.set_name("country");
        std::string col4new("China");
        ConstantExpression val2(col4new);
        uProp2.set_value(Expression::encode(val2));
        updatedProps.emplace_back(uProp2);
        req.set_updated_props(std::move(updatedProps));
        req.set_insertable(false);

        LOG(INFO) << "Test UpdateVertexRequest...";
        auto* processor = UpdateVertexProcessor::instance(env, nullptr);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();

        LOG(INFO) << "Check the results...";
        EXPECT_EQ(0, resp.result.failed_parts.size());
    }
    {
        // Get all vertex count in tag 1
        auto* processor = GetVerticesStatisProcessor::instance(env, nullptr);
        LOG(INFO) << "Build GetVerticesStatisRequest...";
        cpp2::GetVerticesStatisRequest req = buildGetVerticesStatisRequest(parts, 5);

        LOG(INFO) << "Test GetVerticesStatisProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        EXPECT_EQ(51, resp.count);
    }
    {
        // Get all vertex count in space
        auto* processor = GetVerticesStatisProcessor::instance(env, nullptr);
        LOG(INFO) << "Build GetVerticesStatisRequest...";
        cpp2::GetVerticesStatisRequest req = buildGetVerticesStatisRequest(parts, 4);

        LOG(INFO) << "Test GetVerticesStatisProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        EXPECT_EQ(81, resp.count);
    }
    // upsert new vertex
    {
        GraphSpaceID spaceId = 1;
        TagID tagId = 1;
        LOG(INFO) << "Build UpdateVertexRequest...";
        cpp2::UpdateVertexRequest req;

        req.set_space_id(spaceId);
        auto partId = std::hash<std::string>()("Brandon Ingram") % parts + 1;
        VertexID vertexId("Brandon Ingram");
        req.set_part_id(partId);
        req.set_vertex_id(vertexId);
        req.set_tag_id(tagId);

        LOG(INFO) << "Build updated props...";
        std::vector<cpp2::UpdatedProp> updatedProps;
        // string: player.name= "Brandon Ingram"
        cpp2::UpdatedProp uProp1;
        uProp1.set_name("name");
        std::string colnew("Brandon Ingram");
        ConstantExpression val1(colnew);
        uProp1.set_value(Expression::encode(val1));
        updatedProps.emplace_back(uProp1);

        // int: player.age = 20
        cpp2::UpdatedProp uProp2;
        uProp2.set_name("age");
        ConstantExpression val2(20L);
        uProp2.set_value(Expression::encode(val2));
        updatedProps.emplace_back(uProp2);
        req.set_updated_props(std::move(updatedProps));
        req.set_insertable(true);

        LOG(INFO) << "Test UpdateVertexRequest...";
        auto* processor = UpdateVertexProcessor::instance(env, nullptr);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();

        LOG(INFO) << "Check the results...";
        EXPECT_EQ(0, resp.result.failed_parts.size());
    }
    {
        // Get all vertex count in tag 1
        auto* processor = GetVerticesStatisProcessor::instance(env, nullptr);
        LOG(INFO) << "Build GetVerticesStatisRequest...";
        cpp2::GetVerticesStatisRequest req = buildGetVerticesStatisRequest(parts, 5);

        LOG(INFO) << "Test GetVerticesStatisProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        EXPECT_EQ(52, resp.count);
    }
    {
        // Get all vertex count in space
        auto* processor = GetVerticesStatisProcessor::instance(env, nullptr);
        LOG(INFO) << "Build GetVerticesStatisRequest...";
        cpp2::GetVerticesStatisRequest req = buildGetVerticesStatisRequest(parts, 4);

        LOG(INFO) << "Test GetVerticesStatisProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        EXPECT_EQ(82, resp.count);
    }
    {
        // Delete vertex data
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

        // All the added datas are deleted except upsert vertex data,
        // the number of vertices is 1
        checkVerticesData(spaceVidLen, req.space_id, req.parts, env, 0);
    }
    {
        // Get all vertex count in tag 1
        auto* processor = GetVerticesStatisProcessor::instance(env, nullptr);
        LOG(INFO) << "Build GetVerticesStatisRequest...";
        cpp2::GetVerticesStatisRequest req = buildGetVerticesStatisRequest(parts, 5);

        LOG(INFO) << "Test GetVerticesStatisProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        EXPECT_EQ(1, resp.count);
    }
    {
        // Get all vertex count in tag 2
        auto* processor = GetVerticesStatisProcessor::instance(env, nullptr);
        LOG(INFO) << "Build GetVerticesStatisRequest...";
        cpp2::GetVerticesStatisRequest req = buildGetVerticesStatisRequest(parts, 6);

        LOG(INFO) << "Test GetVerticesStatisProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        EXPECT_EQ(0, resp.count);
    }
    {
        // Get all vertex count in space
        auto* processor = GetVerticesStatisProcessor::instance(env, nullptr);
        LOG(INFO) << "Build GetVerticesStatisRequest...";
        cpp2::GetVerticesStatisRequest req = buildGetVerticesStatisRequest(parts, 4);

        LOG(INFO) << "Test GetVerticesStatisProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        EXPECT_EQ(1, resp.count);
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

