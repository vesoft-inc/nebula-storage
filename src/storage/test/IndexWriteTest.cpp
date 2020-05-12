/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "common/NebulaKeyUtils.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "fs/TempDir.h"
#include "storage/mutate/AddEdgesProcessor.h"
#include "storage/mutate/AddVerticesProcessor.h"
#include "storage/mutate/DeleteEdgesProcessor.h"
#include "storage/mutate/DeleteVerticesProcessor.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"
#include "interface/gen-cpp2/storage_types.h"
#include "interface/gen-cpp2/common_types.h"
#include "storage/test/TestUtils.h"
#include "common/NebulaKeyUtils.h"

namespace nebula {
namespace storage {

std::string convertVertexId(size_t vIdLen, int32_t vId) {
    std::string id;
    id.reserve(vIdLen);
    id.append(reinterpret_cast<const char*>(&vId), sizeof(vId))
      .append(vIdLen - sizeof(vId), '\0');
    return id;
}

int64_t verifyResultNum(GraphSpaceID spaceId, PartitionID partId,
                        const std::string& prefix,
                        nebula::kvstore::KVStore *kv) {
    std::unique_ptr<kvstore::KVIterator> iter;
    EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, kv->prefix(spaceId, partId, prefix, &iter));
    int64_t rowCount = 0;
    while (iter->valid()) {
        rowCount++;
        iter->next();
    }
    return rowCount;
}

TEST(IndexTest, SimpleVerticesTest) {
    fs::TempDir rootPath("/tmp/SimpleVerticesTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto vIdLen = env->schemaMan_->getSpaceVidLen(1).value();

    // verify insert
    {
        cpp2::AddVerticesRequest req;
        req.set_space_id(1);
        req.set_overwritable(true);
        // mock v2 vertices
        for (auto partId = 1; partId <= 6; partId++) {
            nebula::storage::cpp2::NewVertex newVertex;
            nebula::storage::cpp2::NewTag newTag;
            newTag.set_tag_id(3);
            const Date date = {2020, 2, 20};
            const DateTime dt = {2020, 2, 20, 10, 30, 45, -8 * 3600};
            std::vector<Value>  props;
            props.emplace_back(Value(true));
            props.emplace_back(Value(1L));
            props.emplace_back(Value(1.1f));
            props.emplace_back(Value(1.1f));
            props.emplace_back(Value("string"));
            props.emplace_back(Value(1L));
            props.emplace_back(Value(1L));
            props.emplace_back(Value(1L));
            props.emplace_back(Value(1L));
            props.emplace_back(Value(std::move(date)));
            props.emplace_back(Value(std::move(dt)));
            newTag.set_props(std::move(props));
            std::vector<nebula::storage::cpp2::NewTag> newTags;
            newTags.push_back(std::move(newTag));
            newVertex.set_id(convertVertexId(vIdLen, partId));
            newVertex.set_tags(std::move(newTags));
            req.parts[partId].emplace_back(std::move(newVertex));
        }
        auto* processor = AddVerticesProcessor::instance(env, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());

        LOG(INFO) << "Check insert data...";
        for (auto partId = 1; partId <= 6; partId++) {
            auto prefix = NebulaKeyUtils::vertexPrefix(vIdLen, partId,
                                                       convertVertexId(vIdLen, partId));
            auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_);
            EXPECT_EQ(1, retNum);
        }

        LOG(INFO) << "Check insert index...";
        for (auto partId = 1; partId <= 6; partId++) {
            auto prefix = IndexKeyUtils::indexPrefix(partId, 3);
            auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_);
            EXPECT_EQ(1, retNum);
        }
    }
    // verify delete
    {
        auto* processor = DeleteVerticesProcessor::instance(env, nullptr);
        cpp2::DeleteVerticesRequest req;
        req.set_space_id(1);
        for (auto partId = 1; partId <= 6; partId++) {
            std::vector<VertexID> vertices;
            vertices.emplace_back(convertVertexId(vIdLen, partId));
            req.parts[partId] = std::move(vertices);
        }
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());

        LOG(INFO) << "Check delete data...";
        for (auto partId = 1; partId <= 6; partId++) {
            auto prefix = NebulaKeyUtils::vertexPrefix(vIdLen, partId,
                                                       convertVertexId(vIdLen, partId));
            auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_);
            EXPECT_EQ(0, retNum);
        }

        LOG(INFO) << "Check delete index...";
        for (auto partId = 1; partId <= 6; partId++) {
            auto prefix = IndexKeyUtils::indexPrefix(partId, 3);
            auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_);
            EXPECT_EQ(0, retNum);
        }
    }
}

TEST(IndexTest, SimpleEdgesTest) {
    fs::TempDir rootPath("/tmp/SimpleEdgesTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto vIdLen = env->schemaMan_->getSpaceVidLen(1).value();

    // verify insert
    {
        cpp2::AddEdgesRequest req;
        req.set_space_id(1);
        req.set_overwritable(true);
        // mock v2 edges
        for (auto partId = 1; partId <= 6; partId++) {
            nebula::storage::cpp2::NewEdge newEdge;
            nebula::storage::cpp2::EdgeKey edgeKey;
            edgeKey.set_src(convertVertexId(vIdLen, partId));
            edgeKey.set_edge_type(101);
            edgeKey.set_ranking(0);
            edgeKey.set_dst(convertVertexId(vIdLen, partId + 6));
            newEdge.set_key(std::move(edgeKey));
            std::vector<Value>  props;
            props.emplace_back(Value("col1"));
            props.emplace_back(Value("col2"));
            props.emplace_back(Value(3L));
            props.emplace_back(Value(4L));
            props.emplace_back(Value(5L));
            props.emplace_back(Value(6L));
            props.emplace_back(Value(7.7F));
            newEdge.set_props(std::move(props));
            req.parts[partId].emplace_back(newEdge);
            newEdge.key.set_edge_type(-101);
            req.parts[partId].emplace_back(std::move(newEdge));
        }
        auto* processor = AddEdgesProcessor::instance(env, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());

        LOG(INFO) << "Check insert data...";
        for (auto partId = 1; partId <= 6; partId++) {
            auto prefix = NebulaKeyUtils::partPrefix(partId);
            auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_);
            EXPECT_EQ(2, retNum);
        }

        LOG(INFO) << "Check insert index...";
        for (auto partId = 1; partId <= 6; partId++) {
            auto prefix = IndexKeyUtils::indexPrefix(partId, 101);
            auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_);
            EXPECT_EQ(1, retNum);
        }
    }
    // verify delete
    {
        auto* processor = DeleteEdgesProcessor::instance(env);
        cpp2::DeleteEdgesRequest req;
        req.set_space_id(1);
        for (auto partId = 1; partId <= 6; partId++) {
            nebula::storage::cpp2::EdgeKey edgeKey;
            edgeKey.set_src(convertVertexId(vIdLen, partId));
            edgeKey.set_edge_type(101);
            edgeKey.set_ranking(0);
            edgeKey.set_dst(convertVertexId(vIdLen, partId + 6));
            req.parts[partId].emplace_back(edgeKey);
            edgeKey.set_edge_type(-101);
            req.parts[partId].emplace_back(std::move(edgeKey));
        }
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());

        LOG(INFO) << "Check delete data...";
        for (auto partId = 1; partId <= 6; partId++) {
            auto prefix = NebulaKeyUtils::partPrefix(partId);
            auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_);
            EXPECT_EQ(0, retNum);
        }

        LOG(INFO) << "Check delete index...";
        for (auto partId = 1; partId <= 6; partId++) {
            auto prefix = IndexKeyUtils::indexPrefix(partId, 101);
            auto retNum = verifyResultNum(1, partId, prefix, env->kvstore_);
            EXPECT_EQ(0, retNum);
        }
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


