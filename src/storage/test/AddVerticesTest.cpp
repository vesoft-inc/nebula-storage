/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "common/NebulaKeyUtils.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "fs/TempDir.h"
#include "storage/mutate/AddVerticesProcessor.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"
#include "interface/gen-cpp2/storage_types.h"
#include "interface/gen-cpp2/common_types.h"

namespace nebula {
namespace storage {

TEST(AddVerticesTest, SimpleTest) {
    fs::TempDir rootPath("/tmp/AddVerticesTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();

    auto* processor = AddVerticesProcessor::instance(env, nullptr);

    LOG(INFO) << "Build AddVerticesRequest...";
    cpp2::AddVerticesRequest req = mock::MockData::mockAddVerticesReq();

    LOG(INFO) << "Test AddVerticesProcessor...";
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());

    LOG(INFO) << "Check data in kv store...";
    auto ret = env->schemaMan_->getSpaceVidLen(1);
    auto spaceVidLen = ret.value();

    int totalCount = 0;
    for (auto& part : req.parts) {
        auto partId = part.first;
        auto newVertexVec = part.second;

        auto count = 0;
        for (auto& newVertex : newVertexVec) {
            auto vid = newVertex.id;
            auto newTagVec = newVertex.tags;

            for (auto& newTag : newTagVec) {
                auto tagId = newTag.tag_id;
                auto prefix = NebulaKeyUtils::vertexPrefix(spaceVidLen, partId, vid, tagId);
                std::unique_ptr<kvstore::KVIterator> iter;
                EXPECT_EQ(kvstore::ResultCode::SUCCEEDED,
                          env->kvstore_->prefix(1, partId, prefix, &iter));

                auto schema = env->schemaMan_->getTagSchema(1, tagId);
                EXPECT_TRUE(schema != NULL);

                while (iter && iter->valid()) {
                    auto reader = RowReader::getRowReader(schema.get(), iter->val());
                    // For players tagId is 1
                    Value val;
                    if (tagId == 1) {
                        for (auto i = 0; i < 9; i++) {
                            val = reader->getValueByIndex(i);
                            EXPECT_EQ(newTag.props[i], val);
                        }
                        if (newTag.props.size() >= 10) {
                            val = reader->getValueByIndex(9);
                            EXPECT_EQ(newTag.props[9], val);
                            if (newTag.props.size() == 11) {
                                val = reader->getValueByIndex(10);
                                EXPECT_EQ(newTag.props[10], val);
                            }
                        }
                    } else if (tagId == 2) {
                        // For teams tagId is 2
                        val = reader->getValueByIndex(0);
                        EXPECT_EQ(newTag.props[0], val);
                    } else {
                        // Impossible to get here
                        ASSERT_TRUE(false);
                    }
                    count++;
                    iter->next();
                }
            }
        }
        // There is only one tag per vertex, either tagId is 1 or tagId is 2
        EXPECT_EQ(newVertexVec.size(), count);
        totalCount += count;
    }
    // The number of vertices is 81
    EXPECT_EQ(81, totalCount);
}


TEST(AddVerticesTest, SpecifyPropertyNameTest) {
    fs::TempDir rootPath("/tmp/AddVerticesTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();

    auto* processor = AddVerticesProcessor::instance(env, nullptr);

    LOG(INFO) << "Build AddVerticesRequest...";
    cpp2::AddVerticesRequest req = mock::MockData::mockAddVerticesSpecifiedOrderReq();

    LOG(INFO) << "Test AddVerticesProcessor...";
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());

    LOG(INFO) << "Check data in kv store...";
    auto ret = env->schemaMan_->getSpaceVidLen(1);
    auto spaceVidLen = ret.value();

    int totalCount = 0;
    for (auto& part : req.parts) {
        auto partId = part.first;
        auto newVertexVec = part.second;
        auto count = 0;
        for (auto& newVertex : newVertexVec) {
            auto vid = newVertex.id;
            auto newTagVec = newVertex.tags;

            for (auto& newTag : newTagVec) {
                auto tagId = newTag.tag_id;
                auto prefix = NebulaKeyUtils::vertexPrefix(spaceVidLen, partId, vid, tagId);
                std::unique_ptr<kvstore::KVIterator> iter;
                EXPECT_EQ(kvstore::ResultCode::SUCCEEDED,
                          env->kvstore_->prefix(1, partId, prefix, &iter));

                auto schema = env->schemaMan_->getTagSchema(1, tagId);
                EXPECT_TRUE(schema != NULL);

                while (iter && iter->valid()) {
                    auto reader = RowReader::getRowReader(schema.get(), iter->val());
                    // For players tagId is 1
                    Value val;
                    if (tagId == 1) {
                        // For the specified attribute order, the default value and nullable columns
                        // always use the default value or null value
                        for (auto i = 0; i < 9; i++) {
                            val = reader->getValueByIndex(i);
                            EXPECT_EQ(newTag.props[8 - i], val);
                        }
                    } else if (tagId == 2) {
                        // For teams tagId is 2
                        val = reader->getValueByIndex(0);
                        EXPECT_EQ(newTag.props[0], val);
                    } else {
                        // Impossible to get here
                        ASSERT_TRUE(false);
                    }
                    count++;
                    iter->next();
                }
            }
        }
        // There is only one tag per vertex, either tagId is 1 or tagId is 2
        EXPECT_EQ(newVertexVec.size(), count);
        totalCount += count;
    }
    // The number of vertices is 81
    EXPECT_EQ(81, totalCount);
}

TEST(AddVerticesTest, MultiVersionTest) {
    fs::TempDir rootPath("/tmp/AddVerticesTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();

    LOG(INFO) << "Build AddVerticesRequest...";
    cpp2::AddVerticesRequest req = mock::MockData::mockAddVerticesReq();
    cpp2::AddVerticesRequest specifiedOrderReq = mock::MockData::mockAddVerticesSpecifiedOrderReq();

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
    auto ret = env->schemaMan_->getSpaceVidLen(1);
    auto spaceVidLen = ret.value();

    int totalCount = 0;
    for (auto& part : req.parts) {
        auto partId = part.first;
        auto newVertexVec = part.second;
        auto count = 0;
        for (auto& newVertex : newVertexVec) {
            auto vid = newVertex.id;
            auto newTagVec = newVertex.tags;

            for (auto& newTag : newTagVec) {
                auto tagId = newTag.tag_id;
                auto prefix = NebulaKeyUtils::vertexPrefix(spaceVidLen, partId, vid, tagId);
                std::unique_ptr<kvstore::KVIterator> iter;
                EXPECT_EQ(kvstore::ResultCode::SUCCEEDED,
                          env->kvstore_->prefix(1, partId, prefix, &iter));

                auto schema = env->schemaMan_->getTagSchema(1, tagId);
                EXPECT_TRUE(schema != NULL);
                int num = 0;
                while (iter && iter->valid()) {
                    auto reader = RowReader::getRowReader(schema.get(), iter->val());
                    // For players tagId is 1
                    Value val;
                    if (tagId == 1) {
                        if (num == 0) {
                            // For the specified attribute order, the default value and nullable
                            // columns always use the default value or null value
                            for (auto i = 0; i < 9; i++) {
                                val = reader->getValueByIndex(i);
                                EXPECT_EQ(newTag.props[i], val);
                            }
                            val = reader->getValueByIndex(9);
                            EXPECT_EQ("America", val.getStr());
                        } else {
                            for (auto i = 0; i < 9; i++) {
                                val = reader->getValueByIndex(i);
                                EXPECT_EQ(newTag.props[i], val);
                            }
                            if (newTag.props.size() >= 10) {
                                val = reader->getValueByIndex(9);
                                EXPECT_EQ(newTag.props[9], val);
                                if (newTag.props.size() == 11) {
                                    val = reader->getValueByIndex(10);
                                    EXPECT_EQ(newTag.props[10], val);
                                }
                            }
                         }
                    }

                    if (tagId == 2) {
                        // For teams tagId is 2
                        val = reader->getValueByIndex(0);
                        EXPECT_EQ(newTag.props[0], val);
                    }
                    num++;
                    count++;
                    iter->next();
                }
                EXPECT_EQ(2, num);
            }
        }
        // There is only one tag per vertex, either tagId is 1 or tagId is 2
        EXPECT_EQ(newVertexVec.size(), count / 2);
        totalCount += count;
    }
    // The number of vertices  is 162
    EXPECT_EQ(162, totalCount);
}

}  // namespace storage
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}


