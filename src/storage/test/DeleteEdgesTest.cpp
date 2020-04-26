/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "fs/TempDir.h"
#include "storage/mutate/DeleteEdgesProcessor.h"
#include "storage/mutate/AddEdgesProcessor.h"
#include "common/NebulaKeyUtils.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"
#include "interface/gen-cpp2/storage_types.h"
#include "interface/gen-cpp2/common_types.h"

namespace nebula {
namespace storage {

TEST(DeleteEdgesTest, SimpleTest) {
    fs::TempDir rootPath("/tmp/DeleteEdgesTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();

    // Add edges
    {
        auto* processor = AddEdgesProcessor::instance(env, nullptr);

        LOG(INFO) << "Build AddEdgesRequest...";
        cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq();

        LOG(INFO) << "Test AddEdgesProcessor...";
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
            auto newEdgeVec = part.second;
            for (auto& newEdge : newEdgeVec) {
                auto edgekey = newEdge.key;
                auto newEdgeProp = newEdge.props;

                auto prefix = NebulaKeyUtils::edgePrefix(spaceVidLen,
                                                         partId,
                                                         edgekey.src,
                                                         edgekey.edge_type,
                                                         edgekey.ranking,
                                                         edgekey.dst);
                std::unique_ptr<kvstore::KVIterator> iter;
                EXPECT_EQ(kvstore::ResultCode::SUCCEEDED,
                          env->kvstore_->prefix(1, partId, prefix, &iter));

                auto schema = env->schemaMan_->getEdgeSchema(1, edgekey.edge_type);
                EXPECT_TRUE(schema != NULL);

                Value val;
                while (iter && iter->valid()) {
                    auto reader = RowReader::getRowReader(schema.get(), iter->val());
                    for (auto i = 0; i < 7; i++) {
                        val = reader->getValueByIndex(i);
                        EXPECT_EQ(newEdgeProp[i], val);
                    }
                    if (newEdgeProp.size() >= 8) {
                        val = reader->getValueByIndex(7);
                        EXPECT_EQ(newEdgeProp[7], val);
                        if (newEdgeProp.size() == 9) {
                            val = reader->getValueByIndex(8);
                            EXPECT_EQ(newEdgeProp[8], val);
                        }
                    }
                    totalCount++;
                    iter->next();
                }
            }
        }
        // The number of data in serve is 160
        EXPECT_EQ(160, totalCount);
    }

    // Delete edges
    {
        auto* processor = DeleteEdgesProcessor::instance(env);

        LOG(INFO) << "Build DeleteEdgesRequest...";
        cpp2::DeleteEdgesRequest req = mock::MockData::mockDeleteEdgesReq();

        LOG(INFO) << "Test DeleteEdgesProcessor...";
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
            auto deleteEdgeKeyVec = part.second;
            for (auto& edgeKey : deleteEdgeKeyVec) {
                auto prefix = NebulaKeyUtils::edgePrefix(spaceVidLen,
                                                         partId,
                                                         edgeKey.src,
                                                         edgeKey.edge_type,
                                                         edgeKey.ranking,
                                                         edgeKey.dst);
                std::unique_ptr<kvstore::KVIterator> iter;
                EXPECT_EQ(kvstore::ResultCode::SUCCEEDED,
                          env->kvstore_->prefix(1, partId, prefix, &iter));

                while (iter && iter->valid()) {
                    totalCount++;
                    iter->next();
                }
            }
        }
        // All the added datas are deleted, the number of edge is 0
        EXPECT_EQ(0, totalCount);
    }
}

TEST(DeleteEdgesTest, MultiVersionTest) {
    fs::TempDir rootPath("/tmp/DeleteEdgesTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();

    // Add edges
    {
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
        auto ret = env->schemaMan_->getSpaceVidLen(1);
        auto spaceVidLen = ret.value();

        int totalCount = 0;
        for (auto& part : req.parts) {
            auto partId = part.first;
            auto newEdgeVec = part.second;
            for (auto& newEdge : newEdgeVec) {
                auto edgekey = newEdge.key;
                auto newEdgeProp = newEdge.props;

                auto prefix = NebulaKeyUtils::edgePrefix(spaceVidLen,
                                                         partId,
                                                         edgekey.src,
                                                         edgekey.edge_type,
                                                         edgekey.ranking,
                                                         edgekey.dst);
                std::unique_ptr<kvstore::KVIterator> iter;
                EXPECT_EQ(kvstore::ResultCode::SUCCEEDED,
                          env->kvstore_->prefix(1, partId, prefix, &iter));

                auto schema = env->schemaMan_->getEdgeSchema(1, edgekey.edge_type);
                EXPECT_TRUE(schema != NULL);

                Value val;
                int num = 0;
                while (iter && iter->valid()) {
                    auto reader = RowReader::getRowReader(schema.get(), iter->val());
                    for (auto i = 0; i < 7; i++) {
                        val = reader->getValueByIndex(i);
                        EXPECT_EQ(newEdgeProp[i], val);
                    }
                    // When adding edge in specified Order, the last two columns
                    // use the default value and null
                    if (num == 0) {
                        val = reader->getValueByIndex(7);
                        EXPECT_EQ(false, val.getBool());
                    } else {
                        if (newEdgeProp.size() >= 8) {
                            val = reader->getValueByIndex(7);
                            EXPECT_EQ(newEdgeProp[7], val);
                            if (newEdgeProp.size() == 9) {
                                val = reader->getValueByIndex(8);
                                EXPECT_EQ(newEdgeProp[8], val);
                            }
                        }
                    }
                    num++;
                    totalCount++;
                    iter->next();
                }
                EXPECT_EQ(2, num);
            }
        }
        // The number of data in serve is 320
        EXPECT_EQ(320, totalCount);
    }

    // Delete edges
    {
        auto* processor = DeleteEdgesProcessor::instance(env);

        LOG(INFO) << "Build DeleteEdgesRequest...";
        cpp2::DeleteEdgesRequest req = mock::MockData::mockDeleteEdgesReq();

        LOG(INFO) << "Test DeleteEdgesProcessor...";
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
            auto deleteEdgeKeyVec = part.second;
            for (auto& edgeKey : deleteEdgeKeyVec) {
                auto prefix = NebulaKeyUtils::edgePrefix(spaceVidLen,
                                                         partId,
                                                         edgeKey.src,
                                                         edgeKey.edge_type,
                                                         edgeKey.ranking,
                                                         edgeKey.dst);
                std::unique_ptr<kvstore::KVIterator> iter;
                EXPECT_EQ(kvstore::ResultCode::SUCCEEDED,
                          env->kvstore_->prefix(1, partId, prefix, &iter));

                while (iter && iter->valid()) {
                    totalCount++;
                    iter->next();
                }
            }
        }
        // All the added datas are deleted, the number of edge is 0
        EXPECT_EQ(0, totalCount);
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

