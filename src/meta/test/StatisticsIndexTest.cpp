/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include <gtest/gtest.h>
#include "meta/test/TestUtils.h"
#include "meta/processors/indexMan/CreateTagIndexProcessor.h"
#include "meta/processors/indexMan/DropTagIndexProcessor.h"
#include "meta/processors/indexMan/GetTagIndexProcessor.h"
#include "meta/processors/indexMan/ListTagIndexesProcessor.h"
#include "meta/processors/indexMan/CreateEdgeIndexProcessor.h"
#include "meta/processors/indexMan/DropEdgeIndexProcessor.h"
#include "meta/processors/indexMan/GetEdgeIndexProcessor.h"
#include "meta/processors/indexMan/ListEdgeIndexesProcessor.h"

namespace nebula {
namespace meta {

using cpp2::PropertyType;

// vertex_count or vertex index
TEST(ProcessorTest, StatisticsTagIndexTest) {
    fs::TempDir rootPath("/tmp/StatisticsTagIndexTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
    TestUtils::createSomeHosts(kv.get());
    ASSERT_TRUE(TestUtils::assembleSpace(kv.get(), 1, 1));
    TestUtils::mockTag(kv.get(), 2);
    // VERTEX_COUNT index test
    {
        // Failed, VERTEX_COUNT index use to get all vertex count in one space
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_index_name("all_vertex_count_index");
        req.set_index_type(cpp2::IndexType::VERTEX_COUNT);
        req.set_tag_name("tag_0");
        auto* processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        // Succeed
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_index_name("all_vertex_count_index");
        req.set_index_type(cpp2::IndexType::VERTEX_COUNT);
        req.set_tag_name("*");
        auto* processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        // Failed, create a duplicate VERTEX_COUNT index
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_index_name("duplicate_all_vertex_count_index");
        req.set_index_type(cpp2::IndexType::VERTEX_COUNT);
        req.set_tag_name("*");
        auto* processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        // Exist VERTEX_COUNT index
        cpp2::ListTagIndexesReq req;
        req.set_space_id(1);
        auto* processor = ListTagIndexesProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());

        auto items = resp.get_items();
        ASSERT_EQ(1, items.size());
    }
    {
        // Get VERTEX_COUNT index
        cpp2::GetTagIndexReq req;
        req.set_space_id(1);
        req.set_index_name("all_vertex_count_index");
        auto* processor = GetTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());

        auto item = resp.get_item();
        ASSERT_EQ("all_vertex_count_index", item.get_index_name());
        ASSERT_EQ(cpp2::IndexType::VERTEX_COUNT, item.get_index_type());
        ASSERT_EQ("*", item.get_schema_name());
        ASSERT_EQ(0, item.get_fields().size());
    }
    // Vertex index test
    {
        // Failed, count(col) currently not supported
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_index_name("vertex_in_tag_index");
        req.set_index_type(cpp2::IndexType::VERTEX);
        req.set_tag_name("tag_0");
        std::vector<std::string> fields{"tag_0_col_0"};
        req.set_fields(std::move(fields));
        auto* processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        // Failed
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_index_name("vertex_in_tag_index");
        req.set_index_type(cpp2::IndexType::VERTEX);
        req.set_tag_name("*");
        auto* processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        // Succeed
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_index_name("vertex_in_tag_index");
        req.set_index_type(cpp2::IndexType::VERTEX);
        req.set_tag_name("tag_0");
        auto* processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        // Failed, create a duplicate VERTEX index
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_index_name("duplicate_vertex_in_tag_index");
        req.set_index_type(cpp2::IndexType::VERTEX);
        req.set_tag_name("tag_0");
        auto* processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        // Exist VERTEX index
        cpp2::ListTagIndexesReq req;
        req.set_space_id(1);
        auto* processor = ListTagIndexesProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());

        auto items = resp.get_items();
        ASSERT_EQ(2, items.size());
    }
    {   // Get VERTEX index
        cpp2::GetTagIndexReq req;
        req.set_space_id(1);
        req.set_index_name("vertex_in_tag_index");
        auto* processor = GetTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());

        auto item = resp.get_item();
        ASSERT_EQ("vertex_in_tag_index", item.get_index_name());
        ASSERT_EQ(cpp2::IndexType::VERTEX, item.get_index_type());
        ASSERT_EQ("tag_0", item.get_schema_name());
        ASSERT_EQ(0, item.get_fields().size());
    }
    {   // Drop VERTEX_COUNT index
        cpp2::DropTagIndexReq req;
        req.set_space_id(1);
        req.set_index_name("all_vertex_count_index");
        auto* processor = DropTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {   // Drop VERTEX index
        cpp2::DropTagIndexReq req;
        req.set_space_id(1);
        req.set_index_name("vertex_in_tag_index");
        auto* processor = DropTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        // Not exist VERTEX_COUNT or VERTEX index
        cpp2::ListTagIndexesReq req;
        req.set_space_id(1);
        auto* processor = ListTagIndexesProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        auto items = resp.get_items();
        ASSERT_EQ(0, items.size());
    }
}

// edge_count or edge index
TEST(ProcessorTest, StatisticsEdgeIndexTest) {
    fs::TempDir rootPath("/tmp/StatisticsEdgeIndexTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
    TestUtils::createSomeHosts(kv.get());
    ASSERT_TRUE(TestUtils::assembleSpace(kv.get(), 1, 1));
    TestUtils::mockEdge(kv.get(), 2);
    // EDGE_COUNT test
    {
        // Failed, EDGE_COUNT use to get all edge count in one space
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_index_name("all_edge_count_index");
        req.set_index_type(cpp2::IndexType::EDGE_COUNT);
        req.set_edge_name("edge_0");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        // Succeed
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_index_name("all_edge_count_index");
        req.set_index_type(cpp2::IndexType::EDGE_COUNT);
        req.set_edge_name("*");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        // Failed, create a duplicate EDGE_COUNT index
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_index_name("duplicate_all_edge_count_index");
        req.set_index_type(cpp2::IndexType::EDGE_COUNT);
        req.set_edge_name("*");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        // Exist EDGE_COUNT index
        cpp2::ListEdgeIndexesReq req;
        req.set_space_id(1);
        auto* processor = ListEdgeIndexesProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        auto items = resp.get_items();
        ASSERT_EQ(1, items.size());
    }
    {   // Get EDGE_COUNT index
        cpp2::GetEdgeIndexReq req;
        req.set_space_id(1);
        req.set_index_name("all_edge_count_index");
        auto* processor = GetEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());

        auto item = resp.get_item();
        ASSERT_EQ("all_edge_count_index", item.get_index_name());
        ASSERT_EQ(cpp2::IndexType::EDGE_COUNT, item.get_index_type());
        ASSERT_EQ("*", item.get_schema_name());
        ASSERT_EQ(0, item.get_fields().size());
    }
    // EDGE index test
    {
        // count(col) currently not supported
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_index_name("edge_in_edgetype_index");
        req.set_index_type(cpp2::IndexType::EDGE);
        req.set_edge_name("edge_0");
        std::vector<std::string> fields{"edge_0_col_0"};
        req.set_fields(std::move(fields));
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        // Failed
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_index_name("edge_in_edgetype_index");
        req.set_index_type(cpp2::IndexType::EDGE);
        req.set_edge_name("*");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        // Succeed
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_index_name("edge_in_edgetype_index");
        req.set_index_type(cpp2::IndexType::EDGE);
        req.set_edge_name("edge_0");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        // Failed, create a duplicate EDGE index
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_index_name("duplicate_edge_in_edgetype_index");
        req.set_index_type(cpp2::IndexType::EDGE);
        req.set_edge_name("edge_0");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        // Exist EDGE index
        cpp2::ListEdgeIndexesReq req;
        req.set_space_id(1);
        auto* processor = ListEdgeIndexesProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        auto items = resp.get_items();
        ASSERT_EQ(2, items.size());
    }
    {
        // Get EDGE index
        cpp2::GetEdgeIndexReq req;
        req.set_space_id(1);
        req.set_index_name("edge_in_edgetype_index");
        auto* processor = GetEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());

        auto item = resp.get_item();
        ASSERT_EQ("edge_in_edgetype_index", item.get_index_name());
        ASSERT_EQ(cpp2::IndexType::EDGE, item.get_index_type());
        ASSERT_EQ("edge_0", item.get_schema_name());
        ASSERT_EQ(0, item.get_fields().size());
    }
    {
        // Drop EDGE_COUNT index
        cpp2::DropEdgeIndexReq req;
        req.set_space_id(1);
        req.set_index_name("all_edge_count_index");

        auto* processor = DropEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        // Drop EDGE index
        cpp2::DropEdgeIndexReq req;
        req.set_space_id(1);
        req.set_index_name("edge_in_edgetype_index");

        auto* processor = DropEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        // Not exist EDGE_COUNT or EDGE index
        cpp2::ListEdgeIndexesReq req;
        req.set_space_id(1);
        auto* processor = ListEdgeIndexesProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        auto items = resp.get_items();
        ASSERT_EQ(0, items.size());
    }
}

}  // namespace meta
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}



