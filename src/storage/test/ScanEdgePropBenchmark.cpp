/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include <gtest/gtest.h>
#include <folly/stop_watch.h>
#include "mock/AdHocSchemaManager.h"
#include "storage/query/GetNeighborsProcessor.h"
#include "storage/exec/GetNeighborsNode.h"
#include "storage/exec/EdgeNode.h"
#include "storage/test/QueryTestUtils.h"

namespace nebula {
namespace storage {

class ScanEdgePropBench : public ::testing::TestWithParam<std::pair<int, int>> {
};

TEST_P(ScanEdgePropBench, ProcessEdgeProps) {
    auto param = GetParam();
    SchemaVer schemaVerCount = param.first;
    EdgeRanking rankCount = param.second;
    fs::TempDir rootPath("/tmp/ScanEdgePropBench.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path(), {"0", 0}, schemaVerCount);
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockBenchEdgeData(env, totalParts, schemaVerCount, rankCount));

    std::hash<std::string> hash;
    VertexID vId = "Tim Duncan";
    GraphSpaceID spaceId = 1;
    PartitionID partId = (hash(vId) % totalParts) + 1;
    EdgeType edgeType = 101;
    auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId).value();

    std::vector<PropContext> props;
    {
        std::vector<std::string> names = {"playerName", "teamName", "startYear", "endYear"};
        for (const auto& name : names) {
            PropContext ctx(name.c_str());
            ctx.returned_ = true;
            props.emplace_back(std::move(ctx));
        }
    }

    GetNeighborsNode node(vIdLen);
    auto prefix = NebulaKeyUtils::edgePrefix(vIdLen, partId, vId, edgeType);
    {
        // mock the process in 1.0, each time we get the schema from SchemaMan
        // (cache in MetaClient), and then collect values
        nebula::Value result = nebula::List();
        nebula::List list;
        auto* schemaMan = dynamic_cast<mock::AdHocSchemaManager*>(env->schemaMan_);
        std::unique_ptr<kvstore::KVIterator> kvIter;
        std::unique_ptr<StorageIterator> iter;
        auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &kvIter);
        if (ret == kvstore::ResultCode::SUCCEEDED && kvIter && kvIter->valid()) {
            iter.reset(new SingleEdgeIterator(std::move(kvIter), edgeType, vIdLen));
        }
        size_t edgeRowCount = 0;
        folly::stop_watch<std::chrono::microseconds> watch;
        for (; iter->valid(); iter->next(), edgeRowCount++) {
            auto key = iter->key();
            auto val = iter->val();

            SchemaVer schemaVer;
            int32_t readerVer;
            RowReaderWrapper::getVersions(val, schemaVer, readerVer);
            auto schema = schemaMan->getEdgeSchemaFromMap(spaceId, edgeType, schemaVer);
            ASSERT_TRUE(schema != nullptr);
            auto wrapper = std::make_unique<RowReaderWrapper>();
            ASSERT_TRUE(wrapper->reset(schema.get(), val, readerVer));
            auto code = node.collectEdgeProps(edgeType, key, wrapper.get(), &props, list);
            ASSERT_EQ(kvstore::ResultCode::SUCCEEDED, code);
            result.mutableList().values.emplace_back(std::move(list));
        }
        LOG(WARNING) << "ProcessEdgeProps with schema from map: process " << edgeRowCount
                     << " edges takes " << watch.elapsed().count() << " us.";
    }
    {
        // new edge reader each time
        nebula::Value result = nebula::List();
        nebula::List list;
        std::unique_ptr<kvstore::KVIterator> kvIter;
        std::unique_ptr<StorageIterator> iter;
        auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &kvIter);
        if (ret == kvstore::ResultCode::SUCCEEDED && kvIter && kvIter->valid()) {
            iter.reset(new SingleEdgeIterator(std::move(kvIter), edgeType, vIdLen));
        }
        size_t edgeRowCount = 0;
        std::unique_ptr<RowReader> reader;
        folly::stop_watch<std::chrono::microseconds> watch;
        for (; iter->valid(); iter->next(), edgeRowCount++) {
            auto key = iter->key();
            auto val = iter->val();
            reader = RowReader::getEdgePropReader(env->schemaMan_, spaceId,
                                                  std::abs(edgeType), val);
            ASSERT_TRUE(reader.get() != nullptr);
            auto code = node.collectEdgeProps(edgeType, key, reader.get(), &props, list);
            ASSERT_EQ(kvstore::ResultCode::SUCCEEDED, code);
            result.mutableList().values.emplace_back(std::move(list));
        }
        LOG(WARNING) << "ProcessEdgeProps without reader reset: process " << edgeRowCount
                     << " edges takes " << watch.elapsed().count() << " us.";
    }
    {
        // reset edge reader each time instead of new one
        nebula::Value result = nebula::List();
        nebula::List list;
        std::unique_ptr<kvstore::KVIterator> kvIter;
        std::unique_ptr<StorageIterator> iter;
        auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &kvIter);
        if (ret == kvstore::ResultCode::SUCCEEDED && kvIter && kvIter->valid()) {
            iter.reset(new SingleEdgeIterator(std::move(kvIter), edgeType, vIdLen));
        }
        size_t edgeRowCount = 0;
        std::unique_ptr<RowReader> reader;
        folly::stop_watch<std::chrono::microseconds> watch;
        for (; iter->valid(); iter->next(), edgeRowCount++) {
            auto key = iter->key();
            auto val = iter->val();
            if (reader.get() == nullptr) {
                reader = RowReader::getEdgePropReader(env->schemaMan_, spaceId,
                                                      std::abs(edgeType), val);
                ASSERT_TRUE(reader.get() != nullptr);
            } else {
                ASSERT_TRUE(reader->resetEdgePropReader(env->schemaMan_, spaceId,
                                                        std::abs(edgeType), val));
            }
            auto code = node.collectEdgeProps(edgeType, key, reader.get(), &props, list);
            ASSERT_EQ(kvstore::ResultCode::SUCCEEDED, code);
            result.mutableList().values.emplace_back(std::move(list));
        }
        LOG(WARNING) << "ProcessEdgeProps with reader reset: process " << edgeRowCount
                     << " edges takes " << watch.elapsed().count() << " us.";
    }
    {
        // use the schema saved in processor
        nebula::Value result = nebula::List();
        nebula::List list;
        std::unique_ptr<kvstore::KVIterator> kvIter;
        std::unique_ptr<StorageIterator> iter;
        auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &kvIter);
        if (ret == kvstore::ResultCode::SUCCEEDED && kvIter && kvIter->valid()) {
            iter.reset(new SingleEdgeIterator(std::move(kvIter), edgeType, vIdLen));
        }
        size_t edgeRowCount = 0;
        std::unique_ptr<RowReader> reader;

        // find all version of edge schema
        auto edges = env->schemaMan_->getAllVerEdgeSchema(spaceId);
        ASSERT_TRUE(edges.ok());
        auto edgeSchemas = std::move(edges).value();
        auto edgeIter = edgeSchemas.find(std::abs(edgeType));
        ASSERT_TRUE(edgeIter != edgeSchemas.end());
        const auto& schemas = edgeIter->second;

        folly::stop_watch<std::chrono::microseconds> watch;
        for (; iter->valid(); iter->next(), edgeRowCount++) {
            auto key = iter->key();
            auto val = iter->val();
            if (reader.get() == nullptr) {
                reader = RowReader::getRowReader(schemas, val);
                ASSERT_TRUE(reader.get() != nullptr);
            } else {
                ASSERT_TRUE(reader->reset(schemas, val));
            }
            auto code = node.collectEdgeProps(edgeType, key, reader.get(), &props, list);
            ASSERT_EQ(kvstore::ResultCode::SUCCEEDED, code);
            result.mutableList().values.emplace_back(std::move(list));
        }
        LOG(WARNING) << "ProcessEdgeProps using local schmeas: process " << edgeRowCount
                     << " edges takes " << watch.elapsed().count() << " us.";
    }
}

TEST_P(ScanEdgePropBench, EdgeTypePrefixScanVsVertexPrefixScan) {
    auto param = GetParam();
    SchemaVer schemaVerCount = param.first;
    EdgeRanking rankCount = param.second;
    fs::TempDir rootPath("/tmp/ScanEdgePropBench.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path(), {"", 0}, schemaVerCount);
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockBenchEdgeData(env, totalParts, schemaVerCount, rankCount));

    std::hash<std::string> hash;
    VertexID vId = "Tim Duncan";
    GraphSpaceID spaceId = 1;
    PartitionID partId = (hash(vId) % totalParts) + 1;
    EdgeType serve = 101, teammate = 102;
    auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId).value();

    std::vector<EdgeType> edgeTypes = {serve, teammate};
    std::vector<std::vector<PropContext>> ctxs;
    std::vector<PropContext> serveProps;
    {
        std::vector<std::string> names = {"playerName", "teamName", "startYear", "endYear"};
        for (const auto& name : names) {
            PropContext ctx(name.c_str());
            ctx.returned_ = true;
            serveProps.emplace_back(std::move(ctx));
        }
        ctxs.emplace_back(serveProps);
    }
    std::vector<PropContext> teammateProps;
    {
        std::vector<std::string> names = {"teamName", "startYear", "endYear"};
        for (const auto& name : names) {
            PropContext ctx(name.c_str());
            ctx.returned_ = true;
            teammateProps.emplace_back(std::move(ctx));
        }
        ctxs.emplace_back(teammateProps);
    }

    // find all version of edge schema
    auto edgeSchemas = env->schemaMan_->getAllVerEdgeSchema(spaceId);
    ASSERT_TRUE(edgeSchemas.ok());
    EdgeContext edgeContext;
    edgeContext.schemas_ = std::move(edgeSchemas).value();
    edgeContext.propContexts_.emplace_back(serve, serveProps);
    edgeContext.propContexts_.emplace_back(teammate, teammateProps);
    edgeContext.indexMap_.emplace(serve, 0);
    edgeContext.indexMap_.emplace(teammate, 1);
    edgeContext.offset_ = 2;

    {
        // build dag with several EdgeTypePrefixScanNode
        GetNeighborsProcessor processor(env, nullptr, nullptr);
        processor.edgeContext_ = edgeContext;
        processor.spaceId_ = spaceId;
        processor.spaceVidLen_ = vIdLen;
        nebula::DataSet result;
        auto dag = processor.buildDAG(&result);

        folly::stop_watch<std::chrono::microseconds> watch;
        auto code = dag.go(partId, vId);
        ASSERT_EQ(kvstore::ResultCode::SUCCEEDED, code);
        LOG(WARNING) << "GetNeighbors with EdgeTypePrefixScanNode takes "
                     << watch.elapsed().count() << " us.";
    }
    {
        // build dag with one VertexPrefixScanNode
        nebula::DataSet result;

        StorageDAG<VertexID> dag;
        std::vector<TagNode*> tags;
        std::vector<EdgeNode<VertexID>*> edges;
        auto edgeNode = std::make_unique<VertexPrefixScanNode>(&edgeContext, env, spaceId, vIdLen);
        edges.emplace_back(edgeNode.get());
        dag.addNode(std::move(edgeNode));
        auto filter = std::make_unique<FilterNode>(nullptr, tags, edges, nullptr, &edgeContext);
        for (auto* edge : edges) {
            filter->addDependency(edge);
        }
        auto cat = std::make_unique<GetNeighborsNode>(
                tags, filter.get(), nullptr, &edgeContext, vIdLen, &result);
        cat->addDependency(filter.get());
        dag.addNode(std::move(filter));
        dag.addNode(std::move(cat));

        folly::stop_watch<std::chrono::microseconds> watch;
        auto code = dag.go(partId, vId);
        ASSERT_EQ(kvstore::ResultCode::SUCCEEDED, code);
        LOG(WARNING) << "GetNeighbors with VertexPrefixScanNode takes "
                     << watch.elapsed().count() << " us.";
    }
}

// the parameter pair<int, int> is count of edge schema version,
// and how many edges of diffent rank of a mock edge
INSTANTIATE_TEST_CASE_P(
    ScanEdgePropBench,
    ScanEdgePropBench,
    ::testing::Values(
        std::make_pair(1, 10000),
        std::make_pair(10, 10000),
        std::make_pair(100, 10000)));


}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::WARNING);
    return RUN_ALL_TESTS();
}
