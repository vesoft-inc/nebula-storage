/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "base/Base.h"
#include <gtest/gtest.h>
#include "fs/TempDir.h"
#include "time/WallClock.h"
#include "mock/AdHocSchemaManager.h"
#include "storage/query/GetNeighborsProcessor.h"
#include "storage/test/QueryTestUtils.h"

namespace nebula {
namespace storage {

// the parameter pair<int, int> is count of edge schema version,
// and how many edges of diffent rank of a mock edge
class GetNeighborsBench : public ::testing::TestWithParam<std::pair<int, int>> {
};

TEST_P(GetNeighborsBench, ProcessEdgeProps) {
    auto param = GetParam();
    SchemaVer schemaVerCount = param.first;
    EdgeRanking rankCount = param.second;
    fs::TempDir rootPath("/tmp/GetNeighborsBench.XXXXXX");
    mock::MockCluster cluster;
    cluster.startStorage({0, 0}, rootPath.path(), schemaVerCount);
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockBenchEdgeData(env, totalParts, schemaVerCount, rankCount));

    std::hash<std::string> hash;
    VertexID vId = "Tim Duncan";
    GraphSpaceID spaceId = 1;
    PartitionID partId = (hash(vId) % totalParts) + 1;
    EdgeType edgeType = 101;

    std::vector<PropContext> props;
    {
        std::vector<std::string> names = {"playerName", "teamName", "startYear", "endYear"};
        for (const auto& name : names) {
            PropContext ctx(name.c_str());
            ctx.returned_ = true;
            props.emplace_back(std::move(ctx));
        }
    }

    {
        // mock the process in 1.0, each time we get the schema from SchemaMan
        // (cache in MetaClient), and then collect values
        auto* schemaMan = dynamic_cast<mock::AdHocSchemaManager*>(env->schemaMan_);
        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr, nullptr);
        processor->spaceId_ = spaceId;
        processor->vIdLen_ = env->schemaMan_->getSpaceVidLen(spaceId).value();
        int64_t edgeRowCount = 0;
        nebula::DataSet dataSet;
        auto tick = time::WallClock::fastNowInMicroSec();
        auto retCode = processor->processEdgeProps(partId, vId, edgeType, edgeRowCount,
            [schemaMan, processor, spaceId, edgeType, &props, &dataSet]
            (std::unique_ptr<RowReader>* reader, folly::StringPiece key, folly::StringPiece val)
            -> kvstore::ResultCode {
                UNUSED(reader);
                SchemaVer schemaVer;
                int32_t readerVer;
                RowReaderWrapper::getVersions(val, schemaVer, readerVer);
                auto schema = schemaMan->getEdgeSchemaFromMap(spaceId, edgeType, schemaVer);
                if (schema == nullptr) {
                    return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
                }
                auto wrapper = std::make_unique<RowReaderWrapper>();
                if (!wrapper->reset(schema.get(), val, readerVer)) {
                    return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
                }

                return processor->collectEdgeProps(edgeType, wrapper.get(), key, props,
                                                   dataSet, nullptr);
            });
        EXPECT_EQ(retCode, kvstore::ResultCode::SUCCEEDED);
        auto tock = time::WallClock::fastNowInMicroSec();
        LOG(WARNING) << "ProcessEdgeProps with reader reset: process " << edgeRowCount
                     << " edges takes " << tock - tick << " us.";
    }
    {
        // reset edge reader each time instead of new one
        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr, nullptr);
        processor->spaceId_ = spaceId;
        processor->vIdLen_ = env->schemaMan_->getSpaceVidLen(spaceId).value();
        int64_t edgeRowCount = 0;
        nebula::DataSet dataSet;
        auto tick = time::WallClock::fastNowInMicroSec();
        auto retCode = processor->processEdgeProps(partId, vId, edgeType, edgeRowCount,
            [processor, spaceId, edgeType, env, &props, &dataSet]
            (std::unique_ptr<RowReader>* reader, folly::StringPiece key, folly::StringPiece val)
            -> kvstore::ResultCode {
                if (reader->get() == nullptr) {
                    *reader = RowReader::getEdgePropReader(env->schemaMan_, spaceId,
                                                           std::abs(edgeType), val);
                    if (!reader) {
                        return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
                    }
                } else if (!(*reader)->resetEdgePropReader(env->schemaMan_, spaceId,
                                                           std::abs(edgeType), val)) {
                    return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
                }

                return processor->collectEdgeProps(edgeType, reader->get(), key, props,
                                                   dataSet, nullptr);
            });
        EXPECT_EQ(retCode, kvstore::ResultCode::SUCCEEDED);
        auto tock = time::WallClock::fastNowInMicroSec();
        LOG(WARNING) << "ProcessEdgeProps with reader reset: process " << edgeRowCount
                     << " edges takes " << tock - tick << " us.";
    }
    {
        // new edge reader each time
        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr, nullptr);
        processor->spaceId_ = spaceId;
        processor->vIdLen_ = env->schemaMan_->getSpaceVidLen(spaceId).value();
        int64_t edgeRowCount = 0;
        nebula::DataSet dataSet;
        auto tick = time::WallClock::fastNowInMicroSec();
        auto retCode = processor->processEdgeProps(partId, vId, edgeType, edgeRowCount,
            [processor, spaceId, edgeType, env, &props, &dataSet]
            (std::unique_ptr<RowReader>* reader, folly::StringPiece key, folly::StringPiece val)
            -> kvstore::ResultCode {
                *reader = RowReader::getEdgePropReader(env->schemaMan_, spaceId,
                                                       std::abs(edgeType), val);
                if (!reader) {
                    return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
                }

                return processor->collectEdgeProps(edgeType, reader->get(), key, props,
                                                   dataSet, nullptr);
            });
        EXPECT_EQ(retCode, kvstore::ResultCode::SUCCEEDED);
        auto tock = time::WallClock::fastNowInMicroSec();
        LOG(WARNING) << "ProcessEdgeProps without reader reset: process " << edgeRowCount
                     << " edges takes " << tock - tick << " us.";
    }
    {
        // use the schema saved in processor
        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr, nullptr);
        processor->spaceId_ = spaceId;
        processor->vIdLen_ = env->schemaMan_->getSpaceVidLen(spaceId).value();
        int64_t edgeRowCount = 0;
        nebula::DataSet dataSet;

        // find all version of edge schema
        auto edges = env->schemaMan_->getAllVerEdgeSchema(spaceId);
        ASSERT_TRUE(edges.ok());
        auto edgeSchemas = std::move(edges).value();
        auto edgeIter = edgeSchemas.find(std::abs(edgeType));
        ASSERT_TRUE(edgeIter != edgeSchemas.end());
        const auto& schemas = edgeIter->second;

        auto tick = time::WallClock::fastNowInMicroSec();
        auto retCode = processor->processEdgeProps(partId, vId, edgeType, edgeRowCount,
            [processor, spaceId, edgeType, &props, &dataSet, &schemas]
            (std::unique_ptr<RowReader>* reader, folly::StringPiece key, folly::StringPiece val)
            -> kvstore::ResultCode {
                if (reader->get() == nullptr) {
                    *reader = RowReader::getRowReader(schemas, val);
                    if (!reader) {
                        return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
                    }
                } else if (!(*reader)->reset(schemas, val)) {
                    return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
                }

                return processor->collectEdgeProps(edgeType, reader->get(), key, props,
                                                   dataSet, nullptr);
            });
        EXPECT_EQ(retCode, kvstore::ResultCode::SUCCEEDED);
        auto tock = time::WallClock::fastNowInMicroSec();
        LOG(WARNING) << "ProcessEdgeProps using local schmeas: process " << edgeRowCount
                     << " edges takes " << tock - tick << " us.";
    }
}

TEST_P(GetNeighborsBench, ScanEdgesVsProcessEdgeProps) {
    auto param = GetParam();
    SchemaVer schemaVerCount = param.first;
    EdgeRanking rankCount = param.second;
    fs::TempDir rootPath("/tmp/GetNeighborsBench.XXXXXX");
    mock::MockCluster cluster;
    cluster.startStorage({0, 0}, rootPath.path(), schemaVerCount);
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockBenchEdgeData(env, totalParts, schemaVerCount, rankCount));

    std::hash<std::string> hash;
    VertexID vId = "Tim Duncan";
    GraphSpaceID spaceId = 1;
    PartitionID partId = (hash(vId) % totalParts) + 1;
    EdgeType serve = 101, teammate = 102;

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

    {
        // scan with vertex prefix
        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr, nullptr);
        processor->spaceId_ = spaceId;
        processor->vIdLen_ = env->schemaMan_->getSpaceVidLen(spaceId).value();
        FilterContext fcontext;
        nebula::Row row;
        row.columns.emplace_back(vId);
        row.columns.emplace_back(NullType::__NULL__);

        // find all version of edge schema
        auto edges = env->schemaMan_->getAllVerEdgeSchema(spaceId);
        ASSERT_TRUE(edges.ok());
        processor->edgeSchemas_ = std::move(edges).value();
        processor->edgeContexts_.emplace_back(serve, serveProps);
        processor->edgeContexts_.emplace_back(teammate, teammateProps);
        processor->edgeIndexMap_.emplace(serve, 0);
        processor->edgeIndexMap_.emplace(teammate, 1);

        {
            auto tick = time::WallClock::fastNowInMicroSec();
            auto retCode = processor->scanEdges(partId, vId, fcontext, row);
            EXPECT_EQ(retCode, kvstore::ResultCode::SUCCEEDED);
            auto tock = time::WallClock::fastNowInMicroSec();
            LOG(WARNING) << "ScanEdges using local schmeas: process"
                         << " edges takes " << tock - tick << " us.";
        }
    }
    {
        // use the schema saved in processor
        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr, nullptr);
        processor->spaceId_ = spaceId;
        processor->vIdLen_ = env->schemaMan_->getSpaceVidLen(spaceId).value();
        int64_t edgeRowCount = 0;
        nebula::DataSet dataSet;

        // find all version of edge schema
        auto edges = env->schemaMan_->getAllVerEdgeSchema(spaceId);
        ASSERT_TRUE(edges.ok());
        auto edgeSchemas = std::move(edges).value();

        auto tick = time::WallClock::fastNowInMicroSec();
        for (size_t i = 0; i < edgeTypes.size(); i++) {
            EdgeType edgeType = edgeTypes[i];
            auto edgeIter = edgeSchemas.find(std::abs(edgeType));
            ASSERT_TRUE(edgeIter != edgeSchemas.end());
            const auto& schemas = edgeIter->second;
            const auto& props = ctxs[i];

            auto ttl = processor->getEdgeTTLInfo(edgeType);
            auto retCode = processor->processEdgeProps(partId, vId, edgeType, edgeRowCount,
                [processor, spaceId, edgeType, &props, &dataSet, &schemas, &ttl]
                (std::unique_ptr<RowReader>* reader, folly::StringPiece key, folly::StringPiece val)
                -> kvstore::ResultCode {
                    if (reader->get() == nullptr) {
                        *reader = RowReader::getRowReader(schemas, val);
                        if (!reader) {
                            return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
                        }
                    } else if (!(*reader)->reset(schemas, val)) {
                        return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
                    }

                    const auto& latestSchema = schemas.back();
                    if (ttl.has_value() &&
                        checkDataExpiredForTTL(latestSchema.get(), reader->get(),
                                            ttl.value().first, ttl.value().second)) {
                        return kvstore::ResultCode::ERR_RESULT_EXPIRED;
                    }

                    return processor->collectEdgeProps(edgeType, reader->get(), key, props,
                                                       dataSet, nullptr);
                });
            EXPECT_EQ(retCode, kvstore::ResultCode::SUCCEEDED);
        }
        auto tock = time::WallClock::fastNowInMicroSec();
        LOG(WARNING) << "ProcessEdgeProps using local schmeas: process " << edgeRowCount
                     << " edges takes " << tock - tick << " us.";
    }
}

INSTANTIATE_TEST_CASE_P(
    GetNeighborsBench,
    GetNeighborsBench,
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
