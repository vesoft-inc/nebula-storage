/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_TEST_QUERYTESTUTILS_H_
#define STORAGE_TEST_QUERYTESTUTILS_H_

#include "common/base/Base.h"
#include "common/expression/Expression.h"
#include "common/expression/PropertyExpression.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"
#include "codec/RowWriterV2.h"
#include "codec/RowReaderWrapper.h"
#include "utils/NebulaKeyUtils.h"

DECLARE_bool(mock_ttl_col);
DECLARE_int32(mock_ttl_duration);

namespace nebula {
namespace storage {

class QueryTestUtils {
public:
    static bool mockVertexData(storage::StorageEnv* env,
                               int32_t totalParts,
                               bool enableIndex = false) {
        GraphSpaceID spaceId = 1;
        auto status = env->schemaMan_->getSpaceVidLen(spaceId);
        if (!status.ok()) {
            LOG(ERROR) << "Get space vid length failed";
            return false;
        }
        std::hash<std::string> hash;
        auto spaceVidLen = status.value();
        auto vertices = mock::MockData::mockVertices();
        std::vector<kvstore::KV> data;
        VertexID lastVId = "";
        folly::Baton<true, std::atomic> baton;
        std::atomic<size_t> count(vertices.size());
        for (const auto& vertex : vertices) {
            PartitionID partId = (hash(vertex.vId_) % totalParts) + 1;
            TagID tagId = vertex.tId_;
            auto key = NebulaKeyUtils::vertexKey(spaceVidLen, partId, vertex.vId_, tagId, 0L);
            auto schema = env->schemaMan_->getTagSchema(spaceId, tagId);
            if (!schema) {
                LOG(ERROR) << "Invalid tagId " << tagId;
                return false;
            }
            EXPECT_TRUE(encode(schema.get(), key, vertex.props_, data));
            if (enableIndex) {
                if (tagId == 1 || tagId == 2) {
                    std::vector<Value::Type> colsType({});
                    encodeTagIndex(spaceVidLen,
                                   partId,
                                   vertex.vId_,
                                   tagId,
                                   vertex.props_,
                                   tagId == 1 ? 3 : 1,
                                   colsType, data);
                }
            }
            env->kvstore_->asyncMultiPut(spaceId, partId, std::move(data),
                                        [&](kvstore::ResultCode code) {
                                            EXPECT_EQ(code, kvstore::ResultCode::SUCCEEDED);
                                            count.fetch_sub(1);
                                            if (count.load() == 0) {
                                                baton.post();
                                            }
                                        });
        }
        baton.wait();
        return true;
    }

    static bool mockEdgeData(storage::StorageEnv* env,
                             int32_t totalParts,
                             EdgeVersion maxVersions = 1,
                             bool enableIndex = false) {
        GraphSpaceID spaceId = 1;
        auto status = env->schemaMan_->getSpaceVidLen(spaceId);
        if (!status.ok()) {
            LOG(ERROR) << "Get space vid length failed";
            return false;
        }
        std::hash<std::string> hash;
        auto spaceVidLen = status.value();
        auto edges = mock::MockData::mockMultiEdges();
        std::vector<kvstore::KV> data;
        for (EdgeVersion version = 0; version < maxVersions; version++) {
            std::atomic<size_t> count(edges.size());
            folly::Baton<true, std::atomic> baton;
            for (const auto& edge : edges) {
                PartitionID partId = (hash(edge.srcId_) % totalParts) + 1;
                auto key = NebulaKeyUtils::edgeKey(spaceVidLen, partId, edge.srcId_, edge.type_,
                                                   edge.rank_, edge.dstId_, version);
                auto schema = env->schemaMan_->getEdgeSchema(spaceId, std::abs(edge.type_));
                if (!schema) {
                    LOG(ERROR) << "Invalid edge " << edge.type_;
                    return false;
                }
                EXPECT_TRUE(encode(schema.get(), key, edge.props_, data));
                if (enableIndex) {
                    if (edge.type_ == 102 || edge.type_ == 101) {
                        std::vector<Value::Type> colsType({});
                        encodeEdgeIndex(spaceVidLen,
                                        partId,
                                        edge.srcId_,
                                        edge.dstId_,
                                        edge.rank_,
                                        edge.type_,
                                        edge.props_,
                                        3,
                                        colsType,
                                        data);
                    }
                }
                env->kvstore_->asyncMultiPut(spaceId, partId, std::move(data),
                                            [&](kvstore::ResultCode code) {
                                                EXPECT_EQ(code, kvstore::ResultCode::SUCCEEDED);
                                                count.fetch_sub(1);
                                                if (count.load() == 0) {
                                                    baton.post();
                                                }
                                            });
            }
            baton.wait();
        }
        return true;
    }

    static bool mockBenchEdgeData(storage::StorageEnv* env,
                                 int32_t totalParts,
                                 SchemaVer schemaVerCount,
                                 EdgeRanking rankCount) {
        GraphSpaceID spaceId = 1;
        auto status = env->schemaMan_->getSpaceVidLen(spaceId);
        if (!status.ok()) {
            LOG(ERROR) << "Get space vid length failed";
            return false;
        }
        std::hash<std::string> hash;
        auto spaceVidLen = status.value();
        auto edges = mock::MockData::mockmMultiRankServes(rankCount);
        std::atomic<size_t> count(edges.size());
        folly::Baton<true, std::atomic> baton;
        for (const auto& entry : edges) {
            PartitionID partId = (hash(entry.first) % totalParts) + 1;
            std::vector<kvstore::KV> data;
            for (const auto& edge : entry.second) {
                auto key = NebulaKeyUtils::edgeKey(spaceVidLen, partId, edge.srcId_, edge.type_,
                                                   edge.rank_, edge.dstId_, 0L);
                SchemaVer ver = folly::Random::rand64() % schemaVerCount;
                auto schema = env->schemaMan_->getEdgeSchema(spaceId, std::abs(edge.type_), ver);
                if (!schema) {
                    LOG(ERROR) << "Invalid edge " << edge.type_;
                    return false;
                }
                EXPECT_TRUE(encode(schema.get(), key, edge.props_, data));
            }
            env->kvstore_->asyncMultiPut(spaceId, partId, std::move(data),
                                        [&](kvstore::ResultCode code) {
                                            EXPECT_EQ(code, kvstore::ResultCode::SUCCEEDED);
                                            count.fetch_sub(1);
                                            if (count.load() == 0) {
                                                baton.post();
                                            }
                                        });
        }
        baton.wait();
        return true;
    }

    static bool encode(const meta::NebulaSchemaProvider* schema,
                       const std::string& key,
                       const std::vector<Value>& props,
                       std::vector<kvstore::KV>& data) {
        RowWriterV2 writer(schema);
        for (size_t i = 0; i < props.size(); i++) {
            auto r = writer.setValue(i, props[i]);
            if (r != WriteResult::SUCCEEDED) {
                LOG(ERROR) << "Invalid prop " << i;
                return false;
            }
        }
        auto ret = writer.finish();
        if (ret != WriteResult::SUCCEEDED) {
            LOG(ERROR) << "Failed to write data";
            return false;
        }
        auto encode = std::move(writer).moveEncodedStr();
        data.emplace_back(std::move(key), std::move(encode));
        return true;
    }

    static void encodeTagIndex(size_t spaceVidLen,
                               PartitionID partId,
                               VertexID vId,
                               IndexID indexId,
                               const std::vector<Value>& values,
                               int32_t count,
                               const std::vector<Value::Type>& colsType,
                               std::vector<kvstore::KV>& data) {
        std::vector<Value> row;
        for (auto i = 0; i < count; i++) {
            auto v = values[i];
            row.emplace_back(std::move(v));
        }
        auto index = IndexKeyUtils::vertexIndexKey(spaceVidLen,
                                                   partId,
                                                   indexId,
                                                   vId,
                                                   row,
                                                   colsType);
        data.emplace_back(std::move(index), "");
    }

    static void encodeEdgeIndex(size_t spaceVidLen,
                               PartitionID partId,
                               VertexID srcId,
                               VertexID dstId,
                               EdgeRanking rank,
                               IndexID indexId,
                               const std::vector<Value>& values,
                               int32_t count,
                               const std::vector<Value::Type>& colsType,
                               std::vector<kvstore::KV>& data) {
        std::vector<Value> row;
        for (auto i = 0; i < count; i++) {
            auto v = values[i];
            row.emplace_back(std::move(v));
        }
        auto index = IndexKeyUtils::edgeIndexKey(spaceVidLen,
                                                 partId,
                                                 indexId,
                                                 srcId,
                                                 rank,
                                                 dstId,
                                                 row,
                                                 colsType);
        data.emplace_back(std::move(index), "");
    }

    static cpp2::GetNeighborsRequest buildRequest(
            int32_t totalParts,
            const std::vector<VertexID> vertices,
            const std::vector<EdgeType>& over,
            const std::vector<std::pair<TagID, std::vector<std::string>>>& tags,
            const std::vector<std::pair<EdgeType, std::vector<std::string>>>& edges,
            bool returnNoneProps = false) {
        std::hash<std::string> hash;
        cpp2::GetNeighborsRequest req;
        decltype(req.traverse_spec) traverseSpec;
        req.space_id = 1;
        req.column_names.emplace_back(kVid);
        for (const auto& vertex : vertices) {
            PartitionID partId = (hash(vertex) % totalParts) + 1;
            nebula::Row row;
            row.values.emplace_back(vertex);
            req.parts[partId].emplace_back(std::move(row));
        }
        for (const auto& edge : over) {
            traverseSpec.edge_types.emplace_back(edge);
        }

        std::vector<cpp2::VertexProp> vertexProps;
        if (tags.empty() && !returnNoneProps) {
            traverseSpec.set_vertex_props(std::move(vertexProps));
        } else if (!returnNoneProps) {
            for (const auto& tag : tags) {
                TagID tagId = tag.first;
                cpp2::VertexProp tagProp;
                tagProp.tag = tagId;
                for (const auto& prop : tag.second) {
                    tagProp.props.emplace_back(std::move(prop));
                }
                vertexProps.emplace_back(std::move(tagProp));
            }
            traverseSpec.set_vertex_props(std::move(vertexProps));
        }

        std::vector<cpp2::EdgeProp> edgeProps;
        if (edges.empty() && !returnNoneProps) {
            traverseSpec.set_edge_props(std::move(edgeProps));
        } else if (!returnNoneProps) {
            for (const auto& edge : edges) {
                EdgeType edgeType = edge.first;
                cpp2::EdgeProp edgeProp;
                edgeProp.type = edgeType;
                for (const auto& prop : edge.second) {
                    edgeProp.props.emplace_back(std::move(prop));
                }
                edgeProps.emplace_back(std::move(edgeProp));
            }
            traverseSpec.set_edge_props(std::move(edgeProps));
        }
        req.set_traverse_spec(std::move(traverseSpec));
        return req;
    }

    // | vId | stat |     tag       |   ...   |      edge      | ...
    //              | prop ... prop |   ...   | prop .... prop |
    // check response when tags or edges is specified
    static void checkResponse(
            const nebula::DataSet& dataSet,
            const std::vector<VertexID>& vertices,
            const std::vector<EdgeType>& over,
            const std::vector<std::pair<TagID, std::vector<std::string>>>& tags,
            const std::vector<std::pair<EdgeType, std::vector<std::string>>>& edges,
            size_t expectRowCount,
            size_t expectColumnCount,
            std::unordered_map<VertexID, std::vector<Value>>* expectStats = nullptr) {
        UNUSED(over);
        if (!edges.empty()) {
            checkColNames(dataSet, tags, edges);
        }
        ASSERT_EQ(expectRowCount, dataSet.rows.size());
        for (const auto& row : dataSet.rows) {
            ASSERT_EQ(expectColumnCount, row.values.size());
            auto vId = row.values[0].getStr();
            auto iter = std::find(vertices.begin(), vertices.end(), vId);
            ASSERT_TRUE(iter != vertices.end());
            if (expectStats != nullptr) {
                auto& expect = (*expectStats)[vId];
                auto& actual = row.values[1].getList().values;
                ASSERT_EQ(expect.size(), actual.size());
                for (size_t i = 0; i < expect.size(); i++) {
                    ASSERT_EQ(expect[i], actual[i]);
                }
            } else {
                ASSERT_EQ(Value::Type::__EMPTY__, row.values[1].type());
            }
            checkRowProps(row, dataSet.colNames, tags, edges);
        }
    }

    // check response when none of the tags or edges is specified, will return all props
    static void checkResponse(
            const nebula::DataSet& dataSet,
            const std::vector<VertexID>& vertices,
            size_t expectRowCount,
            size_t expectColumnCount) {
        ASSERT_EQ(expectColumnCount, dataSet.colNames.size());
        ASSERT_EQ(expectRowCount, dataSet.rows.size());
        for (const auto& row : dataSet.rows) {
            ASSERT_EQ(expectColumnCount, row.values.size());
            auto vId = row.values[0].getStr();
            auto iter = std::find(vertices.begin(), vertices.end(), vId);
            ASSERT_TRUE(iter != vertices.end());
            // the second column is stats
            ASSERT_EQ(Value::Type::__EMPTY__, row.values[1].type());
            // the last column is yeild expression
            ASSERT_EQ(Value::Type::__EMPTY__, row.values[expectColumnCount - 1].type());
            checkRowProps(row, dataSet.colNames, {}, {});
        }
    }

    static void checkResponse(const cpp2::LookupIndexResp& resp,
                              const std::vector<std::string>& expectCols,
                              std::vector<Row> expectRows) {
        EXPECT_EQ(0, resp.result.failed_parts.size());
        auto columns = resp.get_data()->colNames;
        EXPECT_EQ(expectCols, columns);
        EXPECT_EQ(expectRows.size(), resp.get_data()->rows.size());
        auto actualRows = resp.get_data()->rows;
        struct Descending {
            bool operator()(const Row& r1, const Row& r2) {
                if (r1.size() != r2.size()) {
                    return r1.size() < r2.size();
                }
                for (size_t i = 0; i < r1.size(); i++) {
                    if (r1[i] != r2[i]) {
                        return r1[i] < r2[i];
                    }
                }
                return true;
            }
        };
        std::sort(actualRows.begin(), actualRows.end(), Descending());
        std::sort(expectRows.begin(), expectRows.end(), Descending());
        EXPECT_EQ(expectRows, actualRows);
    }

    static void checkColNames(
            const nebula::DataSet& dataSet,
            const std::vector<std::pair<TagID, std::vector<std::string>>>& tags,
            const std::vector<std::pair<EdgeType, std::vector<std::string>>>& edges) {
        auto colNames = dataSet.colNames;
        ASSERT_EQ(colNames.size(), tags.size() + edges.size() + 3);
        ASSERT_EQ(kVid, colNames[0]);
        ASSERT_EQ(0, colNames[1].find("_stats"));
        ASSERT_EQ(0, colNames[colNames.size() - 1].find("_expr"));

        for (size_t i = 0; i < tags.size(); i++) {
            auto expected = "_tag:" + folly::to<std::string>(tags[i].first);
            for (const auto& prop : tags[i].second) {
                expected += ":" + prop;
            }
            ASSERT_EQ(expected, colNames[i + 2]);
        }
        for (size_t i = 0; i < edges.size(); i++) {
            std::string expected = "_edge:";
            expected.append(edges[i].first > 0 ? "+" : "-")
                    .append(folly::to<std::string>(std::abs(edges[i].first)));
            for (const auto& prop : edges[i].second) {
                expected += ":" + prop;
            }
            ASSERT_EQ(expected, colNames[i + 2 + tags.size()]);
        }
    }

    static void checkPlayer(const std::vector<std::string>& props,
                            const mock::Player& player,
                            const std::vector<Value>& values) {
        // if no property specified, would return all props
        if (props.empty()) {
            ASSERT_EQ(player.name_, values[0].getStr());
            ASSERT_EQ(player.age_, values[1].getInt());
            ASSERT_EQ(player.playing_, values[2].getBool());
            ASSERT_EQ(player.career_, values[3].getInt());
            ASSERT_EQ(player.startYear_, values[4].getInt());
            ASSERT_EQ(player.endYear_, values[5].getInt());
            ASSERT_EQ(player.games_, values[6].getInt());
            ASSERT_EQ(player.avgScore_, values[7].getFloat());
            ASSERT_EQ(player.serveTeams_, values[8].getInt());
            int32_t hasTtl = FLAGS_mock_ttl_col ? 1 : 0;
            if (player.country_.empty()) {
                // default value
                ASSERT_EQ("America", values[9 + hasTtl].getStr());
            } else {
                ASSERT_EQ(player.country_, values[9 + hasTtl].getStr());
            }
            if (player.champions_ == 0) {
                // 0 means not initialized, should return null
                ASSERT_EQ(NullType::__NULL__, values[10 + hasTtl].getNull());
            } else {
                ASSERT_EQ(player.champions_, values[10 + hasTtl].getInt());
            }
            return;
        }
        ASSERT_EQ(props.size(), values.size());
        for (size_t i = 0; i < props.size(); i++) {
            if (props[i] == "name") {
                ASSERT_EQ(player.name_, values[i].getStr());
            } else if (props[i] == "age") {
                ASSERT_EQ(player.age_, values[i].getInt());
            } else if (props[i] == "playing") {
                ASSERT_EQ(player.playing_, values[i].getBool());
            } else if (props[i] == "career") {
                ASSERT_EQ(player.career_, values[i].getInt());
            } else if (props[i] == "startYear") {
                ASSERT_EQ(player.startYear_, values[i].getInt());
            } else if (props[i] == "endYear") {
                ASSERT_EQ(player.endYear_, values[i].getInt());
            } else if (props[i] == "games") {
                ASSERT_EQ(player.games_, values[i].getInt());
            } else if (props[i] == "avgScore") {
                ASSERT_EQ(player.avgScore_, values[i].getFloat());
            } else if (props[i] == "serveTeams") {
                ASSERT_EQ(player.serveTeams_, values[i].getInt());
            } else if (props[i] == "country") {
                if (player.country_.empty()) {
                    // default value
                    ASSERT_EQ("America", values[i].getStr());
                } else {
                    ASSERT_EQ(player.country_, values[i].getStr());
                }
            } else if (props[i] == "champions") {
                if (player.champions_ == 0) {
                    // 0 means not initialized, should return null
                    ASSERT_EQ(NullType::__NULL__, values[i].getNull());
                } else {
                    ASSERT_EQ(player.champions_, values[i].getInt());
                }
            } else {
                LOG(FATAL) << "Should not reach here";
            }
        }
    }

    static void checkOutServe(EdgeType edgeType,
                              const std::vector<std::string>& props,
                              const std::vector<mock::Serve>& serves,
                              const std::vector<Value>& values) {
        if (props.empty()) {
            auto iter = std::find_if(serves.begin(), serves.end(), [&] (const auto& serve) {
                // Find corresponding record by team name and start year,
                // in case a player serve the same team more than once.
                // The teamName and startYear would be in col 5 and 6, the first four
                // property would be property in key
                return serve.teamName_ == values[1 + 4].getStr() &&
                       serve.startYear_ == values[2 + 4].getInt();
            });
            ASSERT_TRUE(iter != serves.end());
            checkAllPropertyOfServe(edgeType, *iter, values);
            return;
        }
        // Make sure teamName and startYear is the first two props,
        // otherwise we could not make sure which serve it is
        ASSERT_EQ("teamName", props[0]);
        ASSERT_EQ("startYear", props[1]);
        auto iter = std::find_if(serves.begin(), serves.end(), [&] (const auto& serve) {
            // find corresponding record by team name and start year
            return serve.teamName_ == values[0].getStr() && serve.startYear_ == values[1].getInt();
        });
        ASSERT_TRUE(iter != serves.end());
        checkSomePropertyOfServe(edgeType, props, *iter, values);
    }

    static void checkInServe(EdgeType edgeType,
                             const std::vector<std::string>& props,
                             const std::vector<mock::Serve>& serves,
                             const std::vector<Value>& values) {
        if (props.empty()) {
            auto iter = std::find_if(serves.begin(), serves.end(), [&] (const auto& serve) {
                // Find corresponding record by player name and start year,
                // in case a player serve the same team more than once.
                // The playerName and startYear would be in col 4 and 6, the first four
                // property would be property in key
                return serve.playerName_ == values[0 + 4].getStr() &&
                       serve.startYear_ == values[2 + 4].getInt();
            });
            ASSERT_TRUE(iter != serves.end());
            checkAllPropertyOfServe(edgeType, *iter, values);
            return;
        }
        // Make sure playerName and startYear is the first two props
        // otherwise we could not make sure which serve it is
        ASSERT_EQ("playerName", props[0]);
        ASSERT_EQ("startYear", props[1]);
        auto iter = std::find_if(serves.begin(), serves.end(), [&] (const auto& serve) {
            // find corresponding record by team name
            return serve.playerName_ == values[0].getStr() &&
                   serve.startYear_ == values[1].getInt();
        });
        ASSERT_TRUE(iter != serves.end());
        checkSomePropertyOfServe(edgeType, props, *iter, values);
    }

    static void checkAllPropertyOfServe(EdgeType edgeType,
                                        const mock::Serve& serve,
                                        const std::vector<Value>& values) {
        // property in key
        if (edgeType > 0) {
            ASSERT_EQ(serve.playerName_, values[0].getStr());
            ASSERT_EQ(edgeType, values[1].getInt());
            ASSERT_EQ(serve.startYear_, values[2].getInt());
            ASSERT_EQ(serve.teamName_, values[3].getStr());
        } else {
            ASSERT_EQ(serve.teamName_, values[0].getStr());
            ASSERT_EQ(edgeType, values[1].getInt());
            ASSERT_EQ(serve.startYear_, values[2].getInt());
            ASSERT_EQ(serve.playerName_, values[3].getStr());
        }
        // property in value
        ASSERT_EQ(serve.playerName_, values[4].getStr());
        ASSERT_EQ(serve.teamName_, values[5].getStr());
        ASSERT_EQ(serve.startYear_, values[6].getInt());
        ASSERT_EQ(serve.endYear_, values[7].getInt());
        ASSERT_EQ(serve.teamCareer_, values[8].getInt());
        ASSERT_EQ(serve.teamGames_, values[9].getInt());
        ASSERT_EQ(serve.teamAvgScore_, values[10].getFloat());
        int32_t hasTtl = FLAGS_mock_ttl_col ? 1 : 0;
        if (serve.type_.empty()) {
            ASSERT_EQ("trade", values[11 + hasTtl].getStr());
        } else {
            ASSERT_EQ(serve.type_, values[11 + hasTtl].getStr());
        }
        if (serve.champions_ == 0) {
            // 0 means not initialized, should return null
            ASSERT_EQ(NullType::__NULL__, values[12 + hasTtl].getNull());
        } else {
            ASSERT_EQ(serve.champions_, values[12 + hasTtl].getInt());
        }
    }

    static void checkSomePropertyOfServe(EdgeType edgeType,
                                         const std::vector<std::string>& props,
                                         const mock::Serve& serve,
                                         const std::vector<Value>& values) {
        for (size_t i = 0; i < props.size(); i++) {
            if (props[i] == "playerName") {
                ASSERT_EQ(serve.playerName_, values[i].getStr());
            } else if (props[i] == "teamName") {
                ASSERT_EQ(serve.teamName_, values[i].getStr());
            } else if (props[i] == "startYear") {
                ASSERT_EQ(serve.startYear_, values[i].getInt());
            } else if (props[i] == "endYear") {
                ASSERT_EQ(serve.endYear_, values[i].getInt());
            } else if (props[i] == "teamCareer") {
                ASSERT_EQ(serve.teamCareer_, values[i].getInt());
            } else if (props[i] == "teamGames") {
                ASSERT_EQ(serve.teamGames_, values[i].getInt());
            } else if (props[i] == "teamAvgScore") {
                ASSERT_EQ(serve.teamAvgScore_, values[i].getFloat());
            } else if (props[i] == "type") {
                if (serve.type_.empty()) {
                    ASSERT_EQ("trade", values[i].getStr());
                } else {
                    ASSERT_EQ(serve.type_, values[i].getStr());
                }
            } else if (props[i] == "champions") {
                if (serve.champions_ == 0) {
                    // 0 means not initialized, should return null
                    ASSERT_EQ(NullType::__NULL__, values[i].getNull());
                } else {
                    ASSERT_EQ(serve.champions_, values[i].getInt());
                }
            } else if (props[i] == kSrc) {
                if (edgeType > 0) {
                    ASSERT_EQ(serve.playerName_, values[i].getStr());
                } else {
                    ASSERT_EQ(serve.teamName_, values[i].getStr());
                }
            } else if (props[i] == kDst) {
                if (edgeType > 0) {
                    ASSERT_EQ(serve.teamName_, values[i].getStr());
                } else {
                    ASSERT_EQ(serve.playerName_, values[i].getStr());
                }
            } else if (props[i] == kRank) {
                ASSERT_EQ(serve.startYear_, values[i].getInt());
            } else if (props[i] == kType) {
                ASSERT_EQ(edgeType, values[i].getInt());
            } else {
                LOG(FATAL) << "Should not reach here";
            }
        }
    }

    static void checkTeammate(EdgeType edgeType,
                              const std::vector<std::string>& props,
                              const std::vector<Value>& values) {
        if (props.empty()) {
            auto player1 = values[4].getStr();
            auto player2 = values[5].getStr();
            const auto& teammate = findTeammate(player1, player2);
            // property in key
            ASSERT_TRUE(teammate.player1_ == values[0].getStr() ||
                        teammate.player2_ == values[0].getStr());
            ASSERT_EQ(edgeType, values[1].getInt());
            ASSERT_EQ(teammate.startYear_, values[2].getInt());
            ASSERT_TRUE(teammate.player1_ == values[3].getStr() ||
                        teammate.player2_ == values[3].getStr());
            // property in value
            ASSERT_EQ(teammate.player1_, values[4].getStr());
            ASSERT_EQ(teammate.player2_, values[5].getStr());
            ASSERT_EQ(teammate.teamName_, values[6].getStr());
            ASSERT_EQ(teammate.startYear_, values[7].getInt());
            ASSERT_EQ(teammate.endYear_, values[8].getInt());
            return;
        }
        // Make sure _src and _dst is the first two props
        // otherwise we could not make sure which data it is
        CHECK_GE(props.size(), 2);
        ASSERT_EQ("player1", props[0]);
        ASSERT_EQ("player2", props[1]);
        ASSERT_EQ(props.size(), values.size());
        auto player1 = values[0].getStr();
        auto player2 = values[1].getStr();
        const auto& teammate = findTeammate(player1, player2);
        for (size_t i = 0; i < props.size(); i++) {
            if (props[i] == "_src") {
                ASSERT_EQ(teammate.player1_, values[i].getStr());
            } else if (props[i] == "_dst") {
                ASSERT_EQ(teammate.player2_, values[i].getStr());
            } else if (props[i] == "_rank") {
                ASSERT_EQ(teammate.startYear_, values[i].getInt());
            } else if (props[i] == "_type") {
                ASSERT_EQ(edgeType, values[i].getInt());
            } else if (props[i] == "player1") {
                ASSERT_EQ(teammate.player1_, values[i].getStr());
            } else if (props[i] == "player2") {
                ASSERT_EQ(teammate.player2_, values[i].getStr());
            } else if (props[i] == "teamName") {
                ASSERT_EQ(teammate.teamName_, values[i].getStr());
            } else if (props[i] == "startYear") {
                ASSERT_EQ(teammate.startYear_, values[i].getInt());
            } else if (props[i] == "endYear") {
                ASSERT_EQ(teammate.endYear_, values[i].getInt());
            } else {
                LOG(FATAL) << "Should not reach here";
            }
        }
    }

    static void checkRowProps(
            const nebula::Row& row,
            const std::vector<std::string> colNames,
            const std::vector<std::pair<TagID, std::vector<std::string>>>& tags,
            const std::vector<std::pair<EdgeType, std::vector<std::string>>>& edges) {
        auto vId = row.values[0].getStr();
        // skip the last column which is reserved for expression yields
        for (size_t i = 2; i < colNames.size() - 1; i++) {
            const auto& name = colNames[i];
            std::vector<std::string> cols;
            folly::split(':', name, cols, true);
            if (cols.size() < 2) {
                LOG(FATAL) << "Invalid column name";
            }
            // cols[1] is the tagName, which can be transfromed to entryId
            auto entryId = folly::to<int32_t>(cols[1]);
            auto props = findExpectProps(entryId, tags, edges);
            switch (entryId) {
                case 1: {
                    // tag player
                    auto iter = std::find_if(
                            mock::MockData::players_.begin(), mock::MockData::players_.end(),
                            [&] (const auto& player) { return player.name_ == vId; });
                    if (iter != mock::MockData::players_.end()) {
                        auto tagCell = row.values[i].getList();
                        checkPlayer(props, *iter, tagCell.values);
                    } else {
                        ASSERT_EQ(Value::Type::__EMPTY__, row.values[i].type());
                    }
                    break;
                }
                case 2: {
                    // tag team
                    auto iter = std::find(mock::MockData::teams_.begin(),
                                          mock::MockData::teams_.end(),
                                          vId);
                    if (iter != mock::MockData::teams_.end()) {
                        auto tagCell = row.values[i].getList();
                        ASSERT_EQ(1, tagCell.values.size());
                        ASSERT_EQ(*iter, tagCell.values[0].getStr());
                    } else {
                        ASSERT_EQ(Value::Type::__EMPTY__, row.values[i].type());
                    }
                    break;
                }
                case 101: {
                    // check out edge serve
                    auto iter = mock::MockData::playerServes_.find(vId);
                    if (iter != mock::MockData::playerServes_.end()) {
                        auto edgeCell = row.values[i].getList();
                        ASSERT_EQ(iter->second.size(), edgeCell.values.size());
                        for (const auto& edgeRow : edgeCell.values) {
                            auto& values = edgeRow.getList().values;
                            checkOutServe(entryId, props, iter->second, values);
                        }
                    } else {
                        ASSERT_EQ(Value::Type::__EMPTY__, row.values[i].type());
                    }
                    break;
                }
                case -101: {
                    // check in edge serve
                    auto iter = mock::MockData::teamServes_.find(vId);
                    if (iter != mock::MockData::teamServes_.end()) {
                        auto edgeCell = row.values[i].getList();
                        ASSERT_EQ(iter->second.size(), edgeCell.values.size());
                        for (const auto& edgeRow : edgeCell.values) {
                            auto& values = edgeRow.getList().values;
                            checkInServe(entryId, props, iter->second, values);
                        }
                    } else {
                        ASSERT_EQ(Value::Type::__EMPTY__, row.values[i].type());
                    }
                    break;
                }
                case 102:
                case -102: {
                    auto iter = std::find_if(mock::MockData::teammates_.begin(),
                                             mock::MockData::teammates_.end(),
                                             [&](const auto& teammate) {
                                                 return teammate.player1_ == vId ||
                                                        teammate.player2_ == vId;
                                             });
                    if (iter != mock::MockData::teammates_.end()) {
                        auto edgeCell = row.values[i].getList();
                        for (const auto& edgeRow : edgeCell.values) {
                            auto& values = edgeRow.getList().values;
                            checkTeammate(entryId, props, values);
                        }
                    } else {
                        ASSERT_EQ(Value::Type::__EMPTY__, row.values[i].type());
                    }
                    break;
                }
                default:
                    break;
            }
        }
    }

    static const mock::Teammate& findTeammate(const std::string& player1,
                                              const std::string& player2) {
        auto iter = std::find_if(mock::MockData::teammates_.begin(),
                                 mock::MockData::teammates_.end(),
                                 [&](const auto& teammate) {
                                     return teammate.player1_ == player1 &&
                                            teammate.player2_ == player2;
                                 });
        if (iter != mock::MockData::teammates_.end()) {
            return *iter;
        }
        iter = std::find_if(mock::MockData::teammates_.begin(),
                            mock::MockData::teammates_.end(),
                            [&](const auto& teammate) {
                                return teammate.player1_ == player2 &&
                                       teammate.player2_ == player1;
                            });
        if (iter == mock::MockData::teammates_.end()) {
            LOG(FATAL) << "Can't find speicied teammate";
        }
        return *iter;
    }

    static std::vector<std::string> findExpectProps(
            int32_t entryId,
            const std::vector<std::pair<TagID, std::vector<std::string>>>& tags,
            const std::vector<std::pair<EdgeType, std::vector<std::string>>>& edges) {
        auto propIter = std::find_if(tags.begin(), tags.end(), [&](const auto& entry) {
            return entry.first == entryId;
        });
        if (propIter != tags.end()) {
            return propIter->second;
        }
        propIter = std::find_if(edges.begin(), edges.end(), [&](const auto& entry) {
            return entry.first == entryId;
        });
        if (propIter != edges.end()) {
            return propIter->second;
        }
        // return {} means all property
        return {};
    }
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_TEST_QUERYTESTUTILS_H_
