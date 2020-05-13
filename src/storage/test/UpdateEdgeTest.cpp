/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "common/NebulaKeyUtils.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include <limits>
#include "fs/TempDir.h"
#include "storage/test/TestUtils.h"
#include "storage/mutate/UpdateEdgeProcessor.h"
#include "mock/MockCluster."
#include "mock/MockData.h"

namespace nebula {
namespace storage {

static bool mockEdgeData(storage::StorageEnv* env, int32_t totalParts， int32_t spaceVidLen) {
    GraphSpaceID spaceId = 1;
    auto edgesPart = mock::MockData::mockEdgesofPart();
       
    folly::Baton<true, std::atomic> baton;
    std::atomic<size_t> count(edges.size());
       
    for (const auto& part : edgesPart) {
        std::vector<kvstore::KV> data;
        data.clear();
        auto size = part.second.size();

        for (const auto& edge : part.second) {
            auto key = NebulaKeyUtils::edgeKey(spaceVidLen, part.first, edge.srcId_, edge.type_,
                                               edge.rank_, edge.dstId_, 0L);
            auto schema = env->schemaMan_->getEdgeSchema(spaceId, std::abs(edge.type_));
            if (!schema) {
                LOG(ERROR) << "Invalid edge " << edge.type_;
                return false;
            }
               
            auto ret = encode(schema.get(), key, edge.props_, data);
            if (!ret) {
                LOG(ERROR) << "Write field failed";
                return false;
            }
        }
        env->kvstore_->asyncMultiPut(spaceId, part.first, std::move(data),
                                     [&](kvstore::ResultCode code) {
                                         EXPECT_EQ(code, kvstore::ResultCode::SUCCEEDED);
                                         count.fetch_sub(size);
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

TEST(UpdateEdgeTest, No_Filter_Yield_Test) {
    fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();

    GraphSpaceID spaceId = 1;
    TagID tagId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    if (!status.ok()) {
        LOG(ERROR) << "Get space vid length failed";
        return false;
    }              
    auto spaceVidLen = status.value();

    EXPECT_TRUE(mockEdgeData(env, 6, spaceVidLen));

    LOG(INFO) << "Build UpdateEdgeRequest...";
    cpp::UpdateEdgeRequest req;
    auto partId = std::hash<std::string>()("Tim Duncan") % parts + 1;
    req.set_space_id(spaceId);
    req.set_part_id(partId);
    VertexID srcId = "Tim Duncan";
    VertexID dstId = "Spurs";
    EdgeRanking rank = 1997;
    EdgeType edgeType = 101;
    // src = Tim Duncan, edge_type = 101, ranking = 1997, dst = Spurs
    storage::cpp2::EdgeKey edgeKey;
    edgeKey.set_src(srcId);
    edgeKey.set_edge_type(edgeType);
    edgeKey.set_ranking(rank);
    edgeKey.set_dst(dstId);
    req.set_edge_key(edgeKey);
   
    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedEdgeProp> props;
    // int: 101.teamCareer = 20 
    cpp2::UpdatedEdgeProp prop1;
    prop1.set_name("teamCareer");
    PrimaryExpression val1(20);
    prop1.set_value(Expression::encode(&val1));
    props.emplace_back(prop1);
    
    // bool: 101.starting = false
    cpp2::UpdatedEdgeProp prop2;
    prop1.set_name("starting");
    bool isStart = false;
    PrimaryExpression val2(isStart);
    prop1.set_value(Expression::encode(&val2));
    props.emplace_back(prop1);
    req.set_updated_props(std::move(props));

    LOG(INFO) << "Build yield...";
    // Return player props: playerName, teamName, teamCareer, starting
    decltype(req.return_props) tmpProps;
    std::string alias("101");
    std::string propName1("playerName");
    SourcePropertyExpression sourcePropExp1(&alias, &propName1);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp1));

    std::string propName2("teamName");
    SourcePropertyExpression sourcePropExp2(&alias, &propName2);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp2));

    std::string propName3("teamCareer");
    SourcePropertyExpression sourcePropExp3(&alias, &propName3);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp3));

    std::string propName4("starting");
    SourcePropertyExpression sourcePropExp4(&alias, &propName4);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp4));

    req.set_return_props(std::move(tmpProps));
    req.set_insertable(false);

    LOG(INFO) << "Test UpdateEdgeRequest...";
    auto* processor = UpdateEdgeProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(0, resp.result.failed_parts.size());
    EXPECT_EQ(5, resp.props.column_names.size());
    EXPECT_EQ("_inserted", resp.props.column_names[0]);
    EXPECT_EQ("101:playerName", resp.props.column_names[1]);
    EXPECT_EQ("101:teamName", resp.props.column_names[2]);
    EXPECT_EQ("101:teamCareer", resp.props.column_names[3]);
    EXPECT_EQ("101:starting", resp.props.column_names[4]);

    EXPECT_EQ(1, resp.props.rows.size());
    EXPECT_EQ(5, resp.props.rows[0].columns.size());

    EXPECT_EQ(false, resp.props.rows[0].columns[0]);
    EXPECT_EQ("Tim Duncan", resp.props.rows[0]column_names[1]);
    EXPECT_EQ("Spurs", resp.props.rows[0].column_names[2]);
    EXPECT_EQ(20, resp.props.rows[0].column_names[3]);   
    EXPECT_EQ(false, resp.props.rows[0].column_names[4]);  

    // get serve from kvstore directly
    auto prefix = NebulaKeyUtils::edgePrefix(spaceVidLen, partId, srcId, edgeType, rank, dstId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
    EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, ret);
    EXPECT_TRUE(iter && iter->valid());
    
    auto edgeReader = RowReader::getEdgePropReader(env->schemaMan_, spaceId, std::abs(edgeType), iter->val());
    auto val = edgeReader->getValueByName("playerName");
    auto val  = RowReader::getPropByName(edgeReader.get(), "playerName");
    EXPECT_EQ("Tim Duncan", val.getStr());

    val = reader->getValueByName("teamName");
    EXPECT_EQ("Spurs", val.getStr());

    val = reader->getValueByName("teamCareer");
    EXPECT_EQ(20, val.getInt());

    val = reader->getValueByName("starting");
    EXPECT_EQ(false, val.getBool());
}

TEST(UpdateEdgeTest, Filter_Yield_Test) {
    fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();

    GraphSpaceID spaceId = 1;
    TagID tagId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    if (!status.ok()) {
        LOG(ERROR) << "Get space vid length failed";
        return false;
    }              
    auto spaceVidLen = status.value();

    EXPECT_TRUE(mockEdgeData(env, 6, spaceVidLen));

    LOG(INFO) << "Build UpdateEdgeRequest...";
    cpp::UpdateEdgeRequest req;
    auto partId = std::hash<std::string>()("Tim Duncan") % parts + 1;
    req.set_space_id(spaceId);
    req.set_part_id(partId);
    VertexID srcId = "Tim Duncan";
    VertexID dstId = "Spurs";
    EdgeRanking rank = 1997;
    EdgeType edgeType = 101;
    // src = Tim Duncan, edge_type = 101, ranking = 1997, dst = Spurs
    storage::cpp2::EdgeKey edgeKey;
    edgeKey.set_src(srcId);
    edgeKey.set_edge_type(edgeType);
    edgeKey.set_ranking(rank);
    edgeKey.set_dst(dstId);
    req.set_edge_key(edgeKey);

    LOG(INFO) << "Build condition...";
    std::string alias("101");
    // left int:  101.startYear = 1997
    std::string propName1("startYear");
    auto* srcExp1 = new SourcePropertyExpression(&alias, &propName1);
    auto* priExp1 = new PrimaryExpression(1997L);
    auto* left = new RelationalExpression(srcExp1,
                                          RelationalExpression::Operator::EQ,                              
                                          priExp1);
 
    // right int: 101.endYear = 2017
    std::string propName2("endYear");
    auto* srcExp2 = new SourcePropertyExpression(&alias, &propName2);
    auto* priExp2 = new PrimaryExpression(2017L);
    auto* right = new RelationalExpression(srcExp2,
                                           RelationalExpression::Operator::EQ,
                                           priExp2);
    // left AND right is ture
    auto logExp = std::make_unique<LogicalExpression>(left, LogicalExpression::AND, right);
    req.set_condition(Expression::encode(logExp.get()));
    
    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedEdgeProp> props;
    // int: 101.teamCareer = 20 
    cpp2::UpdatedEdgeProp prop1;
    prop1.set_name("teamCareer");
    PrimaryExpression val1(20);
    prop1.set_value(Expression::encode(&val1));
    props.emplace_back(prop1);
    
    // bool: 101.starting = false
    cpp2::UpdatedEdgeProp prop2;
    prop1.set_name("starting");
    bool isStart = false;
    PrimaryExpression val2(isStart);
    prop1.set_value(Expression::encode(&val2));
    props.emplace_back(prop1);
    req.set_updated_props(std::move(props));

    LOG(INFO) << "Build yield...";
    // Return player props: playerName, teamName, teamCareer, starting
    decltype(req.return_props) tmpProps;
    std::string alias("101");
    std::string propName1("playerName");
    SourcePropertyExpression sourcePropExp1(&alias, &propName1);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp1));

    std::string propName2("teamName");
    SourcePropertyExpression sourcePropExp2(&alias, &propName2);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp2));

    std::string propName3("teamCareer");
    SourcePropertyExpression sourcePropExp3(&alias, &propName3);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp3));

    std::string propName4("starting");
    SourcePropertyExpression sourcePropExp4(&alias, &propName4);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp4));

    req.set_return_props(std::move(tmpProps));
    req.set_insertable(false);

    LOG(INFO) << "Test UpdateEdgeRequest...";
    auto* processor = UpdateEdgeProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(0, resp.result.failed_parts.size());
    EXPECT_EQ(5, resp.props.column_names.size());
    EXPECT_EQ("_inserted", resp.props.column_names[0]);
    EXPECT_EQ("101:playerName", resp.props.column_names[1]);
    EXPECT_EQ("101:teamName", resp.props.column_names[2]);
    EXPECT_EQ("101:teamCareer", resp.props.column_names[3]);
    EXPECT_EQ("101:starting", resp.props.column_names[4]);

    EXPECT_EQ(0, resp.props.rows.size());

    // get serve from kvstore directly
    // Because no update, the value is old
    auto prefix = NebulaKeyUtils::edgePrefix(spaceVidLen, partId, srcId, edgeType, rank, dstId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
    EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, ret);
    EXPECT_TRUE(iter && iter->valid());
    
    auto edgeReader = RowReader::getEdgePropReader(env->schemaMan_, spaceId, std::abs(edgeType), iter->val());
    auto val = edgeReader->getValueByName("playerName");
    auto val  = RowReader::getPropByName(edgeReader.get(), "playerName");
    EXPECT_EQ("Tim Duncan", val.getStr());

    val = reader->getValueByName("teamName");
    EXPECT_EQ("Spurs", val.getStr());

    val = reader->getValueByName("teamCareer");
    EXPECT_EQ(19, val.getInt());

    val = reader->getValueByName("starting");
    EXPECT_EQ(true, val.getBool());
}


TEST(UpdateEdgeTest, Insertable_Test) {
    fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();

    GraphSpaceID spaceId = 1;
    TagID tagId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    if (!status.ok()) {
        LOG(ERROR) << "Get space vid length failed";
        return false;
    }              
    auto spaceVidLen = status.value();

    EXPECT_TRUE(mockEdgeData(env, 6, spaceVidLen));

    LOG(INFO) << "Build UpdateEdgeRequest...";
    cpp::UpdateEdgeRequest req;
    auto partId = std::hash<std::string>()("Brandon Ingram") % parts + 1;
    req.set_space_id(spaceId);
    req.set_part_id(partId);
    VertexID srcId = "Brandon Ingram";
    VertexID dstId = "Lakers";
    EdgeRanking rank = 2016;
    EdgeType edgeType = 101;
    // src = Brandon Ingram, edge_type = 101, ranking = 2016, dst = Lakers
    storage::cpp2::EdgeKey edgeKey;
    edgeKey.set_src(srcId);
    edgeKey.set_edge_type(edgeType);
    edgeKey.set_ranking(rank);
    edgeKey.set_dst(dstId);
    req.set_edge_key(edgeKey);
    
    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedEdgeProp> props;
    
    // Because not default value and no nullable, so provide values
    // string: 101.playerName = Brandon Ingram
    cpp2::UpdatedEdgeProp prop1;
    prop1.set_name("playerName");
    std::string col1new("Brandon Ingram");
    PrimaryExpression val1(col1new);
    prop1.set_value(Expression::encode(&val1));
    props.emplace_back(prop1);
    
    // string: 101.starting = Lakers
    cpp2::UpdatedEdgeProp prop2;
    prop2.set_name("teamName");
    std::string col2new("Lakers");
    PrimaryExpression val2(col2new);
    prop1.set_value(Expression::encode(&val2));
    props.emplace_back(prop2);

    // int: 101.startYear = 2016
    cpp2::UpdatedEdgeProp prop3;
    prop3.set_name("startYear");
    PrimaryExpression val3(2016);
    prop3.set_value(Expression::encode(&val3));
    props.emplace_back(prop3);

    // int: 101.endYear = 2019
    cpp2::UpdatedEdgeProp prop4;
    prop4.set_name("endYear");
    PrimaryExpression val4(2019);
    prop4.set_value(Expression::encode(&val4));
    props.emplace_back(prop4);

    // int: 101.teamCareer = 3
    cpp2::UpdatedEdgeProp prop5;
    prop5.set_name("teamCareer");
    PrimaryExpression val5(3);
    prop5.set_value(Expression::encode(&val5));
    props.emplace_back(prop5);

    // int: 101.teamGames = 190
    cpp2::UpdatedEdgeProp prop6;
    prop6.set_name("teamGames");
    PrimaryExpression val6(190);
    prop6.set_value(Expression::encode(&val6));
    props.emplace_back(prop6);

    // int: 101.teamAvgScore = 24.3
    cpp2::UpdatedEdgeProp prop7;
    prop7.set_name("teamAvgScore");
    PrimaryExpression val7(24.3);
    prop7.set_value(Expression::encode(&val7));
    props.emplace_back(prop7);
    
    req.set_updated_props(std::move(props));

    LOG(INFO) << "Build yield...";
    // Return player props: playerName, teamName, teamCareer, starting
    decltype(req.return_props) tmpProps;
    std::string alias("101");
    std::string propName1("playerName");
    SourcePropertyExpression sourcePropExp1(&alias, &propName1);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp1));

    std::string propName2("teamName");
    SourcePropertyExpression sourcePropExp2(&alias, &propName2);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp2));

    std::string propName3("teamCareer");
    SourcePropertyExpression sourcePropExp3(&alias, &propName3);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp3));

    std::string propName4("starting");
    SourcePropertyExpression sourcePropExp4(&alias, &propName4);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp4));

    req.set_return_props(std::move(tmpProps));
    req.set_insertable(true);

    LOG(INFO) << "Test UpdateEdgeRequest...";
    auto* processor = UpdateEdgeProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(0, resp.result.failed_parts.size());
    EXPECT_EQ(5, resp.props.column_names.size());
    EXPECT_EQ("_inserted", resp.props.column_names[0]);
    EXPECT_EQ("101:playerName", resp.props.column_names[1]);
    EXPECT_EQ("101:teamName", resp.props.column_names[2]);
    EXPECT_EQ("101:teamCareer", resp.props.column_names[3]);
    EXPECT_EQ("101:starting", resp.props.column_names[4]);

    EXPECT_EQ(1, resp.props.rows.size());
    EXPECT_EQ(5, resp.props.rows[0].columns.size());

    EXPECT_EQ(true, resp.props.rows[0].columns[0]);
    EXPECT_EQ("Brandon Ingram", resp.props.rows[0]column_names[1]);
    EXPECT_EQ("Lakers", resp.props.rows[0].column_names[2]);
    EXPECT_EQ(3, resp.props.rows[0].column_names[3]);   
    EXPECT_EQ(false, resp.props.rows[0].column_names[4]);  

    // get serve from kvstore directly
    auto prefix = NebulaKeyUtils::edgePrefix(spaceVidLen, partId, srcId, edgeType, rank, dstId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
    EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, ret);
    EXPECT_TRUE(iter && iter->valid());
    
    auto edgeReader = RowReader::getEdgePropReader(env->schemaMan_, spaceId, std::abs(edgeType), iter->val());
    auto val = edgeReader->getValueByName("playerName");
    auto val  = RowReader::getPropByName(edgeReader.get(), "playerName");
    EXPECT_EQ("Brandon Ingram", val.getStr());

    val = reader->getValueByName("teamName");
    EXPECT_EQ("Lakers", val.getStr());

    val = reader->getValueByName("teamCareer");
    EXPECT_EQ(3, val.getInt());

    val = reader->getValueByName("starting");
    EXPECT_EQ(false, val.getBool());
}

TEST(UpdateEdgeTest, Invalid_Update_Prop_Test) {
    fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();

    GraphSpaceID spaceId = 1;
    TagID tagId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    if (!status.ok()) {
        LOG(ERROR) << "Get space vid length failed";
        return false;
    }              
    auto spaceVidLen = status.value();

    EXPECT_TRUE(mockEdgeData(env, 6, spaceVidLen));

    LOG(INFO) << "Build UpdateEdgeRequest...";
    cpp::UpdateEdgeRequest req;
    auto partId = std::hash<std::string>()("Tim Duncan") % parts + 1;
    req.set_space_id(spaceId);
    req.set_part_id(partId);
    VertexID srcId = "Tim Duncan";
    VertexID dstId = "Spurs";
    EdgeRanking rank = 1997;
    EdgeType edgeType = 101;
    // src = Tim Duncan, edge_type = 101, ranking = 1997, dst = Spurs
    storage::cpp2::EdgeKey edgeKey;
    edgeKey.set_src(srcId);
    edgeKey.set_edge_type(edgeType);
    edgeKey.set_ranking(rank);
    edgeKey.set_dst(dstId);
    req.set_edge_key(edgeKey);
    
    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedEdgeProp> props;
    // int: 101.teamCareer = 20 
    cpp2::UpdatedEdgeProp prop1;
    prop1.set_name("teamCareer");
    PrimaryExpression val1(30);
    prop1.set_value(Expression::encode(&val1));
    props.emplace_back(prop1);
    
    // bool: 101.birth = 1997
    cpp2::UpdatedEdgeProp prop2;
    prop2.set_name("birth");
    PrimaryExpression val2(1997);
    prop2.set_value(Expression::encode(&val2));
    props.emplace_back(prop2);
    req.set_updated_props(std::move(props));

    LOG(INFO) << "Build yield...";
    // Return player props: playerName, teamName, teamCareer, starting
    decltype(req.return_props) tmpProps;
    std::string alias("101");
    std::string propName1("playerName");
    SourcePropertyExpression sourcePropExp1(&alias, &propName1);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp1));

    std::string propName2("teamName");
    SourcePropertyExpression sourcePropExp2(&alias, &propName2);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp2));

    std::string propName3("teamCareer");
    SourcePropertyExpression sourcePropExp3(&alias, &propName3);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp3));

    std::string propName4("birth");
    SourcePropertyExpression sourcePropExp4(&alias, &propName4);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp4));

    req.set_return_props(std::move(tmpProps));
    req.set_insertable(false);

    LOG(INFO) << "Test UpdateEdgeRequest...";
    auto* processor = UpdateEdgeProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(1, resp.result.failed_parts.size());
    EXPECT_TRUE(nebula::storage::cpp2::ErrorCode::E_INVALID_UPDATER
                    == resp.result.failed_parts[0].code);

    // get serve from kvstore directly
    // Because no update, the value is old
    auto prefix = NebulaKeyUtils::edgePrefix(spaceVidLen, partId, srcId, edgeType, rank, dstId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
    EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, ret);
    EXPECT_TRUE(iter && iter->valid());
    
    auto edgeReader = RowReader::getEdgePropReader(env->schemaMan_, spaceId, std::abs(edgeType), iter->val());
    auto val = edgeReader->getValueByName("playerName");
    auto val  = RowReader::getPropByName(edgeReader.get(), "playerName");
    EXPECT_EQ("Tim Duncan", val.getStr());

    val = reader->getValueByName("teamName");
    EXPECT_EQ("Spurs", val.getStr());

    val = reader->getValueByName("teamCareer");
    EXPECT_EQ(19, val.getInt());

    val = reader->getValueByName("starting");
    EXPECT_EQ(true, val.getBool());
}

TEST(UpdateEdgeTest, Invalid_Filter_Test) {
    fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();

    GraphSpaceID spaceId = 1;
    TagID tagId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    if (!status.ok()) {
        LOG(ERROR) << "Get space vid length failed";
        return false;
    }              
    auto spaceVidLen = status.value();

    EXPECT_TRUE(mockEdgeData(env, 6, spaceVidLen));

    LOG(INFO) << "Build UpdateEdgeRequest...";
    cpp::UpdateEdgeRequest req;
    auto partId = std::hash<std::string>()("Tim Duncan") % parts + 1;
    req.set_space_id(spaceId);
    req.set_part_id(partId);
    VertexID srcId = "Tim Duncan";
    VertexID dstId = "Spurs";
    EdgeRanking rank = 1997;
    EdgeType edgeType = 101;
    // src = Tim Duncan, edge_type = 101, ranking = 1997, dst = Spurs
    storage::cpp2::EdgeKey edgeKey;
    edgeKey.set_src(srcId);
    edgeKey.set_edge_type(edgeType);
    edgeKey.set_ranking(rank);
    edgeKey.set_dst(dstId);
    req.set_edge_key(edgeKey);

    LOG(INFO) << "Build condition...";
     std::string alias("101");
    // left int:  101.startYear = 1997
    std::string propName1("startYear");
    auto* srcExp1 = new SourcePropertyExpression(&alias, &propName1);
    auto* priExp1 = new PrimaryExpression(1997L);
    auto* left = new RelationalExpression(srcExp1,
                                          RelationalExpression::Operator::EQ,                              
                                          priExp1);
 
    // right int: 101.birth = 1990
    std::string propName2("birth");
    auto* srcExp2 = new SourcePropertyExpression(&alias, &propName2);
    auto* priExp2 = new PrimaryExpression(1990);
    auto* right = new RelationalExpression(srcExp2,
                                           RelationalExpression::Operator::EQ,
                                           priExp2);
    // left AND right is ture
    auto logExp = std::make_unique<LogicalExpression>(left, LogicalExpression::AND, right);
    req.set_condition(Expression::encode(logExp.get()));
    
    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedEdgeProp> props;
    // int: 101.teamCareer = 20 
    cpp2::UpdatedEdgeProp prop1;
    prop1.set_name("teamCareer");
    PrimaryExpression val1(30);
    prop1.set_value(Expression::encode(&val1));
    props.emplace_back(prop1);
    
    // bool: 101.starting = false
    cpp2::UpdatedEdgeProp prop2;
    prop1.set_name("starting");
    bool isStart = false;
    PrimaryExpression val2(isStart);
    prop1.set_value(Expression::encode(&val2));
    props.emplace_back(prop1);
    req.set_updated_props(std::move(props));

    LOG(INFO) << "Build yield...";
    // Return player props: playerName, teamName, teamCareer, starting
    decltype(req.return_props) tmpProps;
    std::string alias("101");
    std::string propName1("playerName");
    SourcePropertyExpression sourcePropExp1(&alias, &propName1);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp1));

    std::string propName2("teamName");
    SourcePropertyExpression sourcePropExp2(&alias, &propName2);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp2));

    std::string propName3("teamCareer");
    SourcePropertyExpression sourcePropExp3(&alias, &propName3);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp3));

    std::string propName4("starting");
    SourcePropertyExpression sourcePropExp4(&alias, &propName4);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp4));

    req.set_return_props(std::move(tmpProps));
    req.set_insertable(false);

    LOG(INFO) << "Test UpdateEdgeRequest...";
    auto* processor = UpdateEdgeProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(1, resp.result.failed_parts.size());

    EXPECT_TRUE(nebula::storage::cpp2::ErrorCode::E_INVALID_FILTER
                   == resp.result.failed_parts[0].code);

    // get serve from kvstore directly
    // Because no update, the value is old
    auto prefix = NebulaKeyUtils::edgePrefix(spaceVidLen, partId, srcId, edgeType, rank, dstId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
    EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, ret);
    EXPECT_TRUE(iter && iter->valid());
    
    auto edgeReader = RowReader::getEdgePropReader(env->schemaMan_, spaceId, std::abs(edgeType), iter->val());
    auto val = edgeReader->getValueByName("playerName");
    auto val  = RowReader::getPropByName(edgeReader.get(), "playerName");
    EXPECT_EQ("Tim Duncan", val.getStr());

    val = reader->getValueByName("teamName");
    EXPECT_EQ("Spurs", val.getStr());

    val = reader->getValueByName("teamCareer");
    EXPECT_EQ(19, val.getInt());

    val = reader->getValueByName("starting");
    EXPECT_EQ(true, val.getBool());
}

TEST(UpdateEdgeTest, CorruptDataTest) {
    fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();

    GraphSpaceID spaceId = 1;
    TagID tagId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    if (!status.ok()) {
        LOG(ERROR) << "Get space vid length failed";
        return false;
    }              
    auto spaceVidLen = status.value();

    LOG(INFO) << "Write a vertex with empty value!";
    auto partId = std::hash<std::string>()("Lonzo Ball") % parts + 1;
    VertexID srcId = "Lonzo Ball";
    VertexID dstId = "Lakers";
    EdgeRanking rank = 2017;
    EdgeType edgeType = 101;
    auto key = NebulaKeyUtils::edgeKey(spaceVidLen,
                                       partId,
                                       srcId,
                                       edgeType,
                                       rank,
                                       dstId,
                                       0L);

    std::vector<kvstore::KV> data;
    data.emplace_back(std::make_pair(key, ""));
    folly::Baton<> baton;
    env->kvstore_->asyncMultiPut(spaceId, partId, std::move(data),
        [&](kvstore::ResultCode code) {
            CHECK_EQ(code, kvstore::ResultCode::SUCCEEDED);
            baton.post();
        }); 
    baton.wait();

    LOG(INFO) << "Build UpdateEdgeRequest...";
    cpp::UpdateEdgeRequest req;
    req.set_space_id(spaceId);
    req.set_part_id(partId);
    // src = Lonzo Ball, edge_type = 101, ranking = 2017, dst = Lakers
    storage::cpp2::EdgeKey edgeKey;
    edgeKey.set_src(srcId);
    edgeKey.set_edge_type(edgeType);
    edgeKey.set_ranking(rank);
    edgeKey.set_dst(dstId);
    req.set_edge_key(edgeKey);
    
    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedEdgeProp> props;
    // int: 101.teamCareer = 2 
    cpp2::UpdatedEdgeProp prop1;
    prop1.set_name("teamCareer");
    PrimaryExpression val1(2);
    prop1.set_value(Expression::encode(&val1));
    props.emplace_back(prop1);
    
    // bool: 101.starting = false
    cpp2::UpdatedEdgeProp prop2;
    prop1.set_name("starting");
    bool isStart = true;
    PrimaryExpression val2(isStart);
    prop1.set_value(Expression::encode(&val2));
    props.emplace_back(prop1);
    req.set_updated_props(std::move(props));

    LOG(INFO) << "Build yield...";
    // Return player props: playerName, teamName, teamCareer, starting
    decltype(req.return_props) tmpProps;
    std::string alias("101");
    std::string propName1("playerName");
    SourcePropertyExpression sourcePropExp1(&alias, &propName1);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp1));

    std::string propName2("teamName");
    SourcePropertyExpression sourcePropExp2(&alias, &propName2);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp2));

    std::string propName3("teamCareer");
    SourcePropertyExpression sourcePropExp3(&alias, &propName3);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp3));

    std::string propName4("starting");
    SourcePropertyExpression sourcePropExp4(&alias, &propName4);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp4));

    req.set_return_props(std::move(tmpProps));
    req.set_insertable(false);

    LOG(INFO) << "Test UpdateEdgeRequest...";
    auto* processor = UpdateEdgeProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(1, resp.result.failed_parts.size());

    EXPECT_TRUE(nebula::storage::cpp2::ErrorCode::E_EDGE_NOT_FOUND
                   == resp.result.failed_parts[0].code);
}

}  // namespace storage
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}

