/* Copyright (c) 2020 vesoft inc. All rights reserved.
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
#include "storage/mutate/UpdateVertexProcessor.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"

namespace nebula {
namespace storage {

bool mockVertexData(storage::StorageEnv* env, int32_t totalParts, int32_t spaceVidLen) {
    GraphSpaceID spaceId = 1;
    auto verticesPart = mock::MockData::mockVerticesofPart();

    folly::Baton<true, std::atomic> baton;
    std::atomic<size_t> count(verticesPart.size());

    for (const auto& part : verticesPart) {
        std::vector<kvstore::KV> data;
        data.clear();
        for (const auto& vertex : part.second) {
            TagID tagId = vertex.tId_;
            auto key = NebulaKeyUtils::vertexKey(spaceVidLen, part.first, vertex.vId_, tagId, 0L);
            auto schema = env->schemaMan_->getTagSchema(spaceId, tagId);
            if (!schema) {
                LOG(ERROR) << "Invalid tagId " << tagId;
                return false;
            }

            auto ret = encode(schema.get(), key, vertex.props_, data);
            if (!ret) {
                LOG(ERROR) << "Write field failed";
                return false;
            }
        }

        env->kvstore_->asyncMultiPut(spaceId, part.first, std::move(data),
                                    [&](kvstore::ResultCode code) {
                                        CHECK_EQ(code, kvstore::ResultCode::SUCCEEDED);
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

TEST(UpdateVertexTest, No_Filter_Yield_Test) {
    fs::TempDir rootPath("/tmp/UpdateVertexTest.XXXXXX");
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

    EXPECT_TRUE(mockVertexData(env, 6, spaceVidLen));

    LOG(INFO) << "Build UpdateVertexRequest...";
    cpp2::UpdateVertexRequest req;

    req.set_space_id(spaceId);
    auto partId = std::hash<std::string>()("Tim Duncan") % parts + 1;
    VertexID vertexId("Tim Duncan");
    req.set_part_id(partId);
    req.set_vertex_id(vertexId);

    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedVertexProp> updatedProps;
    // int: player.age = 45
    cpp2::UpdatedVertexProp prop1;
    prop1.set_tag_id(tagId);
    prop1.set_name("age");
    PrimaryExpression val1(45L);
    prop1.set_value(Expression::encode(&val1));
    updatedProps.emplace_back(prop1);
    // string: player.Tim Duncan.country= China
    cpp2::UpdatedVertexProp prop2;
    prop2.set_tag_id(tagId);
    prop2.set_name("country");
    std::string col4new("China");
    PrimaryExpression val2(col4new);
    prop2.set_value(Expression::encode(&val2));
    updatedProps.emplace_back(prop2);
    req.set_updated_props(std::move(updatedProps));


    LOG(INFO) << "Build yield...";
    // Return player props: name, age, country
    decltype(req.return_props) tmpProps;
    std::string alias("1");
    std::string propName1("name");
    SourcePropertyExpression sourcePropExp1(&alias, &propName1);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp1));

    std::string propName2("age");
    SourcePropertyExpression sourcePropExp2(&alias, &propName2);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp2));

    std::string propName3("country");
    SourcePropertyExpression sourcePropExp3(&alias, &propName3);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp3));

    req.set_return_props(std::move(tmpProps));
    req.set_insertable(false);

    LOG(INFO) << "Test UpdateVertexRequest...";
    auto* processor = UpdateVertexProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(0, resp.result.failed_parts.size());
    EXPECT_EQ(4, resp.props.column_names.size());
    EXPECT_EQ("_inserted", resp.props.column_names[0]);
    EXPECT_EQ("1:name", resp.props.column_names[1]);
    EXPECT_EQ("1:age", resp.props.column_names[2]);
    EXPECT_EQ("1:country", resp.props.column_names[3]);

    EXPECT_EQ(1, resp.props.rows.size());
    EXPECT_EQ(4, resp.props.rows[0].columns.size());

    EXPECT_EQ(false, resp.props.rows[0].columns[0])
    EXPECT_EQ("Tim Duncan", resp.props.rows[0]column_names[1]);
    EXPECT_EQ(45, resp.props.rows[0].column_names[2]);
    EXPECT_EQ("China", resp.props.rows[0].column_names[3]);

    // get player from kvstore directly
    auto prefix = NebulaKeyUtils::vertexPrefix(spaceVidLen, partId, vertexId, tagId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
    EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, ret);
    EXPECT_TRUE(iter && iter->valid());

    auto reader = RowReader::getTagPropReader(env->schemaMan_, spaceId, tagId, iter->val());
    auto val = reader->getValueByName("name");
    EXPECT_EQ("Tim Duncan", val.getStr());

    val = reader->getValueByName("age");
    EXPECT_EQ(45, val.getInt());

    val = reader->getValueByName("country");
    EXPECT_EQ("China", val.getStr());
}

TEST(UpdateVertexTest, Filter_Yield_Test) {
    fs::TempDir rootPath("/tmp/UpdateVertexTest.XXXXXX");
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

    EXPECT_TRUE(mockVertexData(env, 6, spaceVidLen));

    LOG(INFO) << "Build UpdateVertexRequest...";
    cpp2::UpdateVertexRequest req;

    req.set_space_id(spaceId);
    auto partId = std::hash<std::string>()("Tim Duncan") % parts + 1;
    VertexID vertexId("Tim Duncan");
    req.set_part_id(partId);
    req.set_vertex_id(vertexId);

    LOG(INFO) << "Build filter...";
    // left int:  1.startYear = 1997
    std::string alias("1");
    std::string propName1("startYear");

    auto* srcExp1 = new SourcePropertyExpression(&alias, &propName1);
    auto* priExp1 = new PrimaryExpression(1997L);
    auto* left = new RelationalExpression(srcExp1,
                                          RelationalExpression::Operator::EQ,
                                          priExp1);

    // right int: 1.endYear = 2017
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
    std::vector<cpp2::UpdatedVertexProp> updatedProps;
    // int: player.age = 45
    cpp2::UpdatedVertexProp prop1;
    prop1.set_tag_id(tagId);
    prop1.set_name("age");
    PrimaryExpression val1(45L);
    prop1.set_value(Expression::encode(&val1));
    updatedProps.emplace_back(prop1);
    // string: player.Tim Duncan.country= China
    cpp2::UpdatedVertexProp prop2;
    prop2.set_tag_id(tagId);
    prop2.set_name("country");
    std::string col4new("China");
    PrimaryExpression val2(col4new);
    prop2.set_value(Expression::encode(&val2));
    updatedProps.emplace_back(prop2);
    req.set_updated_props(std::move(updatedProps));


    LOG(INFO) << "Build yield...";
    // Return player props: name, age, country
    decltype(req.return_props) tmpProps;
    std::string alias("1");
    std::string propName1("name");
    SourcePropertyExpression sourcePropExp1(&alias, &propName1);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp1));

    std::string propName2("age");
    SourcePropertyExpression sourcePropExp2(&alias, &propName2);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp2));

    std::string propName3("country");
    SourcePropertyExpression sourcePropExp3(&alias, &propName3);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp3));

    req.set_return_props(std::move(tmpProps));
    req.set_insertable(false);

    LOG(INFO) << "Test UpdateVertexRequest...";
    auto* processor = UpdateVertexProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(0, resp.result.failed_parts.size());
    EXPECT_EQ(4, resp.props.column_names.size());
    EXPECT_EQ("_inserted", resp.props.column_names[0]);
    EXPECT_EQ("1:name", resp.props.column_names[1]);
    EXPECT_EQ("1:age", resp.props.column_names[2]);
    EXPECT_EQ("1:country", resp.props.column_names[3]);

    EXPECT_EQ(0, resp.props.rows.size());

    // get player from kvstore directly
    // Because no update, the value is old
    auto prefix = NebulaKeyUtils::vertexPrefix(spaceVidLen, partId, vertexId, tagId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
    EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, ret);
    EXPECT_TRUE(iter && iter->valid());

    auto reader = RowReader::getTagPropReader(env->schemaMan_, spaceId, tagId, iter->val());
    auto val = reader->getValueByName("name");
    EXPECT_EQ("Tim Duncan", val.getStr());

    val = reader->getValueByName("age");
    EXPECT_EQ(44, val.getInt());

    val = reader->getValueByName("country");
    EXPECT_EQ("America", val.getStr());
}


TEST(UpdateVertexTest, Insertable_Test) {
    fs::TempDir rootPath("/tmp/UpdateVertexTest.XXXXXX");
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

    EXPECT_TRUE(mockVertexData(env, 6, spaceVidLen));

    LOG(INFO) << "Build UpdateVertexRequest...";
    cpp2::UpdateVertexRequest req;

    req.set_space_id(spaceId);
    auto partId = std::hash<std::string>()("Brandon Ingram") % parts + 1;
    VertexID vertexId("Brandon Ingram");
    req.set_part_id(partId);
    req.set_vertex_id(vertexId);

    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedVertexProp> updatedProps;
    // Because not default value and no nullable, so provide values
    // string: player.name = Brandon Ingram
    cpp2::UpdatedVertexProp prop1;
    prop1.set_tag_id(tagId);
    prop1.set_name("name");
    std::string col1new("Brandon Ingram")
    PrimaryExpression val1(col1new);
    prop1.set_value(Expression::encode(&val1));
    updatedProps.emplace_back(prop1);

    // int: player.age = 23
    cpp2::UpdatedVertexProp prop2;
    prop2.set_tag_id(tagId);
    prop2.set_name("age");
    PrimaryExpression val2(23L);
    prop2.set_value(Expression::encode(&val2));
    updatedProps.emplace_back(prop2);

    // bool: player.playing = true
    cpp2::UpdatedVertexProp prop3;
    prop3.set_tag_id(tagId);
    prop3.set_name("playing");
    bool isPlaying = true;
    PrimaryExpression val3(isPlaying);
    prop3.set_value(Expression::encode(&val3));
    updatedProps.emplace_back(prop3);

    // int: player.career = 4
    cpp2::UpdatedVertexProp prop4;
    prop4.set_tag_id(tagId);
    prop4.set_name("career");
    PrimaryExpression val4(4L);
    prop4.set_value(Expression::encode(&val4));
    updatedProps.emplace_back(prop4);

    // int: player.startYear = 2016
    cpp2::UpdatedVertexProp prop5;
    prop5.set_tag_id(tagId);
    prop5.set_name("startYear");
    PrimaryExpression val5(2016L);
    prop5.set_value(Expression::encode(&val5));
    updatedProps.emplace_back(prop5);

    // int: player.endYear = 2020
    cpp2::UpdatedVertexProp prop6;
    prop6.set_tag_id(tagId);
    prop6.set_name("endYear");
    PrimaryExpression val6(2020L);
    prop5.set_value(Expression::encode(&val6));
    updatedProps.emplace_back(prop6);

    // int: player.games = 246
    cpp2::UpdatedVertexProp prop7;
    prop7.set_tag_id(tagId);
    prop7.set_name("games");
    PrimaryExpression val7(246L);
    prop7.set_value(Expression::encode(&val7));
    updatedProps.emplace_back(prop7);

    // double: player.avgScore = 24.3
    cpp2::UpdatedVertexProp prop8;
    prop8.set_tag_id(tagId);
    prop8.set_name("avgScore");
    double avgScore = 24.3;
    PrimaryExpression val8(avgScore);
    prop3.set_value(Expression::encode(&val8));
    updatedProps.emplace_back(prop8);

     // int: player.serveTeams = 2
    cpp2::UpdatedVertexProp prop9;
    prop9.set_tag_id(tagId);
    prop9.set_name("serveTeams");
    PrimaryExpression val9(2);
    prop9.set_value(Expression::encode(&val9));
    updatedProps.emplace_back(prop9);
    req.set_updated_props(std::move(updatedProps));


    LOG(INFO) << "Build yield...";
    // Return player props: name, age, country
    decltype(req.return_props) tmpProps;
    std::string alias("1");
    std::string propName1("name");
    SourcePropertyExpression sourcePropExp1(&alias, &propName1);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp1));

    std::string propName2("age");
    SourcePropertyExpression sourcePropExp2(&alias, &propName2);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp2));

    std::string propName3("country");
    SourcePropertyExpression sourcePropExp3(&alias, &propName3);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp3));

    req.set_return_props(std::move(tmpProps));
    req.set_insertable(true);

    LOG(INFO) << "Test UpdateVertexRequest...";
    auto* processor = UpdateVertexProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(0, resp.result.failed_parts.size());
    EXPECT_EQ(4, resp.props.column_names.size());
    EXPECT_EQ("_inserted", resp.props.column_names[0]);
    EXPECT_EQ("1:name", resp.props.column_names[1]);
    EXPECT_EQ("1:age", resp.props.column_names[2]);
    EXPECT_EQ("1:country", resp.props.column_names[3]);

    EXPECT_EQ(0, resp.props.rows.size());

    // get player from kvstore directly
    // Because no update, the value is old
    auto prefix = NebulaKeyUtils::vertexPrefix(spaceVidLen, partId, vertexId, tagId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
    EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, ret);
    EXPECT_TRUE(iter && iter->valid());

    auto reader = RowReader::getTagPropReader(env->schemaMan_, spaceId, tagId, iter->val());
    auto val = reader->getValueByName("name");
    EXPECT_EQ("Brandon Ingram", val.getStr());

    val = reader->getValueByName("age");
    EXPECT_EQ(23, val.getInt());

    val = reader->getValueByName("country");
    EXPECT_EQ("America", val.getStr());
}

TEST(UpdateVertexTest, Invalid_Update_Prop_Test) {
    fs::TempDir rootPath("/tmp/UpdateVertexTest.XXXXXX");
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

    EXPECT_TRUE(mockVertexData(env, 6, spaceVidLen));

    LOG(INFO) << "Build UpdateVertexRequest...";
    cpp2::UpdateVertexRequest req;

    req.set_space_id(spaceId);
    auto partId = std::hash<std::string>()("Tim Duncan") % parts + 1;
    VertexID vertexId("Tim Duncan");
    req.set_part_id(partId);
    req.set_vertex_id(vertexId);

    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedVertexProp> updatedProps;
    // int: player.age = 45
    cpp2::UpdatedVertexProp prop1;
    prop1.set_tag_id(tagId);
    prop1.set_name("age");
    PrimaryExpression val1(45L);
    prop1.set_value(Expression::encode(&val1));
    updatedProps.emplace_back(prop1);
    // int: player.birth = 1997 invalid
    cpp2::UpdatedVertexProp prop2;
    prop2.set_tag_id(tagId);
    prop2.set_name("birth");
    PrimaryExpression val2(1997L);
    prop2.set_value(Expression::encode(&val2));
    updatedProps.emplace_back(prop2);
    req.set_updated_props(std::move(updatedProps));


    LOG(INFO) << "Build yield...";
    // Return player props: name, age, country
    decltype(req.return_props) tmpProps;
    std::string alias("1");
    std::string propName1("name");
    SourcePropertyExpression sourcePropExp1(&alias, &propName1);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp1));

    std::string propName2("age");
    SourcePropertyExpression sourcePropExp2(&alias, &propName2);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp2));

    std::string propName3("country");
    SourcePropertyExpression sourcePropExp3(&alias, &propName3);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp3));

    req.set_return_props(std::move(tmpProps));
    req.set_insertable(false);

    LOG(INFO) << "Test UpdateVertexRequest...";
    auto* processor = UpdateVertexProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(1, resp.result.failed_parts.size());
    EXPECT_TRUE(nebula::storage::cpp2::ErrorCode::E_INVALID_UPDATER
                    == resp.result.failed_parts[0].code);

    // get player from kvstore directly
    // Because no update, the value is old
    auto prefix = NebulaKeyUtils::vertexPrefix(spaceVidLen, partId, vertexId, tagId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
    EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, ret);
    EXPECT_TRUE(iter && iter->valid());

    auto reader = RowReader::getTagPropReader(env->schemaMan_, spaceId, tagId, iter->val());
    auto val = reader->getValueByName("name");
    EXPECT_EQ("Tim Duncan", val.getStr());

    val = reader->getValueByName("age");
    EXPECT_EQ(44, val.getInt());

    val = reader->getValueByName("country");
    EXPECT_EQ("America", val.getStr());
}

TEST(UpdateVertexTest, Invalid_Filter_Test) {
    fs::TempDir rootPath("/tmp/UpdateVertexTest.XXXXXX");
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

    EXPECT_TRUE(mockVertexData(env, 6, spaceVidLen));

    LOG(INFO) << "Build UpdateVertexRequest...";
    cpp2::UpdateVertexRequest req;

    req.set_space_id(spaceId);
    auto partId = std::hash<std::string>()("Tim Duncan") % parts + 1;
    VertexID vertexId("Tim Duncan");
    req.set_part_id(partId);
    req.set_vertex_id(vertexId);

    LOG(INFO) << "Build condition...";
    // left int:  1.startYear = 1997
    std::string alias("1");
    std::string propName1("startYear");

    auto* srcExp1 = new SourcePropertyExpression(&alias, &propName1);
    auto* priExp1 = new PrimaryExpression(1997L);
    auto* left = new RelationalExpression(srcExp1,
                                          RelationalExpression::Operator::EQ,
                                          priExp1);

    // invalid prop
    // right int: 1.birth
    std::string propName2("birth");
    auto* srcExp2 = new SourcePropertyExpression(&alias, &propName2);
    auto* priExp2 = new PrimaryExpression(1990L);
    auto* right = new RelationalExpression(srcExp2,
                                           RelationalExpression::Operator::EQ,
                                           priExp2);
    // left AND right is ture
    auto logExp = std::make_unique<LogicalExpression>(left, LogicalExpression::AND, right);
    req.set_condition(Expression::encode(logExp.get()));

    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedVertexProp> updatedProps;
    // int: player.age = 45
    cpp2::UpdatedVertexProp prop1;
    prop1.set_tag_id(tagId);
    prop1.set_name("age");
    PrimaryExpression val1(45L);
    prop1.set_value(Expression::encode(&val1));
    updatedProps.emplace_back(prop1);
    // string: player.Tim Duncan.country= China
    cpp2::UpdatedVertexProp prop2;
    prop2.set_tag_id(tagId);
    prop2.set_name("country");
    std::string col4new("China");
    PrimaryExpression val2(col4new);
    prop2.set_value(Expression::encode(&val2));
    updatedProps.emplace_back(prop2);
    req.set_updated_props(std::move(updatedProps));


    LOG(INFO) << "Build yield...";
    // Return player props: name, age, country
    decltype(req.return_props) tmpProps;
    std::string alias("1");
    std::string propName1("name");
    SourcePropertyExpression sourcePropExp1(&alias, &propName1);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp1));

    std::string propName2("age");
    SourcePropertyExpression sourcePropExp2(&alias, &propName2);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp2));

    std::string propName3("country");
    SourcePropertyExpression sourcePropExp3(&alias, &propName3);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp3));

    req.set_return_props(std::move(tmpProps));
    req.set_insertable(false);

    LOG(INFO) << "Test UpdateVertexRequest...";
    auto* processor = UpdateVertexProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(1, resp.result.failed_parts.size());
    EXPECT_TRUE(nebula::storage::cpp2::ErrorCode::E_INVALID_FILTER
                    == resp.result.failed_parts[0].code);

    // get player from kvstore directly
    // Because no update, the value is old
    auto prefix = NebulaKeyUtils::vertexPrefix(spaceVidLen, partId, vertexId, tagId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
    EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, ret);
    EXPECT_TRUE(iter && iter->valid());

    auto reader = RowReader::getTagPropReader(env->schemaMan_, spaceId, tagId, iter->val());
    auto val = reader->getValueByName("name");
    EXPECT_EQ("Tim Duncan", val.getStr());

    val = reader->getValueByName("age");
    EXPECT_EQ(44, val.getInt());

    val = reader->getValueByName("country");
    EXPECT_EQ("America", val.getStr());
}

TEST(UpdateVertexTest, CorruptDataTest) {
    fs::TempDir rootPath("/tmp/UpdateVertexTest.XXXXXX");
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
    VertexID vertexId("Lonzo Ball");
    auto key = NebulaKeyUtils::vertexKey(spaceVidLen, partId,  vertexId, 1, 0L);
    std::vector<kvstore::KV> data;
    data.emplace_back(std::make_pair(key, ""));
    folly::Baton<> baton;
    env->kvstore_->asyncMultiPut(spaceId, partId, std::move(data),
        [&](kvstore::ResultCode code) {
            CHECK_EQ(code, kvstore::ResultCode::SUCCEEDED);
            baton.post();
        });
    baton.wait();

    LOG(INFO) << "Build UpdateVertexRequest...";
    cpp2::UpdateVertexRequest req;
    req.set_space_id(spaceId);
    req.set_part_id(partId);
    req.set_vertex_id(vertexId);

    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedVertexProp> updatedProps;
    // int: player.age = 23
    cpp2::UpdatedVertexProp prop1;
    prop1.set_tag_id(tagId);
    prop1.set_name("age");
    PrimaryExpression val1(23L);
    prop1.set_value(Expression::encode(&val1));
    updatedProps.emplace_back(prop1);
    req.set_updated_props(std::move(updatedProps));

    LOG(INFO) << "Build yield...";
    // Return player props: name, age, country
    decltype(req.return_props) tmpProps;
    std::string alias("1");
    std::string propName1("name");
    SourcePropertyExpression sourcePropExp1(&alias, &propName1);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp1));

    req.set_return_props(std::move(tmpProps));
    req.set_insertable(false);

    LOG(INFO) << "Test UpdateVertexRequest...";
    auto* processor = UpdateVertexProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(1, resp.result.failed_parts.size());
    EXPECT_TRUE(nebula::storage::cpp2::ErrorCode::E_TAG_NOT_FOUND
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

