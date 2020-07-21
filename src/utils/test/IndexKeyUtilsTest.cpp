/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common//base/Base.h"
#include "utils/IndexKeyUtils.h"
#include <gtest/gtest.h>

namespace nebula {

VertexID getStringId(int64_t vId) {
    std::string id;
    id.append(reinterpret_cast<const char*>(&vId), sizeof(int64_t));
    return id;
}

std::vector<IndexValue> getIndexValues() {
    std::vector<IndexValue> values;
    values.emplace_back(std::make_tuple(Value("str"), Value::Type::STRING, 20));
    values.emplace_back(std::make_tuple(Value(true), Value::Type::BOOL,
        IndexKeyUtils::valueLength(Value::Type::BOOL)));
    values.emplace_back(std::make_tuple(Value(12L), Value::Type::INT,
        IndexKeyUtils::valueLength(Value::Type::INT)));
    values.emplace_back(std::make_tuple(Value(12.12f), Value::Type::FLOAT,
        IndexKeyUtils::valueLength(Value::Type::FLOAT)));
    Date d = {2020, 1, 20};
    values.emplace_back(std::make_tuple(Value(d), Value::Type::DATE,
        IndexKeyUtils::valueLength(Value::Type::DATE)));

    DateTime dt = {2020, 4, 11, 12, 30, 22, 1111, 2222};
    values.emplace_back(std::make_tuple(Value(dt), Value::Type::DATETIME,
        IndexKeyUtils::valueLength(Value::Type::DATETIME)));
    return values;
}

bool evalInt64(int64_t val) {
    IndexValue v = std::make_tuple(Value(val), Value::Type::INT, 0);
    auto str = IndexKeyUtils::encodeValue(v);
    auto res = IndexKeyUtils::decodeValue(str, Value::Type::INT);

    EXPECT_EQ(Value::Type::INT, res.type());
    EXPECT_EQ(std::get<0>(v), res);
    return val == res.getInt();
}

bool evalDouble(double val) {
    IndexValue v = std::make_tuple(Value(val), Value::Type::FLOAT, 0);
    auto str = IndexKeyUtils::encodeValue(v);
    auto res = IndexKeyUtils::decodeValue(str, Value::Type::FLOAT);
    EXPECT_EQ(Value::Type::FLOAT, res.type());
    EXPECT_EQ(std::get<0>(v), res);
    return val == res.getFloat();
}

bool evalBool(bool val) {
    IndexValue v = std::make_tuple(Value(val), Value::Type::BOOL, 0);
    auto str = IndexKeyUtils::encodeValue(v);
    auto res = IndexKeyUtils::decodeValue(str, Value::Type::BOOL);
    EXPECT_EQ(Value::Type::BOOL, res.type());
    EXPECT_EQ(std::get<0>(v), res);
    return val == res.getBool();
}

bool evalString(std::string val) {
    IndexValue v = std::make_tuple(Value(val), Value::Type::STRING, 4);
    auto str = IndexKeyUtils::encodeValue(v);
    auto res = IndexKeyUtils::decodeValue(str, Value::Type::STRING);
    EXPECT_EQ(Value::Type::STRING, res.type());
    EXPECT_EQ(std::get<0>(v), res);
    return val == res.getStr();
}

bool evalDate(nebula::Date val) {
    IndexValue v = std::make_tuple(Value(val), Value::Type::DATE, 0);
    auto str = IndexKeyUtils::encodeValue(v);
    auto res = IndexKeyUtils::decodeValue(str, Value::Type::DATE);
    EXPECT_EQ(Value::Type::DATE, res.type());
    EXPECT_EQ(std::get<0>(v), res);
    return val == res.getDate();
}

bool evalDateTime(nebula::DateTime val) {
    IndexValue v = std::make_tuple(Value(val), Value::Type::DATETIME, 0);
    auto str = IndexKeyUtils::encodeValue(v);
    auto res = IndexKeyUtils::decodeValue(str, Value::Type::DATETIME);
    EXPECT_EQ(Value::Type::DATETIME, res.type());
    EXPECT_EQ(std::get<0>(v), res);
    return val == res.getDateTime();
}

TEST(IndexKeyUtilsTest, encodeValue) {
    EXPECT_TRUE(evalInt64(1));
    EXPECT_TRUE(evalInt64(0));
    EXPECT_TRUE(evalInt64(std::numeric_limits<int64_t>::max()));
    EXPECT_TRUE(evalInt64(std::numeric_limits<int64_t>::min()));
    EXPECT_TRUE(evalDouble(1.1));
    EXPECT_TRUE(evalDouble(0.0));
    EXPECT_TRUE(evalDouble(std::numeric_limits<double>::max()));
    EXPECT_TRUE(evalDouble(std::numeric_limits<double>::min()));
    EXPECT_TRUE(evalDouble(-std::numeric_limits<double>::max()));
    EXPECT_TRUE(evalDouble(-(0.000000001 - std::numeric_limits<double>::min())));
    EXPECT_TRUE(evalBool(true));
    EXPECT_TRUE(evalBool(false));
    EXPECT_TRUE(evalString("test"));
    nebula::Date d;
    d.year = 2020;
    d.month = 4;
    d.day = 11;
    EXPECT_TRUE(evalDate(d));

    nebula::DateTime dt;
    dt.year = 2020;
    dt.month = 4;
    dt.day = 11;
    dt.hour = 12;
    dt.minute = 30;
    dt.sec = 22;
    dt.microsec = 1111;
    dt.timezone = 2222;
    EXPECT_TRUE(evalDateTime(dt));
}

TEST(IndexKeyUtilsTest, encodeDouble) {
    EXPECT_TRUE(evalDouble(100.5));
    EXPECT_TRUE(evalDouble(200.5));
    EXPECT_TRUE(evalDouble(300.5));
    EXPECT_TRUE(evalDouble(400.5));
    EXPECT_TRUE(evalDouble(500.5));
    EXPECT_TRUE(evalDouble(600.5));
}

TEST(IndexKeyUtilsTest, vertexIndexKeyV1) {
    auto values = getIndexValues();
    auto key = IndexKeyUtils::vertexIndexKey(8, 1, 1, getStringId(1), values);
    ASSERT_EQ(1, IndexKeyUtils::getIndexId(key));
    ASSERT_EQ(getStringId(1), IndexKeyUtils::getIndexVertexID(8, key));
    ASSERT_EQ(true, IndexKeyUtils::isIndexKey(key));
}

TEST(IndexKeyUtilsTest, vertexIndexKeyV2) {
    auto values = getIndexValues();
    auto key = IndexKeyUtils::vertexIndexKey(100, 1, 1, "vertex_1_1_1_1", values);
    ASSERT_EQ(1, IndexKeyUtils::getIndexId(key));

    VertexID vid = "vertex_1_1_1_1";
    vid.append(100 - vid.size(), '\0');
    ASSERT_EQ(vid, IndexKeyUtils::getIndexVertexID(100, key));
    ASSERT_EQ(true, IndexKeyUtils::isIndexKey(key));
}

TEST(IndexKeyUtilsTest, edgeIndexKeyV1) {
    auto values = getIndexValues();
    auto key = IndexKeyUtils::edgeIndexKey(8, 1, 1, getStringId(1), 1, getStringId(2), values);
    ASSERT_EQ(1, IndexKeyUtils::getIndexId(key));
    ASSERT_EQ(getStringId(1), IndexKeyUtils::getIndexSrcId(8, key));
    ASSERT_EQ(1, IndexKeyUtils::getIndexRank(8, key));
    ASSERT_EQ(getStringId(2), IndexKeyUtils::getIndexDstId(8, key));
    ASSERT_EQ(true, IndexKeyUtils::isIndexKey(key));
}

TEST(IndexKeyUtilsTest, edgeIndexKeyV2) {
    VertexID vid = "vertex_1_1_1_1";
    auto values = getIndexValues();
    auto key = IndexKeyUtils::edgeIndexKey(100, 1, 1, vid, 1, vid, values);
    ASSERT_EQ(1, IndexKeyUtils::getIndexId(key));
    vid.append(100 - vid.size(), '\0');
    ASSERT_EQ(vid, IndexKeyUtils::getIndexSrcId(100, key));
    ASSERT_EQ(1, IndexKeyUtils::getIndexRank(100, key));
    ASSERT_EQ(vid, IndexKeyUtils::getIndexDstId(100, key));
    ASSERT_EQ(true, IndexKeyUtils::isIndexKey(key));
}

TEST(IndexKeyUtilsTest, nullableValue) {
    {
        std::string raw;
        std::vector<IndexValue> values;
        for (int64_t j = 1; j <= 6; j++) {
            auto type = Value::Type(1UL << j);
            if (type == Value::Type::STRING) {
                values.emplace_back(std::make_tuple(Value(NullType::__NULL__), type, 0));
            } else {
                values.emplace_back(std::make_tuple(Value(NullType::__NULL__), type,
                                                    IndexKeyUtils::valueLength(type)));
            }
        }
        IndexKeyUtils::encodeValues(std::move(values), raw, true);
        u_short s = 0x03FF; /* the binary is '00000011 11111111'*/
        std::string expected;
        expected.append(reinterpret_cast<const char*>(&s), sizeof(u_short));
        auto result = raw.substr(raw.size() - sizeof(u_short), sizeof(u_short));
        ASSERT_EQ(expected, result);
    }
    {
        std::string raw;
        std::vector<IndexValue> values;
        auto type = Value::Type::BOOL;
        values.emplace_back(std::make_tuple(Value(true), type,
            IndexKeyUtils::valueLength(type)));
        values.emplace_back(std::make_tuple(Value(NullType::__NULL__), type,
            IndexKeyUtils::valueLength(type)));
        IndexKeyUtils::encodeValues(std::move(values), raw, true);
        u_short s = 0xBFFF; /* the binary is '10111111 11111111'*/
        std::string expected;
        expected.append(reinterpret_cast<const char*>(&s), sizeof(u_short));
        auto result = raw.substr(raw.size() - sizeof(u_short), sizeof(u_short));
        ASSERT_EQ(expected, result);
    }
    {
        std::string raw;
        std::vector<IndexValue> values;
        values.emplace_back(std::make_tuple(Value(true), Value::Type::BOOL,
            IndexKeyUtils::valueLength(Value::Type::BOOL)));
        values.emplace_back(std::make_tuple(Value(false), Value::Type::BOOL,
            IndexKeyUtils::valueLength(Value::Type::BOOL)));
        IndexKeyUtils::encodeValues(std::move(values), raw);
        ASSERT_EQ(2, raw.size());
    }
    {
        std::string raw;
        std::vector<IndexValue> values;
        for (int64_t i = 0; i <2; i++) {
            for (int64_t j = 1; j <= 6; j++) {
                auto type = Value::Type(1UL << j);
                values.emplace_back(std::make_tuple(Value(NullType::__NULL__), type,
                                    IndexKeyUtils::valueLength(type)));
            }
        }

        IndexKeyUtils::encodeValues(std::move(values), raw, true);
        u_short s = 0x000f; /* the binary is '00000000 00001111'*/
        std::string expected;
        expected.append(reinterpret_cast<const char*>(&s), sizeof(u_short));
        auto result = raw.substr(raw.size() - sizeof(u_short),
                                 sizeof(u_short));
        ASSERT_EQ(expected, result);
    }
    {
        std::string raw;
        std::vector<IndexValue> values;
        std::unordered_map<Value::Type, IndexValue> mockValues;
        {
            mockValues[Value::Type::BOOL] = std::make_tuple(Value(true), Value::Type::BOOL,
                IndexKeyUtils::valueLength(Value::Type::BOOL));
            mockValues[Value::Type::INT] = std::make_tuple(Value(4L), Value::Type::INT,
                IndexKeyUtils::valueLength(Value::Type::INT));
            mockValues[Value::Type::FLOAT] = std::make_tuple(Value(1.5f), Value::Type::FLOAT,
                IndexKeyUtils::valueLength(Value::Type::FLOAT));
            mockValues[Value::Type::STRING] = std::make_tuple(Value("str"), Value::Type::STRING, 3);
            Date d = {2020, 1, 20};
            mockValues[Value::Type::DATE] = std::make_tuple(Value(d), Value::Type::DATE,
                IndexKeyUtils::valueLength(Value::Type::DATE));
            DateTime dt = {2020, 4, 11, 12, 30, 22, 1111, 2222};
            mockValues[Value::Type::DATETIME] = std::make_tuple(Value(dt), Value::Type::DATETIME,
                IndexKeyUtils::valueLength(Value::Type::DATETIME));
        }
        std::vector<Value::Type> colsType;
        for (int64_t i = 0; i <2; i++) {
            for (int64_t j = 1; j <= 6; j++) {
                auto type = Value::Type(1UL << j);
                if (j%2 == 1) {
                    if (type == Value::Type::STRING) {
                        values.emplace_back(Value(NullType::__NULL__), type, 3);
                    } else {
                        values.emplace_back(Value(NullType::__NULL__), type,
                                            IndexKeyUtils::valueLength(type));
                    }
                } else {
                    values.emplace_back(mockValues[type]);
                }
                colsType.emplace_back(type);
            }
        }

        IndexKeyUtils::encodeValues(std::move(values), raw, true);
        u_short s = 0x555F; /* the binary is '01010101 01011111'*/
        std::string expected;
        expected.append(reinterpret_cast<const char*>(&s), sizeof(u_short));
        auto result = raw.substr(raw.size() - sizeof(u_short),
                                 sizeof(u_short));
        ASSERT_EQ(expected, result);
    }
    {
        std::string raw;
        std::vector<IndexValue> values;
        for (int64_t i = 0; i <9; i++) {
            values.emplace_back(std::make_tuple(Value(NullType::__NULL__), Value::Type::BOOL,
                                                IndexKeyUtils::valueLength(Value::Type::BOOL)));
        }
        IndexKeyUtils::encodeValues(std::move(values), raw, true);
        u_short s = 0x007F; /* the binary is '11111111 10000000'*/
        std::string expected;
        expected.append(reinterpret_cast<const char*>(&s), sizeof(u_short));
        auto result = raw.substr(raw.size() - sizeof(u_short), sizeof(u_short));
        ASSERT_EQ(expected, result);
    }
}

void verifyDecodeIndexKey(bool isEdge,
                          bool nullable,
                          size_t vIdLen,
                          size_t fixedStrLen,
                          const std::vector<std::pair<VertexID, std::vector<IndexValue>>>& data,
                          const std::vector<std::string>& indexKeys,
                          std::vector<std::pair<std::string, Value::Type>> cols) {
    for (size_t j = 0; j < data.size(); j++) {
        ASSERT_EQ(std::get<0>(data[j].second[0]),
                  IndexKeyUtils::getValueFromIndexKey(
                      vIdLen, indexKeys[j], "col_bool", cols, fixedStrLen, isEdge, nullable));
        ASSERT_EQ(std::get<0>(data[j].second[1]),
                  IndexKeyUtils::getValueFromIndexKey(
                      vIdLen, indexKeys[j], "col_int", cols, fixedStrLen, isEdge, nullable));
        ASSERT_EQ(std::get<0>(data[j].second[2]),
                  IndexKeyUtils::getValueFromIndexKey(
                      vIdLen, indexKeys[j], "col_float", cols, fixedStrLen, isEdge, nullable));
        ASSERT_EQ(std::get<0>(data[j].second[3]),
                  IndexKeyUtils::getValueFromIndexKey(
                      vIdLen, indexKeys[j], "col_string", cols, fixedStrLen, isEdge, nullable));
        ASSERT_EQ(std::get<0>(data[j].second[4]),
                  IndexKeyUtils::getValueFromIndexKey(
                      vIdLen, indexKeys[j], "col_date", cols, fixedStrLen, isEdge, nullable));
        ASSERT_EQ(std::get<0>(data[j].second[5]),
                  IndexKeyUtils::getValueFromIndexKey(
                      vIdLen, indexKeys[j], "col_datetime", cols, fixedStrLen, isEdge, nullable));
    }
}


std::vector<std::pair<VertexID, std::vector<IndexValue>>> getIndexValuesWithNull() {
    Date d = {2020, 1, 20};
    DateTime dt = {2020, 4, 11, 12, 30, 22, 1111, 2222};
    std::vector<std::pair<VertexID, std::vector<IndexValue>>> datas = {
        {"1", {std::make_tuple(Value(false), Value::Type::BOOL,
                               IndexKeyUtils::valueLength(Value::Type::BOOL)),
                  std::make_tuple(Value(1L), Value::Type::INT,
                                  IndexKeyUtils::valueLength(Value::Type::INT)),
                  std::make_tuple(Value(1.1f), Value::Type::FLOAT,
                                  IndexKeyUtils::valueLength(Value::Type::FLOAT)),
                  std::make_tuple(Value("row1"), Value::Type::STRING, 4),
                  std::make_tuple(Value(d), Value::Type::DATE,
                                  IndexKeyUtils::valueLength(Value::Type::DATE)),
                  std::make_tuple(Value(dt), Value::Type::DATETIME,
                                  IndexKeyUtils::valueLength(Value::Type::DATETIME))}},
        {"2", {std::make_tuple(Value(true), Value::Type::BOOL,
                               IndexKeyUtils::valueLength(Value::Type::BOOL)),
                  std::make_tuple(Value(2L), Value::Type::INT,
                                  IndexKeyUtils::valueLength(Value::Type::INT)),
                  std::make_tuple(Value(2.1f), Value::Type::FLOAT,
                                  IndexKeyUtils::valueLength(Value::Type::FLOAT)),
                  std::make_tuple(Value("row2"), Value::Type::STRING, 4),
                  std::make_tuple(Value(d), Value::Type::DATE,
                                  IndexKeyUtils::valueLength(Value::Type::DATE)),
                  std::make_tuple(Value(dt), Value::Type::DATETIME,
                                  IndexKeyUtils::valueLength(Value::Type::DATETIME))}},
        {"3", {std::make_tuple(Value(NullType::__NULL__), Value::Type::BOOL,
                               IndexKeyUtils::valueLength(Value::Type::BOOL)),
                  std::make_tuple(Value(3L), Value::Type::INT,
                                  IndexKeyUtils::valueLength(Value::Type::INT)),
                  std::make_tuple(Value(3.1f), Value::Type::FLOAT,
                                  IndexKeyUtils::valueLength(Value::Type::FLOAT)),
                  std::make_tuple(Value("row3"), Value::Type::STRING, 4),
                  std::make_tuple(Value(d), Value::Type::DATE,
                                  IndexKeyUtils::valueLength(Value::Type::DATE)),
                  std::make_tuple(Value(dt), Value::Type::DATETIME,
                                  IndexKeyUtils::valueLength(Value::Type::DATETIME))}},
        {"4", {std::make_tuple(Value(true), Value::Type::BOOL,
                               IndexKeyUtils::valueLength(Value::Type::BOOL)),
                  std::make_tuple(Value(NullType::__NULL__), Value::Type::INT,
                                  IndexKeyUtils::valueLength(Value::Type::INT)),
                  std::make_tuple(Value(4.1f), Value::Type::FLOAT,
                                  IndexKeyUtils::valueLength(Value::Type::FLOAT)),
                  std::make_tuple(Value("row4"), Value::Type::STRING, 4),
                  std::make_tuple(Value(d), Value::Type::DATE,
                                  IndexKeyUtils::valueLength(Value::Type::DATE)),
                  std::make_tuple(Value(dt), Value::Type::DATETIME,
                                  IndexKeyUtils::valueLength(Value::Type::DATETIME))}},
        {"5", {std::make_tuple(Value(false), Value::Type::BOOL,
                               IndexKeyUtils::valueLength(Value::Type::BOOL)),
                  std::make_tuple(Value(5L), Value::Type::INT,
                                  IndexKeyUtils::valueLength(Value::Type::INT)),
                  std::make_tuple(Value(NullType::__NULL__), Value::Type::FLOAT,
                                  IndexKeyUtils::valueLength(Value::Type::FLOAT)),
                  std::make_tuple(Value("row5"), Value::Type::STRING, 4),
                  std::make_tuple(Value(d), Value::Type::DATE,
                                  IndexKeyUtils::valueLength(Value::Type::DATE)),
                  std::make_tuple(Value(dt), Value::Type::DATETIME,
                                  IndexKeyUtils::valueLength(Value::Type::DATETIME))}},
        {"6", {std::make_tuple(Value(true), Value::Type::BOOL,
                               IndexKeyUtils::valueLength(Value::Type::BOOL)),
                  std::make_tuple(Value(6L), Value::Type::INT,
                                  IndexKeyUtils::valueLength(Value::Type::INT)),
                  std::make_tuple(Value(6.1f), Value::Type::FLOAT,
                                  IndexKeyUtils::valueLength(Value::Type::FLOAT)),
                  std::make_tuple(Value(NullType::__NULL__), Value::Type::STRING, 4),
                  std::make_tuple(Value(d), Value::Type::DATE,
                                  IndexKeyUtils::valueLength(Value::Type::DATE)),
                  std::make_tuple(Value(dt), Value::Type::DATETIME,
                                  IndexKeyUtils::valueLength(Value::Type::DATETIME))}},
        {"7", {std::make_tuple(Value(false), Value::Type::BOOL,
                               IndexKeyUtils::valueLength(Value::Type::BOOL)),
                  std::make_tuple(Value(7L), Value::Type::INT,
                                  IndexKeyUtils::valueLength(Value::Type::INT)),
                  std::make_tuple(Value(7.1f), Value::Type::FLOAT,
                                  IndexKeyUtils::valueLength(Value::Type::FLOAT)),
                  std::make_tuple(Value("row7"), Value::Type::STRING, 4),
                  std::make_tuple(Value(NullType::__NULL__), Value::Type::DATE,
                                  IndexKeyUtils::valueLength(Value::Type::DATE)),
                  std::make_tuple(Value(dt), Value::Type::DATETIME,
                                  IndexKeyUtils::valueLength(Value::Type::DATETIME))}},
        {"8", {std::make_tuple(Value(true), Value::Type::BOOL,
                               IndexKeyUtils::valueLength(Value::Type::BOOL)),
                  std::make_tuple(Value(8L), Value::Type::INT,
                                  IndexKeyUtils::valueLength(Value::Type::INT)),
                  std::make_tuple(Value(8.1f), Value::Type::FLOAT,
                                  IndexKeyUtils::valueLength(Value::Type::FLOAT)),
                  std::make_tuple(Value("row8"), Value::Type::STRING, 4),
                  std::make_tuple(Value(d), Value::Type::DATE,
                                  IndexKeyUtils::valueLength(Value::Type::DATE)),
                  std::make_tuple(Value(NullType::__NULL__), Value::Type::DATETIME,
                                  IndexKeyUtils::valueLength(Value::Type::DATETIME))}},
        {"9", {std::make_tuple(Value(NullType::__NULL__), Value::Type::BOOL,
                               IndexKeyUtils::valueLength(Value::Type::BOOL)),
                  std::make_tuple(Value(NullType::__NULL__), Value::Type::INT,
                                  IndexKeyUtils::valueLength(Value::Type::INT)),
                  std::make_tuple(Value(NullType::__NULL__), Value::Type::FLOAT,
                                  IndexKeyUtils::valueLength(Value::Type::FLOAT)),
                  std::make_tuple(Value(NullType::__NULL__), Value::Type::STRING, 4),
                  std::make_tuple(Value(NullType::__NULL__), Value::Type::DATE,
                                  IndexKeyUtils::valueLength(Value::Type::DATE)),
                  std::make_tuple(Value(NullType::__NULL__), Value::Type::DATETIME,
                                  IndexKeyUtils::valueLength(Value::Type::DATETIME))}},
    };
    return datas;
}

std::vector<std::pair<VertexID, std::vector<IndexValue>>> getIndexValuesNonNullable() {
    Date d = {2020, 1, 20};
    DateTime dt = {2020, 4, 11, 12, 30, 22, 1111, 2222};
    std::vector<std::pair<VertexID, std::vector<IndexValue>>> data = {
        {"1", {std::make_tuple(Value(false), Value::Type::BOOL,
                               IndexKeyUtils::valueLength(Value::Type::BOOL)),
                  std::make_tuple(Value(1L), Value::Type::INT,
                                  IndexKeyUtils::valueLength(Value::Type::INT)),
                  std::make_tuple(Value(1.1f), Value::Type::FLOAT,
                                  IndexKeyUtils::valueLength(Value::Type::FLOAT)),
                  std::make_tuple(Value("row1"), Value::Type::STRING, 4),
                  std::make_tuple(Value(d), Value::Type::DATE,
                                  IndexKeyUtils::valueLength(Value::Type::DATE)),
                  std::make_tuple(Value(dt), Value::Type::DATETIME,
                                  IndexKeyUtils::valueLength(Value::Type::DATETIME))}},
        {"2", {std::make_tuple(Value(true), Value::Type::BOOL,
                               IndexKeyUtils::valueLength(Value::Type::BOOL)),
                  std::make_tuple(Value(2L), Value::Type::INT,
                                  IndexKeyUtils::valueLength(Value::Type::INT)),
                  std::make_tuple(Value(2.1f), Value::Type::FLOAT,
                                  IndexKeyUtils::valueLength(Value::Type::FLOAT)),
                  std::make_tuple(Value("row2"), Value::Type::STRING, 4),
                  std::make_tuple(Value(d), Value::Type::DATE,
                                  IndexKeyUtils::valueLength(Value::Type::DATE)),
                  std::make_tuple(Value(dt), Value::Type::DATETIME,
                                  IndexKeyUtils::valueLength(Value::Type::DATETIME))}}
    };
    return data;
}
TEST(IndexKeyUtilsTest, getValueFromIndexKeyTest) {
    size_t vIdLen = 8;
    PartitionID partId = 1;
    IndexID indexId = 1;

    std::vector<std::pair<std::string, Value::Type>> cols = {
        {"col_bool", Value::Type::BOOL},
        {"col_int", Value::Type::INT},
        {"col_float", Value::Type::FLOAT},
        {"col_string", Value::Type::STRING},
        {"col_date", Value::Type::DATE},
        {"col_datetime", Value::Type::DATETIME}
    };

    // vertices test with nullable
    {
        auto vertices = getIndexValuesWithNull();
        std::vector<std::string> indexKeys;

        for (const auto& row : vertices) {
            indexKeys.emplace_back(IndexKeyUtils::vertexIndexKey(
                vIdLen, partId, indexId, row.first, row.second, true));
        }
        verifyDecodeIndexKey(false, true, vIdLen, 4, vertices, indexKeys, cols);
    }

    // vertices test without nullable column
    {
        auto vertices = getIndexValuesNonNullable();

        std::vector<std::string> indexKeys;
        for (const auto& row : vertices) {
            indexKeys.emplace_back(IndexKeyUtils::vertexIndexKey(
                vIdLen, partId, indexId, row.first, row.second, false));
        }

        verifyDecodeIndexKey(false, false, vIdLen, 4, vertices, indexKeys, cols);
    }

    // edges test with nullable
    {
        auto edges = getIndexValuesWithNull();
        std::vector<std::string> indexKeys;

        for (const auto& row : edges) {
            indexKeys.emplace_back(IndexKeyUtils::edgeIndexKey(
                vIdLen, partId, indexId, row.first, 0, row.first, row.second, true));
        }

        verifyDecodeIndexKey(true, true, vIdLen, 4, edges, indexKeys, cols);
    }

    // edges test without nullable column
    {
        auto edges = getIndexValuesNonNullable();
        std::vector<std::string> indexKeys;

        for (const auto& row : edges) {
            indexKeys.emplace_back(IndexKeyUtils::edgeIndexKey(
                vIdLen, partId, indexId, row.first, 0, row.first, row.second, true));
        }

        verifyDecodeIndexKey(true, false, vIdLen, 4, edges, indexKeys, cols);
    }
}
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
