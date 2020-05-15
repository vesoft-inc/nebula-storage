/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_INDEXKEYUTILS_H_
#define COMMON_INDEXKEYUTILS_H_

#include "base/Base.h"
#include "base/StatusOr.h"
#include "interface/gen-cpp2/meta_types.h"
#include "common/Types.h"

namespace nebula {

using PropertyType = nebula::meta::cpp2::PropertyType;

/**
 *  NullCols vector<std::pair<original_col_type, nullable>>
 */
using NullCols = std::vector<std::pair<Value::Type, bool>>;

/**
 * This class supply some utils for index in kvstore.
 * */
class IndexKeyUtils final {
public:
    ~IndexKeyUtils() = default;

    static Value::Type toValueType(PropertyType type) {
        switch (type) {
            case PropertyType::BOOL :
                return Value::Type::BOOL;
            case PropertyType::INT64 :
            case PropertyType::INT32 :
            case PropertyType::INT16 :
            case PropertyType::INT8 :
            case PropertyType::TIMESTAMP :
                return Value::Type::INT;
            case PropertyType::VID :
                return Value::Type::VERTEX;
            case PropertyType::FLOAT :
            case PropertyType::DOUBLE :
                return Value::Type::FLOAT;
            case PropertyType::STRING :
            case PropertyType::FIXED_STRING :
                return Value::Type::STRING;
            case PropertyType::DATE :
                return Value::Type::DATE;
            case PropertyType::DATETIME :
                return Value::Type::DATETIME;
            case PropertyType::UNKNOWN :
                return Value::Type::__EMPTY__;
        }
        return Value::Type::__EMPTY__;
    }

    static std::string encodeNullValue(Value::Type type) {
        size_t len = 0;
        switch (type) {
            case Value::Type::INT : {
                len = sizeof(int64_t);
                break;
            }
            case Value::Type::FLOAT : {
                len = sizeof(double);
                break;
            }
            case Value::Type::BOOL: {
                len = sizeof(bool);
                break;
            }
            case Value::Type::STRING : {
                len = 1;
                break;
            }
            case Value::Type::DATE : {
                len = sizeof(int8_t) * 2 + sizeof(int16_t);
                break;
            }
            case Value::Type::DATETIME : {
                len = sizeof(int32_t) * 2 + sizeof(int16_t) + sizeof(int8_t) * 5;
                break;
            }
            default :
                LOG(ERROR) << "Unsupported default value type";
        }
        std::string raw;
        raw.reserve(len);
        raw.append(len, '\0');
        return raw;
    }

    static std::string encodeValue(const Value& v) {
        switch (v.type()) {
            case Value::Type::INT :
                return encodeInt64(v.getInt());
            case Value::Type::FLOAT :
                return encodeDouble(v.getFloat());
            case Value::Type::BOOL: {
                auto val = v.getBool();
                std::string raw;
                raw.reserve(sizeof(bool));
                raw.append(reinterpret_cast<const char*>(&val), sizeof(bool));
                return raw;
            }
            case Value::Type::STRING :
                return v.getStr();
            case Value::Type::DATE : {
                std::string buf;
                buf.reserve(sizeof(int8_t) * 2 + sizeof(int16_t));
                buf.append(reinterpret_cast<const char*>(&v.getDate().year), sizeof(int16_t))
                   .append(reinterpret_cast<const char*>(&v.getDate().month), sizeof(int8_t))
                   .append(reinterpret_cast<const char*>(&v.getDate().day), sizeof(int8_t));
                return buf;
            }
            case Value::Type::DATETIME : {
                std::string buf;
                buf.reserve(sizeof(int32_t) * 2 + sizeof(int16_t) + sizeof(int8_t) * 5);
                auto dt = v.getDateTime();
                buf.append(reinterpret_cast<const char*>(&dt.year), sizeof(int16_t))
                   .append(reinterpret_cast<const char*>(&dt.month), sizeof(int8_t))
                   .append(reinterpret_cast<const char*>(&dt.day), sizeof(int8_t))
                   .append(reinterpret_cast<const char*>(&dt.hour), sizeof(int8_t))
                   .append(reinterpret_cast<const char*>(&dt.minute), sizeof(int8_t))
                   .append(reinterpret_cast<const char*>(&dt.sec), sizeof(int8_t))
                   .append(reinterpret_cast<const char*>(&dt.microsec), sizeof(int32_t))
                   .append(reinterpret_cast<const char*>(&dt.timezone), sizeof(int32_t));
                return buf;
            }
            default :
                LOG(ERROR) << "Unsupported default value type";
        }
        return "";
    }

    /**
     * Default, positive number first bit is 0, negative number is 1 .
     * To keep the string in order, the first bit must to be inverse.
     * for example as below :
     *    9223372036854775807     -> "\377\377\377\377\377\377\377\377"
     *    1                       -> "\200\000\000\000\000\000\000\001"
     *    0                       -> "\200\000\000\000\000\000\000\000"
     *    -1                      -> "\177\377\377\377\377\377\377\377"
     *    -9223372036854775808    -> "\000\000\000\000\000\000\000\000"
     */

    static std::string encodeInt64(int64_t v) {
        v ^= folly::to<int64_t>(1) << 63;
        auto val = folly::Endian::big(v);
        std::string raw;
        raw.reserve(sizeof(int64_t));
        raw.append(reinterpret_cast<const char*>(&val), sizeof(int64_t));
        return raw;
    }

    static int64_t decodeInt64(const folly::StringPiece& raw) {
        auto val = *reinterpret_cast<const int64_t*>(raw.data());
        val = folly::Endian::big(val);
        val ^= folly::to<int64_t>(1) << 63;
        return val;
    }

    /*
     * Default, the double memory structure is :
     *   sign bit（1bit）+  exponent bit(11bit) + float bit(52bit)
     *   The first bit is the sign bit, 0 for positive and 1 for negative
     *   To keep the string in order, the first bit must to be inverse,
     *   then need to subtract from maximum.
     */

    static std::string encodeDouble(double v) {
        if (v < 0) {
            /**
             *   TODO : now, the -(std::numeric_limits<double>::min())
             *   have a problem of precision overflow. current return value is -nan.
             */
            auto i = *reinterpret_cast<const int64_t*>(&v);
            i = -(std::numeric_limits<int64_t>::max() + i);
            v = *reinterpret_cast<const double*>(&i);
        }
        auto val = folly::Endian::big(v);
        auto* c = reinterpret_cast<char*>(&val);
        c[0] ^= 0x80;
        std::string raw;
        raw.reserve(sizeof(double));
        raw.append(c, sizeof(double));
        return raw;
    }

    static double decodeDouble(const folly::StringPiece& raw) {
        char* v = const_cast<char*>(raw.data());
        v[0] ^= 0x80;
        auto val = *reinterpret_cast<const double*>(v);
        val = folly::Endian::big(val);
        if (val < 0) {
            auto i = *reinterpret_cast<const int64_t*>(&val);
            i = -(std::numeric_limits<int64_t >::max() + i);
            val = *reinterpret_cast<const double*>(&i);
        }
        return val;
    }

    static StatusOr<Value> decodeValue(const folly::StringPiece& raw, Value::Type type) {
        Value v;
        switch (type) {
            case Value::Type::INT : {
                v.setInt(decodeInt64(raw));
                break;
            }
            case Value::Type::FLOAT : {
                v.setFloat(decodeDouble(raw));
                break;
            }
            case Value::Type::BOOL : {
                v.setBool(*reinterpret_cast<const bool*>(raw.data()));
                break;
            }
            case Value::Type::STRING : {
                v.setStr(raw.str());
                break;
            }
            case Value::Type::DATE: {
                nebula::Date dt;
                memcpy(reinterpret_cast<void*>(&dt.year), &raw[0], sizeof(int16_t));
                memcpy(reinterpret_cast<void*>(&dt.month),
                       &raw[sizeof(int16_t)],
                       sizeof(int8_t));
                memcpy(reinterpret_cast<void*>(&dt.day),
                       &raw[sizeof(int16_t) + sizeof(int8_t)],
                       sizeof(int8_t));
                v.setDate(dt);
                break;
            }
            case Value::Type::DATETIME: {
                nebula::DateTime dt;
                memcpy(reinterpret_cast<void*>(&dt.year), &raw[0], sizeof(int16_t));
                memcpy(reinterpret_cast<void*>(&dt.month),
                       &raw[sizeof(int16_t)],
                       sizeof(int8_t));
                memcpy(reinterpret_cast<void*>(&dt.day),
                       &raw[sizeof(int16_t) + sizeof(int8_t)],
                       sizeof(int8_t));
                memcpy(reinterpret_cast<void*>(&dt.hour),
                       &raw[sizeof(int16_t) + 2 * sizeof(int8_t)],
                       sizeof(int8_t));
                memcpy(reinterpret_cast<void*>(&dt.minute),
                       &raw[sizeof(int16_t) + 3 * sizeof(int8_t)],
                       sizeof(int8_t));
                memcpy(reinterpret_cast<void*>(&dt.sec),
                       &raw[sizeof(int16_t) + 4 * sizeof(int8_t)],
                       sizeof(int8_t));
                memcpy(reinterpret_cast<void*>(&dt.microsec),
                       &raw[sizeof(int16_t) + 5 * sizeof(int8_t)],
                       sizeof(int32_t));
                memcpy(reinterpret_cast<void*>(&dt.timezone),
                       &raw[sizeof(int16_t) + 5 * sizeof(int8_t) + sizeof(int32_t)],
                       sizeof(int32_t));
                v.setDateTime(dt);
                break;
            }
            default:
                return Status::Error("Unknown value type");
        }
        return v;
    }

    static VertexIDSlice getIndexVertexID(size_t vIdLen, const folly::StringPiece& rawKey) {
        CHECK_GE(rawKey.size(), kVertexIndexLen + vIdLen);
        auto offset = rawKey.size() - vIdLen;
        return rawKey.subpiece(offset, vIdLen);
     }

    static VertexIDSlice getIndexSrcId(size_t vIdLen, const folly::StringPiece& rawKey) {
        CHECK_GE(rawKey.size(), kEdgeIndexLen + vIdLen * 2);
        auto offset = rawKey.size() - (vIdLen << 1) - sizeof(EdgeRanking);
        return rawKey.subpiece(offset, vIdLen);
    }

    static VertexIDSlice getIndexDstId(size_t vIdLen, const folly::StringPiece& rawKey) {
        CHECK_GE(rawKey.size(), kEdgeIndexLen + vIdLen * 2);
        auto offset = rawKey.size() - vIdLen;
        return rawKey.subpiece(offset, vIdLen);
    }

    static EdgeRanking getIndexRank(size_t vIdLen, const folly::StringPiece& rawKey) {
        CHECK_GE(rawKey.size(), kEdgeIndexLen + vIdLen * 2);
        auto offset = rawKey.size() - vIdLen - sizeof(EdgeRanking);
        return readInt<EdgeRanking>(rawKey.data() + offset, sizeof(EdgeRanking));
    }

    static bool isIndexKey(const folly::StringPiece& key) {
        constexpr int32_t len = static_cast<int32_t>(sizeof(NebulaKeyType));
        auto type = readInt<int32_t>(key.data(), len) & kTypeMask;
        return static_cast<uint32_t>(NebulaKeyType::kIndex) == type;
    }

    static IndexID getIndexId(const folly::StringPiece& rawKey) {
        auto offset = sizeof(PartitionID);
        return readInt<IndexID>(rawKey.data() + offset, sizeof(IndexID));
    }

    /**
     * Generate vertex|edge index key for kv store
     **/
    static void encodeValues(const std::vector<Value>& values, std::string& raw);

    static void encodeValuesWithNull(const std::vector<Value>& values,
                                     const NullCols& nullableCols,
                                     std::string& raw);

    static std::string vertexIndexKey(size_t vIdLen, PartitionID partId,
                                      IndexID indexId, VertexID vId,
                                      const std::vector<Value>& values,
                                      const NullCols& nullableCols = {});

    static std::string edgeIndexKey(size_t vIdLen, PartitionID partId,
                                    IndexID indexId, VertexID srcId,
                                    EdgeRanking rank, VertexID dstId,
                                    const std::vector<Value>& values,
                                    const NullCols& nullableCols = {});

    static std::string indexPrefix(PartitionID partId, IndexID indexId);

private:
    IndexKeyUtils() = delete;
};

}  // namespace nebula
#endif  // COMMON_INDEXKEYUTILS_H_

