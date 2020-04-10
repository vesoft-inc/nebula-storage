/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_INDEXKEYUTILS_H_
#define COMMON_INDEXKEYUTILS_H_

#include "base/Base.h"
#include "base/StatusOr.h"
#include "thrift/ThriftTypes.h"
#include "interface/gen-cpp2/meta_types.h"
#include "common/Types.h"

namespace nebula {

using IndexValues = std::vector<std::pair<nebula::meta::cpp2::PropertyType, std::string>>;

/**
 * This class supply some utils for index in kvstore.
 * */
class IndexKeyUtils final {
public:
    ~IndexKeyUtils() = default;

    static std::string encodeValue(const Value& v)  {
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
                memcpy(&buf[0], reinterpret_cast<const void*>(&v.getDate().year), sizeof(int16_t));
                buf[sizeof(int16_t)] = v.getDate().month;
                buf[sizeof(int16_t) + sizeof(int8_t)] = v.getDate().day;
                return buf;
            }
            case Value::Type::DATETIME : {
                std::string buf;
                memcpy(&buf[0], reinterpret_cast<const void*>(&v.getDateTime().year),
                       sizeof(int16_t));
                buf[sizeof(int16_t)] = v.getDateTime().month;
                buf[sizeof(int16_t) + sizeof(int8_t)] = v.getDateTime().day;
                buf[sizeof(int16_t) + 2 * sizeof(int8_t)] = v.getDateTime().hour;
                buf[sizeof(int16_t) + 3 * sizeof(int8_t)] = v.getDateTime().minute;
                buf[sizeof(int16_t) + 4 * sizeof(int8_t)] = v.getDateTime().sec;
                memcpy(&buf[sizeof(int16_t) + 5 * sizeof(int8_t)],
                       reinterpret_cast<const void*>(&v.getDateTime().microsec),
                       sizeof(int32_t));
                memcpy(&buf[sizeof(int16_t) + 5 * sizeof(int8_t) + sizeof(int32_t)],
                       reinterpret_cast<const void*>(&v.getDateTime().timezone),
                       sizeof(int32_t));
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

    static StatusOr<Value> decodeValue(const folly::StringPiece& raw,
                                       nebula::meta::cpp2::PropertyType type) {
        Value v;
        switch (type) {
            case nebula::meta::cpp2::PropertyType::BOOL : {
                v.setBool(*reinterpret_cast<const bool*>(raw.data()));
                break;
            }
            case nebula::meta::cpp2::PropertyType::INT64 :
            case nebula::meta::cpp2::PropertyType::TIMESTAMP : {
                v.setInt(decodeInt64(raw));
                break;
            }
            case nebula::meta::cpp2::PropertyType::DOUBLE :
            case nebula::meta::cpp2::PropertyType::FLOAT : {
                v.setFloat(decodeDouble(raw));
                break;
            }
            case nebula::meta::cpp2::PropertyType::STRING : {
                v.setStr(raw.str());
                break;
            }
            case nebula::meta::cpp2::PropertyType::DATE: {
                Date dt;
                memcpy(reinterpret_cast<void*>(&dt.year), &raw[0], sizeof(int16_t));
                memcpy(reinterpret_cast<void*>(&dt.month),
                       &raw[sizeof(int16_t)],
                       sizeof(int8_t));
                memcpy(reinterpret_cast<void*>(&dt.day),
                       &raw[sizeof(int16_t) + sizeof(int8_t)],
                       sizeof(int8_t));
                return std::move(dt);
            }
            case nebula::meta::cpp2::PropertyType::DATETIME: {
                DateTime dt;
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
                return std::move(dt);
            }
            default:
                return Status::Error("Unknown value type");
        }
        return std::move(v);
    }

    static VertexIntID getIndexVertexIntID(const folly::StringPiece& rawKey) {
        CHECK_GE(rawKey.size(), kVertexIndexLen);
        auto offset = rawKey.size() - sizeof(VertexIntID);
        return *reinterpret_cast<const VertexIntID*>(rawKey.data() + offset);
     }

    static VertexIntID getIndexSrcId(const folly::StringPiece& rawKey) {
        CHECK_GE(rawKey.size(), kEdgeIndexLen);
        auto offset = rawKey.size() -
                      sizeof(VertexIntID) * 2 - sizeof(EdgeRanking);
        return readInt<VertexIntID>(rawKey.data() + offset, sizeof(VertexIntID));
    }

    static VertexIntID getIndexDstId(const folly::StringPiece& rawKey) {
        CHECK_GE(rawKey.size(), kEdgeIndexLen);
        auto offset = rawKey.size() - sizeof(VertexIntID);
        return readInt<VertexIntID>(rawKey.data() + offset, sizeof(VertexIntID));
    }

    static EdgeRanking getIndexRank(const folly::StringPiece& rawKey) {
        CHECK_GE(rawKey.size(), kEdgeIndexLen);
        auto offset = rawKey.size() - sizeof(VertexIntID) - sizeof(EdgeRanking);
        return readInt<EdgeRanking>(rawKey.data() + offset, sizeof(EdgeRanking));
    }

    

    /**
     * Generate vertex|edge index key for kv store
     **/
    static void indexRaw(const IndexValues &values, std::string& raw);

    static std::string vertexIndexKey(PartitionID partId, IndexID indexId, VertexID vId,
                                      const IndexValues& values);

    static std::string edgeIndexKey(PartitionID partId, IndexID indexId,
                                    VertexID srcId, EdgeRanking rank,
                                    VertexID dstId, const IndexValues& values);

    static std::string indexPrefix(PartitionID partId, IndexID indexId);

private:
    IndexKeyUtils() = delete;

};

}  // namespace nebula
#endif  // COMMON_INDEXKEYUTILS_H_

