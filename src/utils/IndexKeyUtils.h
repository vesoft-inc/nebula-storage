/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef UTILS_INDEXKEYUTILS_H_
#define UTILS_INDEXKEYUTILS_H_

#include "common/base/Base.h"
#include "common/base/StatusOr.h"
#include "common/interface/gen-cpp2/meta_types.h"
#include "utils/Types.h"

namespace nebula {

using PropertyType = nebula::meta::cpp2::PropertyType;

/**
 * This class supply some utils for index in kvstore.
 * */
class IndexKeyUtils final {
public:
    ~IndexKeyUtils() = default;

    /**
     * param valueTypes ： column type of each index column. If there are no nullable columns
     *                     in the index, the parameter can be empty.
     **/
    static std::string vertexIndexKey(size_t vIdLen, PartitionID partId,
                                      IndexID indexId, VertexID vId,
                                      const std::vector<Value>& values,
                                      const std::vector<Value::Type>& valueTypes = {}) {
        int32_t item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kIndex);
        std::string key;
        key.reserve(256);
        key.append(reinterpret_cast<const char*>(&item), sizeof(int32_t))
           .append(reinterpret_cast<const char*>(&indexId), sizeof(IndexID));
        // If have not nullable columns in the index, the valueTypes is empty.
        if (valueTypes.empty()) {
            encodeValues(values, key);
        } else {
            encodeValuesWithNull(values, valueTypes, key);
        }
        key.append(vId.data(), vId.size())
           .append(vIdLen - vId.size(), '\0');
        return key;
    }

    /**
     * param valueTypes ： column type of each index column. If there are no nullable columns
     *                     in the index, the parameter can be empty.
     **/
    static std::string edgeIndexKey(size_t vIdLen, PartitionID partId,
                                    IndexID indexId, VertexID srcId,
                                    EdgeRanking rank, VertexID dstId,
                                    const std::vector<Value>& values,
                                    const std::vector<Value::Type>& valueTypes = {}) {
        int32_t item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kIndex);
        std::string key;
        key.reserve(256);
        key.append(reinterpret_cast<const char*>(&item), sizeof(int32_t))
           .append(reinterpret_cast<const char*>(&indexId), sizeof(IndexID));
        // If have not nullable columns in the index, the valueTypes is empty.
        if (valueTypes.empty()) {
            encodeValues(values, key);
        } else {
            encodeValuesWithNull(values, valueTypes, key);
        }
        key.append(srcId.data(), srcId.size())
           .append(vIdLen - srcId.size(), '\0')
           .append(reinterpret_cast<const char*>(&rank), sizeof(EdgeRanking))
           .append(dstId.data(), dstId.size())
           .append(vIdLen - dstId.size(), '\0');
        return key;
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

    static std::string indexPrefix(PartitionID partId, IndexID indexId) {
        PartitionID item = (partId << kPartitionOffset)
                           | static_cast<uint32_t>(NebulaKeyType::kIndex);
        std::string key;
        key.reserve(sizeof(PartitionID) + sizeof(IndexID));
        key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
           .append(reinterpret_cast<const char*>(&indexId), sizeof(IndexID));
        return key;
    }

    static void encodeValues(const std::vector<Value>& values, std::string& raw);

    static void encodeValuesWithNull(const std::vector<Value>& values,
                                     const std::vector<Value::Type>& colsType,
                                     std::string& raw);

    static Value::Type toValueType(PropertyType type);

    static std::string encodeNullValue(Value::Type type);

    static std::string encodeValue(const Value& v);

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

    static std::string encodeInt64(int64_t v);

    static int64_t decodeInt64(const folly::StringPiece& raw);

    /*
     * Default, the double memory structure is :
     *   sign bit（1bit）+  exponent bit(11bit) + float bit(52bit)
     *   The first bit is the sign bit, 0 for positive and 1 for negative
     *   To keep the string in order, the first bit must to be inverse,
     *   then need to subtract from maximum.
     */

    static std::string encodeDouble(double v);

    static double decodeDouble(const folly::StringPiece& raw);

    static StatusOr<Value> decodeValue(const folly::StringPiece& raw, Value::Type type);

private:
    IndexKeyUtils() = delete;
};

}  // namespace nebula
#endif  // UTILS_INDEXKEYUTILS_H_

