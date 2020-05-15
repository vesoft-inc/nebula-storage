/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/IndexKeyUtils.h"

namespace nebula {

// static
void IndexKeyUtils::encodeValues(const std::vector<Value>& values, std::string& raw) {
    std::vector<int32_t> colsLen;
    for (auto& value : values) {
        if (value.type() == Value::Type::STRING) {
            colsLen.emplace_back(value.getStr().size());
        }
        raw.append(encodeValue(value));
    }
    for (auto len : colsLen) {
        raw.append(reinterpret_cast<const char*>(&len), sizeof(int32_t));
    }
}

// static
void IndexKeyUtils::encodeValuesWithNull(const std::vector<Value>& values,
                                         const NullCols& nullableCols,
                                         std::string& raw) {
    std::vector<int32_t> colsLen;
    std::vector<char> nullables;
    bool hasNullCol = false;
    char nullable = '\0';
    int32_t nullColPos = 0;
    for (size_t i = 0; i < values.size(); i++) {
        std::string val;
        if (nullableCols[i].second) {
            if (!hasNullCol) {
                hasNullCol = true;
            }
            // if the value is null, the nullable bit should be '1'.
            // And create a string of a fixed length，filled with 0.
            // if the value is not null, encode value.
            if (values[i].isNull()) {
                nullable |= 0x80 >> nullColPos%8;
                val = encodeNullValue(nullableCols[i].first);
            } else {
                val = encodeValue(values[i]);
            }
            nullColPos++;
            // One bit mark one nullable column.
            // The smallest store of null column is one byte(8 bit).
            // If less than 8 nullable columns, the byte suffix is filled with 0.
            // If a byte is completely filled, a new byte should be create.
            if (nullColPos > 7 && nullColPos%8 == 0) {
                hasNullCol = false;
                nullables.emplace_back(nullable);
                nullable = '\0';
            }
        } else {
            val = encodeValue(values[i]);
        }

        if (nullableCols[i].first == Value::Type::STRING) {
            colsLen.emplace_back(val.size());
        }
        raw.append(val);
    }

    if (hasNullCol) {
        nullables.emplace_back(std::move(nullable));
    }
    for (auto n : nullables) {
        raw.append(reinterpret_cast<const char*>(&n), sizeof(char));
    }
    for (auto len : colsLen) {
        raw.append(reinterpret_cast<const char*>(&len), sizeof(int32_t));
    }
}

// static
std::string IndexKeyUtils::vertexIndexKey(size_t vIdLen, PartitionID partId,
                                          IndexID indexId, VertexID vId,
                                          const std::vector<Value>& values,
                                          const NullCols& nullableCols) {
    int32_t item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kIndex);
    std::string key;
    key.reserve(256);
    key.append(reinterpret_cast<const char*>(&item), sizeof(int32_t))
       .append(reinterpret_cast<const char*>(&indexId), sizeof(IndexID));
    if (nullableCols.empty()) {
        encodeValues(values, key);
    } else {
        encodeValuesWithNull(values, nullableCols, key);
    }
    key.append(vId.data(), vId.size())
       .append(vIdLen - vId.size(), '\0');
    return key;
}

// static
std::string IndexKeyUtils::edgeIndexKey(size_t vIdLen, PartitionID partId,
                                        IndexID indexId, VertexID srcId,
                                        EdgeRanking rank, VertexID dstId,
                                        const std::vector<Value>& values,
                                        const NullCols& nullableCols) {
    int32_t item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kIndex);
    std::string key;
    key.reserve(256);
    key.append(reinterpret_cast<const char*>(&item), sizeof(int32_t))
       .append(reinterpret_cast<const char*>(&indexId), sizeof(IndexID));
    if (nullableCols.empty()) {
        encodeValues(values, key);
    } else {
        encodeValuesWithNull(values, nullableCols, key);
    }
    key.append(srcId.data(), srcId.size())
       .append(vIdLen - srcId.size(), '\0')
       .append(reinterpret_cast<const char*>(&rank), sizeof(EdgeRanking))
       .append(dstId.data(), dstId.size())
       .append(vIdLen - dstId.size(), '\0');
    return key;
}

// static
std::string IndexKeyUtils::indexPrefix(PartitionID partId, IndexID indexId) {
    PartitionID item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kIndex);
    std::string key;
    key.reserve(sizeof(PartitionID) + sizeof(IndexID));
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&indexId), sizeof(IndexID));
    return key;
}

}  // namespace nebula

