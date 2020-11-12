/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "utils/IndexKeyUtils.h"

namespace nebula {

// doodle
/*
// static
void IndexKeyUtils::encodeValues(const std::vector<Value>& values, std::string& raw) {
    for (auto& value : values) {
        raw.append(encodeValue(value));
    }
}

// static
void IndexKeyUtils::encodeValuesWithNull(const std::vector<Value>& values,
                                         const std::vector<Value::Type>& colsType,
                                         std::string& raw) {
    // An index has a maximum of 16 columns. 2 byte (16 bit) is enough.
    u_short nullableBitset = 0;

    for (size_t i = 0; i < values.size(); i++) {
        std::string val;
        // if the value is null, the nullable bit should be '1'.
        // And create a string of a fixed length，filled with 0.
        // if the value is not null, encode value.
        if (values[i].isNull()) {
            nullableBitset |= 0x8000 >> i;
            val = encodeNullValue(colsType[i]);
        } else {
            val = encodeValue(values[i]);
        }
        raw.append(val);
    }

    raw.append(reinterpret_cast<const char*>(&nullableBitset), sizeof(u_short));
}
*/

// static
std::string IndexKeyUtils::vertexIndexKey(size_t vIdLen, PartitionID partId,
                                          IndexID indexId, VertexID vId,
                                          std::string&& values) {
    int32_t item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kIndex);
    std::string key;
    key.reserve(256);
    key.append(reinterpret_cast<const char*>(&item), sizeof(int32_t))
       .append(reinterpret_cast<const char*>(&indexId), sizeof(IndexID))
       .append(values)
       .append(vId.data(), vId.size())
       .append(vIdLen - vId.size(), '\0');
    return key;
}

// static
std::string IndexKeyUtils::edgeIndexKey(size_t vIdLen, PartitionID partId,
                                        IndexID indexId, VertexID srcId,
                                        EdgeRanking rank, VertexID dstId,
                                        std::string&& values) {
    int32_t item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kIndex);
    std::string key;
    key.reserve(256);
    key.append(reinterpret_cast<const char*>(&item), sizeof(int32_t))
       .append(reinterpret_cast<const char*>(&indexId), sizeof(IndexID))
       .append(values)
       .append(srcId.data(), srcId.size())
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

// static
std::string IndexKeyUtils::indexPrefix(PartitionID partId) {
    PartitionID item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kIndex);
    std::string key;
    key.reserve(sizeof(PartitionID));
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID));
    return key;
}

// static
StatusOr<std::string>
IndexKeyUtils::collectIndexValues(RowReader* reader,
                                  const std::vector<nebula::meta::cpp2::ColumnDef>& cols) {
    if (reader == nullptr) {
        return Status::Error("Invalid row reader");
    }
    std::string values;
    values.reserve(256);
    u_short nullableBitSet = 0;
    for (size_t i = 0; i < cols.size(); i++) {
        const auto& col = cols[i];
        auto v = reader->getValueByName(col.get_name());
        auto isNullable = col.__isset.nullable && *(col.get_nullable());
        auto ret = checkValue(v, isNullable);
        if (!ret.ok()) {
            LOG(ERROR) << "prop error by : " << col.get_name()
                       << ". status : " << ret;
            return ret;
        }
        if (!v.isNull()) {
            if (col.type.get_type() == meta::cpp2::PropertyType::FIXED_STRING) {
                auto len = static_cast<size_t>(*col.type.get_type_length());
                std::string str = v.moveStr();
                if (len > str.size()) {
                    str.append(len - str.size(), '\0');
                } else {
                    str = str.substr(0, len);
                }
                values.append(str);
            } else {
                values.append(encodeValue(v));
            }
        } else {
            nullableBitSet |= 0x8000 >> i;
            auto type = IndexKeyUtils::toValueType(col.type.get_type());
            values.append(encodeNullValue(type, col.type.get_type_length()));
        }
    }
    if (nullableBitSet != 0) {
        values.append(reinterpret_cast<const char*>(&nullableBitSet), sizeof(u_short));
    }
    return values;
}

// static
Status IndexKeyUtils::checkValue(const Value& v, bool isNullable) {
    if (!v.isNull()) {
        return Status::OK();
    }

    switch (v.getNull()) {
        case nebula::NullType::UNKNOWN_PROP : {
            return Status::Error("Unknown prop");
        }
        case nebula::NullType::__NULL__ : {
            if (!isNullable) {
                return Status::Error("Not allowed to be null");
            }
            return Status::OK();
        }
        case nebula::NullType::BAD_DATA : {
            return Status::Error("Bad data");
        }
        case nebula::NullType::BAD_TYPE : {
            return Status::Error("Bad type");
        }
        case nebula::NullType::ERR_OVERFLOW : {
            return Status::Error("Data overflow");
        }
        case nebula::NullType::DIV_BY_ZERO : {
            return Status::Error("Div zero");
        }
        case nebula::NullType::NaN : {
            return Status::Error("NaN");
        }
        case nebula::NullType::OUT_OF_RANGE : {
            return Status::Error("Out of range");
        }
    }
    LOG(FATAL) << "Unknown Null type " << static_cast<int>(v.getNull());
}

}  // namespace nebula
