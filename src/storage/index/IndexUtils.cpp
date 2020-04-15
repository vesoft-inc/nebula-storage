/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/IndexKeyUtils.h"
#include "storage/index/IndexUtils.h"

namespace nebula {
namespace storage {

StatusOr<std::vector<Value>>
IndexUtils::collectIndexValues(RowReader* reader,
                               const std::vector<meta::cpp2::ColumnDef>& cols,
                               std::vector<Value::Type>& colsType) {
    std::vector<Value> values;
    bool haveNullCol = false;
    if (reader == nullptr) {
        return Status::Error("Invalid row reader");
    }
    for (auto& col : cols) {
        auto v = reader->getValueByName(col.get_name());
        auto isNullable = col.__isset.nullable && *(col.get_nullable());
        if (isNullable && !haveNullCol) {
            haveNullCol = true;
        }
        colsType.emplace_back(IndexKeyUtils::toValueType(col.get_type()));
        auto ret = checkValue(v, isNullable);
        if (!ret.ok()) {
            LOG(ERROR) << "prop error by : " << col.get_name()
                       << ". status : " << ret;
            return ret;
        }
        values.emplace_back(std::move(v));
    }
    if (!haveNullCol) {
        colsType.clear();
    }
    return values;
}

Status IndexUtils::checkValue(const Value& v, bool isNullable) {
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
    }
    return Status::OK();
}

}  // namespace storage
}  // namespace nebula
