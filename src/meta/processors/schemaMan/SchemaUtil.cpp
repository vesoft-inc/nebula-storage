/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "SchemaUtil.h"

namespace nebula {
namespace meta {
bool SchemaUtil::checkType(std::vector<cpp2::ColumnDef> &columns) {
    for (auto& column : columns) {
        if (column.__isset.default_value) {
            auto name = column.get_name();
            auto* value = column.get_default_value();
            switch (column.get_type()) {
                case cpp2::PropertyType::BOOL:
                    if (value->type() != nebula::Value::Type::BOOL) {
                        LOG(ERROR) << "Create Tag Failed: " << name
                                   << " type mismatch";
                        return false;
                    }
                    break;
                case cpp2::PropertyType::INT8:
                case cpp2::PropertyType::INT16:
                case cpp2::PropertyType::INT32:
                case cpp2::PropertyType::INT64:
                    if (value->type() != nebula::Value::Type::INT) {
                        LOG(ERROR) << "Create Tag Failed: " << name
                                   << " type mismatch";
                        return false;
                    }
                    break;
                case cpp2::PropertyType::FLOAT:
                case cpp2::PropertyType::DOUBLE:
                    if (value->type() != nebula::Value::Type::FLOAT) {
                        LOG(ERROR) << "Create Tag Failed: " << name
                                   << " type mismatch";
                        return false;
                    }
                    break;
                case cpp2::PropertyType::STRING:
                    if (value->type() != nebula::Value::Type::STRING) {
                        LOG(ERROR) << "Create Tag Failed: " << name
                                   << " type mismatch";
                        return false;
                    }
                    break;
                case cpp2::PropertyType::FIXED_STRING: {
                    if (value->type() != nebula::Value::Type::STRING) {
                        LOG(ERROR) << "Create Tag Failed: " << name
                                   << " type mismatch";
                        return false;
                    }
                    size_t typeLen = column.__isset.type_length ? *column.get_type_length() : 0;
                    if (value->getStr().size() > typeLen) {
                        const auto trimStr = value->getStr().substr(0, typeLen);
                        value->setStr(trimStr);
                    }
                    break;
                }
                case cpp2::PropertyType::TIMESTAMP:
                    if (value->type() != nebula::Value::Type::INT) {
                        LOG(ERROR) << "Create Tag Failed: " << name
                                   << " type mismatch";
                        return false;
                    }
                    break;
                case cpp2::PropertyType::DATE:
                    if (value->type() != nebula::Value::Type::DATE) {
                        LOG(ERROR) << "Create Tag Failed: " << name
                                   << " type mismatch";
                        return false;
                    }
                    break;
                case cpp2::PropertyType::DATETIME:
                    if (value->type() != nebula::Value::Type::DATETIME) {
                        LOG(ERROR) << "Create Tag Failed: " << name
                                   << " type mismatch";
                        return false;
                    }
                    break;
                default:
                    LOG(ERROR) << "Unsupported type";
                    return false;
            }
        }
    }
    return true;
}
}  // namespace meta
}  // namespace nebula

