/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/CommonUtils.h"
#include "common/time/WallClock.h"

namespace nebula {
namespace storage {

bool CommonUtils::checkDataExpiredForTTL(const meta::SchemaProviderIf* schema,
                                         RowReader* reader,
                                         const std::string& ttlCol,
                                         int64_t ttlDuration) {
    auto v = reader->getValueByName(ttlCol);
    return checkDataExpiredForTTL(schema, v, ttlCol, ttlDuration);
}

bool CommonUtils::checkDataExpiredForTTL(const meta::SchemaProviderIf* schema,
                                         const Value& v,
                                         const std::string& ttlCol,
                                         int64_t ttlDuration) {
    const auto& ftype = schema->getFieldType(ttlCol);
    if (ftype != meta::cpp2::PropertyType::TIMESTAMP && ftype != meta::cpp2::PropertyType::INT64) {
        return false;
    }
    auto now = time::WallClock::fastNowInSec();

    // if the value is not INT type (sush as NULL), it will never expire.
    // TODO (sky) : DateTime
    if (v.isInt() && (now > (v.getInt() + ttlDuration))) {
        VLOG(2) << "ttl expired";
        return true;
    }
    return false;
}

std::tuple<bool, int64_t, std::string>
CommonUtils::ttlProps(const meta::SchemaProviderIf* schema) {
    DCHECK(schema != nullptr);
    const auto* ns = dynamic_cast<const meta::NebulaSchemaProvider*>(schema);
    const auto sp = ns->getProp();
    int64_t duration = 0;
    if (sp.get_ttl_duration()) {
        duration = *sp.get_ttl_duration();
    }
    std::string col;
    if (sp.get_ttl_col()) {
        col = *sp.get_ttl_col();
    }
    return std::make_tuple(!(duration <= 0 || col.empty()), duration, col);
}

StatusOr<Value> CommonUtils::ttlValue(const meta::SchemaProviderIf* schema, RowReader* reader) {
    DCHECK(schema != nullptr);
    const auto* ns = dynamic_cast<const meta::NebulaSchemaProvider*>(schema);
    auto tuple = ttlProps(ns);
    auto& [effective, _, column] = tuple;
    if (!effective) {
        return Status::Error();
    }
    return reader->getValueByName(std::move(column));
}

}  // namespace storage
}  // namespace nebula
