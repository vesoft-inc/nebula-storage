/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_INDEXUTILS_H
#define STORAGE_INDEXUTILS_H

#include "codec/RowReader.h"

namespace nebula {
namespace storage {

class IndexUtils {
public:
    ~IndexUtils() = default;

    static StatusOr<std::vector<Value>>
    collectIndexValues(RowReader* reader,
                       const std::vector<meta::cpp2::ColumnDef>& cols,
                       std::vector<Value::Type>& colsType);

    static Status checkValue(const Value& v, bool isNullable);

private:
    IndexUtils() = delete;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_INDEXUTILS_H
