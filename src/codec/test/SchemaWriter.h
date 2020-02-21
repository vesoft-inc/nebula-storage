/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CODEC_TEST_SCHEMAWRITER_H_
#define CODEC_TEST_SCHEMAWRITER_H_

#include "base/Base.h"
#include "codec/test/ResultSchemaProvider.h"

namespace nebula {

class SchemaWriter : public ResultSchemaProvider {
public:
    explicit SchemaWriter(SchemaVer ver = 0) : ResultSchemaProvider(ver) {}

    SchemaWriter& appendCol(folly::StringPiece name,
                            meta::cpp2::PropertyType type,
                            int32_t fixedStrLen = 0,
                            bool nullable = false,
                            Value defaultValue = Value()) noexcept;

private:
};

}  // namespace nebula
#endif  // CODEC_TEST_SCHEMAWRITER_H_

