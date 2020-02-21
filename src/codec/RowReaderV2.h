/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CODEC_ROWREADERV2_H_
#define CODEC_ROWREADERV2_H_

#include "base/Base.h"
#include <gtest/gtest_prod.h>
#include "meta/SchemaProviderIf.h"
#include "codec/RowReader.h"

namespace nebula {

/**
 * This class decodes the data from version 2.0
 */
class RowReaderV2 : public RowReader {
    friend class RowReader;

    FRIEND_TEST(RowReaderV2, headerInfo);
    FRIEND_TEST(RowReaderV2, encodedData);
    FRIEND_TEST(RowReaderV2, iterator);

public:
    RowReaderV2(const meta::SchemaProviderIf* schema,
                std::string row);

    virtual ~RowReaderV2() = default;

    Value getValueByName(const std::string& prop) const noexcept override;
    Value getValueByIndex(const int64_t index) const noexcept override;

    int32_t readerVer() const noexcept override {
        return 2;
    }

private:
    size_t headerLen_;
    size_t numNullBytes_;

    bool isNull(size_t index) const;
};

}  // namespace nebula
#endif  // CODEC_ROWREADERV2_H_

