/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CODEC_ROWWRITERV2_H_
#define CODEC_ROWWRITERV2_H_

#include "base/Base.h"
#include "meta/SchemaProviderIf.h"
#include "codec/RowReader.h"

namespace nebula {

enum class WriteResult {
    SUCCEEDED = 0,
    UNKNOWN_FIELD = -1,
    TYPE_MISMATCH = -2,
    OUT_OF_RANGE = -3,
    NOT_NULLABLE = -4,
    FIELD_UNSET = -5,
};


class RowWriterV2 {
public:
    explicit RowWriterV2(const meta::SchemaProviderIf* schema);
    // This constructor only takes a V2 encoded string
    RowWriterV2(const meta::SchemaProviderIf* schema, std::string&& encoded);
    // This constructor only takes a V2 encoded string
    RowWriterV2(const meta::SchemaProviderIf* schema, const std::string& encoded);
    // This constructor can handle both V1 and V2 readers
    explicit RowWriterV2(RowReader& reader);

    ~RowWriterV2() = default;

    // Return the exact length of the encoded binary array
    int64_t size() const noexcept {
        return buf_.size();
    }

    const meta::SchemaProviderIf* schema() const {
        return schema_;
    }

    const std::string& getEncodedStr() const noexcept {
        CHECK(finished_) << "You need to call finish() first";
        return buf_;
    }

    std::string moveEncodedStr() noexcept {
        CHECK(finished_) << "You need to call finish() first";
        return std::move(buf_);
    }

    WriteResult finish() noexcept;

    // Data write
    template<typename T>
    WriteResult set(ssize_t index, T&& v) noexcept {
        CHECK(!finished_) << "You have called finish()";
        if (index < 0 || index >= schema_->getNumFields()) {
            return WriteResult::UNKNOWN_FIELD;
        }
        return write(index, std::forward<T>(v));
    }

    // Data write
    template<typename T>
    WriteResult set(folly::StringPiece name, T&& v) noexcept {
        CHECK(!finished_) << "You have called finish()";
        int64_t index = schema_->getFieldIndex(name);
        if (index >= 0) {
            return write(index, std::forward<T>(v));
        } else {
            return WriteResult::UNKNOWN_FIELD;
        }
    }

    WriteResult setNull(ssize_t index) noexcept;

    WriteResult setNull(folly::StringPiece name) noexcept;

private:
    const meta::SchemaProviderIf* schema_;
    std::string buf_;
    std::vector<bool> isSet_;
    // Ther number of bytes ocupied by header and the schema version
    size_t headerLen_;
    size_t numNullBytes_;
    size_t approxStrLen_;
    bool finished_;

    // When outOfSpaceStr_ is true, variant length string fields
    // could hold an index, referring to the strings in the strList_
    // By default, outOfSpaceStr_ is false. It turns true only when
    // the existing variant length string is modified
    bool outOfSpaceStr_;
    std::vector<std::string> strList_;

    WriteResult checkUnsetFields() noexcept;
    std::string processOutOfSpace() noexcept;

    void processV2EncodedStr() noexcept;

    void setNullBit(ssize_t index) noexcept;
    void clearNullBit(ssize_t index) noexcept;
    // Return true if the field is NULL; otherwise, return false
    bool checkNullBit(ssize_t index) const noexcept;

    WriteResult write(ssize_t index, bool v) noexcept;
    WriteResult write(ssize_t index, float v) noexcept;
    WriteResult write(ssize_t index, double v) noexcept;

    WriteResult write(ssize_t index, int8_t v) noexcept;
    WriteResult write(ssize_t index, int16_t v) noexcept;
    WriteResult write(ssize_t index, int32_t v) noexcept;
    WriteResult write(ssize_t index, int64_t v) noexcept;
    WriteResult write(ssize_t index, uint8_t v) noexcept;
    WriteResult write(ssize_t index, uint16_t v) noexcept;
    WriteResult write(ssize_t index, uint32_t v) noexcept;
    WriteResult write(ssize_t index, uint64_t v) noexcept;

    WriteResult write(ssize_t index, const std::string& v) noexcept;
    WriteResult write(ssize_t index, folly::StringPiece v) noexcept;
    WriteResult write(ssize_t index, const char* v) noexcept;

    WriteResult write(ssize_t index, const Date& v) noexcept;
    WriteResult write(ssize_t index, const DateTime& v) noexcept;
};

}  // namespace nebula
#endif  // CODEC_ROWWRITERV2_H_

