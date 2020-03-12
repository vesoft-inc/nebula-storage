/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CODEC_ROWREADER_H_
#define CODEC_ROWREADER_H_

#include "base/Base.h"
#include "datatypes/Value.h"
#include "codec/Common.h"
#include "meta/SchemaProviderIf.h"

namespace nebula {

/**
 * This class decodes one row of data
 */
class RowReader {
public:
    class Iterator;

    class Cell final {
        friend class Iterator;
    public:
        Value value() const noexcept;

    private:
        const Iterator* iter_;

        explicit Cell(const Iterator* iter) : iter_(iter) {}
    };


    class Iterator final {
        friend class Cell;
        friend class RowReader;
    public:
        Iterator(Iterator&& iter);

        void operator=(Iterator&& rhs);

        const Cell& operator*() const noexcept;
        const Cell* operator->() const noexcept;

        Iterator& operator++();

        bool operator==(const Iterator& rhs) const noexcept;
        bool operator!=(const Iterator& rhs) const noexcept {
            return ! operator==(rhs);
        }

    private:
        const RowReader* reader_;
        Cell cell_;
        size_t index_;

        Iterator(const RowReader* reader, size_t index = 0)
            : reader_(reader), cell_(this), index_(index) {}
    };


public:
/*
    static std::unique_ptr<RowReader> getTagPropReader(
        meta::SchemaManager* schemaMan,
        GraphSpaceID space,
        TagID tag,
        std::string row);
    static std::unique_ptr<RowReader> getEdgePropReader(
        meta::SchemaManager* schemaMan,
        GraphSpaceID space,
        EdgeType edge,
        std::string row);
*/
    static std::unique_ptr<RowReader> getRowReader(
        const meta::SchemaProviderIf* schema,
        folly::StringPiece row);

    virtual ~RowReader() = default;

    virtual Value getValueByName(const std::string& prop) const noexcept = 0;
    virtual Value getValueByIndex(const int64_t index) const noexcept = 0;

    virtual int32_t readerVer() const noexcept = 0;

    Iterator begin() const noexcept {
        return Iterator(this, 0);
    }

    const Iterator& end() const noexcept {
        return endIter_;
    }

    SchemaVer schemaVer() const noexcept {
        return schema_->getVersion();
    }

    size_t numFields() const noexcept {
        return schema_->getNumFields();
    }

    const meta::SchemaProviderIf* getSchema() const {
        return schema_;
    }

    const std::string getData() const {
        return data_.toString();
    }

protected:
    const meta::SchemaProviderIf* schema_;
    folly::StringPiece data_;

    explicit RowReader(const meta::SchemaProviderIf* schema, folly::StringPiece row)
        : schema_(schema)
        , data_(row)
        , endIter_(this, schema_->getNumFields()) {}

    static void getVersions(const folly::StringPiece& row,
                            SchemaVer& schemaVer,
                            int32_t& readerVer);

private:
    const Iterator endIter_;
};

}  // namespace nebula
#endif  // CODEC_ROWREADER_H_
