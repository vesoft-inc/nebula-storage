/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "codec/RowReader.h"
#include "codec/RowReaderWrapper.h"

namespace nebula {

/*********************************************
 *
 * class RowReader::Cell
 *
 ********************************************/
Value RowReader::Cell::value() const noexcept {
    return iter_->reader_->getValueByIndex(iter_->index_);
}


/*********************************************
 *
 * class RowReader::Iterator
 *
 ********************************************/
RowReader::Iterator::Iterator(Iterator&& iter)
    : reader_(iter.reader_)
    , cell_(std::move(iter.cell_))
    , index_(iter.index_) {
}


void RowReader::Iterator::operator=(Iterator&& rhs) {
    reader_ = rhs.reader_;
    cell_ = std::move(rhs.cell_);
    index_ = rhs.index_;
}


bool RowReader::Iterator::operator==(const Iterator& rhs) const noexcept {
    return reader_ == rhs.reader_ && index_ == rhs.index_;
}


const RowReader::Cell& RowReader::Iterator::operator*() const noexcept {
    return cell_;
}


const RowReader::Cell* RowReader::Iterator::operator->() const noexcept {
    return &cell_;
}


RowReader::Iterator& RowReader::Iterator::operator++() {
    if (index_ < reader_->numFields()) {
        ++index_;
    }
    return *this;
}


/*********************************************
 *
 * class RowReader
 *
 ********************************************/

// static
std::unique_ptr<RowReader> RowReader::getTagPropReader(
        meta::SchemaManager* schemaMan,
        GraphSpaceID space,
        TagID tag,
        folly::StringPiece row) {
    if (schemaMan == nullptr) {
        LOG(ERROR) << "schemaMan should not be nullptr!";
        return nullptr;
    }
    SchemaVer schemaVer;
    int32_t readerVer;
    RowReaderWrapper::getVersions(row, schemaVer, readerVer);
    if (schemaVer >= 0) {
        auto schema = schemaMan->getTagSchema(space, tag, schemaVer);
        if (schema == nullptr) {
            return nullptr;
        }
        return getRowReader(schema.get(), row, readerVer);
    } else {
        LOG(WARNING) << "Invalid schema version in the row data!";
        return nullptr;
    }
}


// static
std::unique_ptr<RowReader> RowReader::getEdgePropReader(
        meta::SchemaManager* schemaMan,
        GraphSpaceID space,
        EdgeType edge,
        folly::StringPiece row) {
    if (schemaMan == nullptr) {
        LOG(ERROR) << "schemaMan should not be nullptr!";
        return nullptr;
    }
    SchemaVer schemaVer;
    int32_t readerVer;
    RowReaderWrapper::getVersions(row, schemaVer, readerVer);
    if (schemaVer >= 0) {
        auto schema = schemaMan->getEdgeSchema(space, edge, schemaVer);
        if (schema == nullptr) {
            return nullptr;
        }
        return getRowReader(schema.get(), row, readerVer);
    } else {
        LOG(WARNING) << "Invalid schema version in the row data!";
        return nullptr;
    }
}

// static
std::unique_ptr<RowReader> RowReader::getRowReader(
        const meta::SchemaProviderIf* schema,
        folly::StringPiece row,
        int32_t readerVer) {
    auto reader = std::make_unique<RowReaderWrapper>();
    if (reader->reset(schema, row, readerVer)) {
        return reader;
    } else {
        LOG(ERROR) << "Failed to initiate the reader, most likely the data"
                      "is corrupted. The data is ["
                   << toHexStr(row)
                   << "]";
        return std::unique_ptr<RowReader>();
    }
}


bool RowReader::resetImpl(meta::SchemaProviderIf const* schema,
                          folly::StringPiece row) noexcept {
    schema_ = schema;
    data_ = row;

    endIter_.reset(schema_->getNumFields());
    return true;
}

}  // namespace nebula
