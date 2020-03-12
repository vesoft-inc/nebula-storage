/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "codec/RowReader.h"
#include "codec/RowReaderV1.h"
#include "codec/RowReaderV2.h"

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
/*
std::unique_ptr<RowReader> RowReader::getTagPropReader(
        meta::SchemaManager* schemaMan,
        GraphSpaceID space,
        TagID tag,
        std::string row) {
    CHECK_NOTNULL(schemaMan);
    int32_t ver = getSchemaVer(row);
    if (ver >= 0) {
        return std::unique_ptr<RowReader>(
            new RowReaderV1(std::move(row), schemaMan->getTagSchema(space, tag, ver)));
    }

    // Invalid data
    // TODO We need a better error handler here
    LOG(FATAL) << "Invalid schema version in the row data!";
}


// static
std::unique_ptr<RowReader> RowReader::getEdgePropReader(
        meta::SchemaManager* schemaMan,
        GraphSpaceID space,
        EdgeType edge,
        std::string row) {
    CHECK_NOTNULL(schemaMan);
    int32_t ver = getSchemaVer(row);
    if (ver >= 0) {
        return std::unique_ptr<RowReader>(
            new RowReaderV1(st::move(row), schemaMan->getEdgeSchema(space, edge, ver)));
    }

    // Invalid data
    // TODO We need a better error handler here
    LOG(FATAL) << "Invalid schema version in the row data!";
}
*/

// static
std::unique_ptr<RowReader> RowReader::getRowReader(
        const meta::SchemaProviderIf* schema,
        folly::StringPiece row) {
    SchemaVer schemaVer;
    int32_t readerVer;
    getVersions(row, schemaVer, readerVer);
    CHECK_EQ(schemaVer, schema->getVersion());

    if (readerVer == 1) {
        return std::make_unique<RowReaderV1>(schema, std::move(row));
    } else {
        return std::make_unique<RowReaderV2>(schema, std::move(row));
    }
}


// static
void RowReader::getVersions(const folly::StringPiece& row,
                            SchemaVer& schemaVer,
                            int32_t& readerVer) {
    size_t index = 0;
    if (row.empty()) {
        LOG(WARNING) << "Row data is empty, so there is no version info";
        schemaVer = 0;
        readerVer = 2;
        return;
    }

    readerVer = ((row[index] & 0x18) >> 3) + 1;

    size_t verBytes = 0;
    if (readerVer == 1) {
        // The first three bits indicate the number of bytes for the
        // schema version. If the number is zero, no schema version
        // presents
        verBytes = row[index++] >> 5;
    } else if (readerVer == 2) {
        // The last three bits indicate the number of bytes for the
        // schema version. If the number is zero, no schema version
        // presents
        verBytes = row[index++] & 0x07;
    } else {
        LOG(FATAL) << "Invalid reader version";
    }

    schemaVer = 0;
    if (verBytes > 0) {
        if (verBytes + 1 > row.size()) {
            // Data is too short
            LOG(FATAL) << "Row data is too short: " << toHexStr(row);
        }
        // Schema Version is stored in Little Endian
        memcpy(reinterpret_cast<void*>(&schemaVer), &row[index], verBytes);
    }

    return;
}

}  // namespace nebula
