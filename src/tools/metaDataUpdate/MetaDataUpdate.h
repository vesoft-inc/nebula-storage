/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef TOOLS_METADATAUPDATETOOL_METADATAUPDATE_H_
#define TOOLS_METADATAUPDATETOOL_METADATAUPDATE_H_

#include <rocksdb/db.h>
#include "common/base/Base.h"
#include "common/base/Status.h"
#include "common/interface/gen-cpp2/meta_types.h"
#include "tools/metaDataUpdate/oldThrift/gen-cpp2/old_meta_types.h"


namespace nebula {
namespace meta {

class MetaDataUpdate {
public:
    Status initDB(const std::string &dataPath);

    rocksdb::Iterator* getDbIter() const {
        rocksdb::ReadOptions options;
        return db_->NewIterator(options);
    }

    Status rewriteHosts(const folly::StringPiece &key, const folly::StringPiece &val);

    Status rewriteSpaces(const folly::StringPiece &key, const folly::StringPiece &val);

    Status rewriteParts(const folly::StringPiece &key, const folly::StringPiece &val);

    Status rewriteLeaders(const folly::StringPiece &key, const folly::StringPiece &val);

    Status rewriteSchemas(const folly::StringPiece &key, const folly::StringPiece &val);

    Status rewriteIndexes(const folly::StringPiece &key, const folly::StringPiece &val);

    Status rewriteConfigs(const folly::StringPiece &key, const folly::StringPiece &val);

    Status deleteDefault(const folly::StringPiece &key);

    void printHosts(const folly::StringPiece &key, const folly::StringPiece &val);

    void printSpaces(const folly::StringPiece &val);

    void printParts(const folly::StringPiece &key, const folly::StringPiece &val);

    void printLeaders(const folly::StringPiece &key);

    void printSchemas(const folly::StringPiece &val);

    void printIndexes(const folly::StringPiece &val);

    void printConfigs(const folly::StringPiece &key, const folly::StringPiece &val);

private:
    Status put(const folly::StringPiece &key, const folly::StringPiece &val) {
        rocksdb::WriteOptions options;
        options.disableWAL = false;
        rocksdb::Status status = db_->Put(options, key.str(), val.str());
        if (!status.ok()) {
            return Status::Error("Rocksdb put failed");
        }

        rocksdb::FlushOptions fOptions;
        status = db_->Flush(fOptions);
        if (!status.ok()) {
            return Status::Error("Rocksdb flush failed");
        }
        return Status::OK();
    }

    Status remove(const folly::StringPiece& key) {
        rocksdb::WriteOptions options;
        options.disableWAL = false;;
        auto status = db_->Delete(options, key.str());
        if (!status.ok()) {
            return Status::Error("Rocksdb delete failed");
        }
        rocksdb::FlushOptions fOptions;
        status = db_->Flush(fOptions);
        if (!status.ok()) {
            return Status::Error("Rocksdb flush failed");
        }
        return Status::OK();
    }

    Status convertToNewColumns(const std::vector<oldmeta::cpp2::ColumnDef> &oldCols,
                               std::vector<cpp2::ColumnDef> &newCols);

private:
    std::unique_ptr<rocksdb::DB> db_;
    rocksdb::Options options_;
};

}  // namespace meta
}  // namespace nebula
#endif  // TOOLS_METADATAUPDATETOOL_METADATAUPDATE_H_
