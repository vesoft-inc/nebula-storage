/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <rocksdb/db.h>
#include <glog/logging.h>

#include "common/base/Base.h"
#include "common/fs/FileUtils.h"
#include "meta/ActiveHostsMan.h"
#include "meta/MetaServiceUtils.h"

DEFINE_string(path, "", "meta rocksdb instance path");
DEFINE_bool(detailed, false, "print out the detail about meta data");

namespace nebula {
namespace storage {

class MetaDump {
public:
    MetaDump() {}

    ~MetaDump() {}

    MetaDump(const MetaDump&) = delete;

    MetaDump& operator=(const MetaDump&) = delete;

    MetaDump(MetaDump&&) = delete;

    MetaDump& operator=(MetaDump&&) = delete;

    void processMetaIndex(const std::string& path) {
        rocksdb::DB* db = nullptr;
        rocksdb::Options options;
        auto status = rocksdb::DB::OpenForReadOnly(options, path, &db);
        CHECK(status.ok()) << status.ToString();
        if (!status.ok()) {
            LOG(ERROR) << "Open failed: " << status.ToString();
            return;
        }

        rocksdb::ReadOptions roptions;
        auto iter = db->NewIterator(rocksdb::ReadOptions());
        iter->Seek(meta::MetaServiceUtils::metaIndexPrefix());
        while (iter->Valid()) {
            auto key = folly::StringPiece(iter->key().data(), iter->key().size());
            auto value = folly::StringPiece(iter->value().data(), iter->value().size());
            if (key.startsWith("__index__")) {
                printMetaIndex(key, value);
            }
            iter->Next();
        }

        delete iter;
        db->Close();
        delete db;
    }

    void scan(const std::string& path) {
        LOG(INFO) << "Reading RocksDB instance on: " << path;
        rocksdb::DB* db = nullptr;
        rocksdb::Options options;
        auto status = rocksdb::DB::OpenForReadOnly(options, path, &db);
        CHECK(status.ok()) << status.ToString();
        if (!status.ok()) {
            LOG(ERROR) << "Open failed: " << status.ToString();
            return;
        }

        rocksdb::Iterator* iter = db->NewIterator(rocksdb::ReadOptions());
        if (!iter) {
            LOG(ERROR) << "Null iterator!";
            return;
        }
        iter->SeekToFirst();
        while (iter->Valid()) {
            auto key = folly::StringPiece(iter->key().data(), iter->key().size());
            auto value = folly::StringPiece(iter->value().data(), iter->value().size());

            // if (key.startsWith("__spaces__")) {
            if (key.startsWith(meta::kSpacesTable)) {
                printSpace(key, value);
            } else if (FLAGS_detailed && key.startsWith(meta::kPartsTable)) {
                printPart(key, value);
            } else if (FLAGS_detailed && key.startsWith(meta::kHostsTable)) {
                printHost(key, value);
            } else if (key.startsWith(meta::kTagsTable)) {
                printSchema(key, value, "TAG");
            } else if (key.startsWith(meta::kEdgesTable)) {
                printSchema(key, value, "EDGE");
            } else if (key.startsWith(meta::kIndexTable)) {
                printIndex(key, value);
            } else if (FLAGS_detailed && key.startsWith(meta::kIndexStatusTable)) {
                printIndexStatus(key, value);
            } else if (key.startsWith(meta::kUsersTable)) {
                printUser(key, value);
            } else if (key.startsWith(meta::kRolesTable)) {
                printRole(key, value);
            } else if (key.startsWith(meta::kGroupsTable)) {
                printGroup(key, value);
            } else if (key.startsWith(meta::kZonesTable)) {
                printZone(key, value);
            }

            iter->Next();
        }

        delete iter;
        db->Close();
        delete db;
    }

private:
    void printMetaIndex(const folly::StringPiece& /*key*/,
                        const folly::StringPiece& value) {
        LOG(INFO) << "Meta Index: " << value;
    }

    void printSpace(const folly::StringPiece& /*key*/,
                    const folly::StringPiece& value) {
        auto spaceDesc = meta::MetaServiceUtils::parseSpace(value);
        LOG(INFO) << folly::stringPrintf("CREATE SPACE %s (partition_num=%d, replica_factor=%d)",
                                         spaceDesc.get_space_name().c_str(),
                                         spaceDesc.get_partition_num(),
                                         spaceDesc.get_replica_factor());
    }

    void printPart(const folly::StringPiece& key,
                   const folly::StringPiece& value) {
        int partSize = 0;

        auto space = meta::MetaServiceUtils::parsePartKeySpaceId(key);
        auto part = meta::MetaServiceUtils::parsePartKeyPartId(key);
        LOG(INFO) << "Space: " << space << " Part: " << part;
        auto addresses = meta::MetaServiceUtils::parsePartVal(value, partSize);
        for (auto address : addresses) {
            LOG(INFO) << "Host: " << address.host << " Port: " << address.port;
        }
        LOG(INFO) << folly::stringPrintf("%ld", addresses.size());
    }

    void printHost(const folly::StringPiece& key,
                   const folly::StringPiece& value) {
        auto host = meta::MetaServiceUtils::parseHostKey(key);
        auto info = meta::HostInfo::decode(value);
        LOG(INFO) << "Host: " << host.host << " Port: " << host.port
                  << " Role: " << roleToString(info.role_)
                  << " Git Info: " << info.gitInfoSha_;
    }

    void printSchema(const folly::StringPiece& /*key*/,
                     const folly::StringPiece& value,
                     const std::string& schemaType) {
        auto schema = meta::MetaServiceUtils::parseSchema(value);
        auto prop = schema.get_schema_prop();

        std::vector<std::string> fields;
        for (const auto& column : schema.get_columns()) {
            auto type = column.get_type().get_type();
            auto field = folly::stringPrintf("%s %s", column.get_name().c_str(),
                                             propertyTypeToString(type).c_str());
            fields.emplace_back(std::move(field));
        }

        auto name = meta::MetaServiceUtils::parseSchemaName(value);
        LOG(INFO) << folly::stringPrintf("CREATE %s %s (%s)",
                                         schemaType.c_str(),
                                         name.c_str(),
                                         folly::join(", ", fields).c_str());

        LOG(INFO) << "ttl_duration: " << prop.ttl_duration
                  << " ttl_col: " << prop.ttl_col;
    }

    void printIndex(const folly::StringPiece& /*key*/,
                    const folly::StringPiece& value) {
        auto indexItem = meta::MetaServiceUtils::parseIndex(value);
        LOG(INFO) << "CREATE Index: " << indexItem.get_index_name();

        for (auto& field : indexItem.get_fields()) {
            LOG(INFO) << "Field: " << field.name;
        }
    }

    void printIndexStatus(const folly::StringPiece& /*key*/,
                          const folly::StringPiece& /*value*/) {
    }

    void printUser(const folly::StringPiece& key,
                   const folly::StringPiece& value) {
        LOG(INFO) << "User: " << key << " " << value;
    }

    void printRole(const folly::StringPiece& key,
                   const folly::StringPiece& value) {
        auto account = meta::MetaServiceUtils::parseRoleUser(key);
        LOG(INFO) << "Account: " << account << " " << value;
    }

    void printGroup(const folly::StringPiece& /*key*/,
                    const folly::StringPiece& /*value*/) {
        LOG(INFO) << "";
    }

    void printZone(const folly::StringPiece& /*key*/,
                   const folly::StringPiece& /*value*/) {
    }

    std::string roleToString(meta::cpp2::HostRole role) {
        switch (role) {
            case meta::cpp2::HostRole::GRAPH:
                return "GRAPH";
            case meta::cpp2::HostRole::STORAGE:
                return "STORAGE";
            case meta::cpp2::HostRole::META:
                return "META";
            default:
                return "";
        }
    }

    std::string propertyTypeToString(meta::cpp2::PropertyType type) {
        switch (type) {
            case meta::cpp2::PropertyType::BOOL:
                return "BOOL";
            case meta::cpp2::PropertyType::INT8:
            case meta::cpp2::PropertyType::INT16:
            case meta::cpp2::PropertyType::INT32:
            case meta::cpp2::PropertyType::INT64:
            case meta::cpp2::PropertyType::VID:
                return "INT";
            case meta::cpp2::PropertyType::FLOAT:
            case meta::cpp2::PropertyType::DOUBLE:
                return "DOUBLE";
            case meta::cpp2::PropertyType::STRING:
            case meta::cpp2::PropertyType::FIXED_STRING:
                return "STRING";
            case meta::cpp2::PropertyType::TIMESTAMP:
                return "TIMESTAMP";
            case meta::cpp2::PropertyType::DATE:
                return "DATE";
            case meta::cpp2::PropertyType::DATETIME:
                return "DATETIME";
            case meta::cpp2::PropertyType::TIME:
                return "TIME";
            default:
                return "UNKNOWN";
        }
    }
};

}  // namespace storage
}  // namespace nebula

int main(int argc, char *argv[]) {
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    if (FLAGS_path.empty()) {
        LOG(ERROR) << "Specify the meta rocksdb's path";
        return -1;
    }

    if (!nebula::fs::FileUtils::exist(FLAGS_path)) {
        LOG(ERROR) << "Path: " << FLAGS_path << " not exist";
        return -1;
    }

    nebula::storage::MetaDump instance;
    instance.processMetaIndex(FLAGS_path);
    instance.scan(FLAGS_path);
    return 0;
}
