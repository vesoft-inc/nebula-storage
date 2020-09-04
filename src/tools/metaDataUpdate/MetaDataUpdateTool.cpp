/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/FileUtils.h"
#include "tools/metaDataUpdate/MetaDataUpdate.h"
#include "tools/metaDataUpdate/oldThrift/MetaServiceUtilsV1.h"

DEFINE_string(meta_data_path, "", "meta data path");
DEFINE_bool(print_info, false, "enable to print the rewrite data");

int main(int argc, char *argv[]) {
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    if (!nebula::fs::FileUtils::exist(FLAGS_meta_data_path)) {
        LOG(ERROR) << FLAGS_meta_data_path << " is not exist.";
        return 1;
    }

    auto newDataPath = nebula::fs::FileUtils::dirname(FLAGS_meta_data_path.c_str()) + "/v2.0_data";
    if (!nebula::fs::FileUtils::exist(newDataPath)) {
        LOG(WARNING) << newDataPath << " is not exist, remove it.";
        if (!nebula::fs::FileUtils::remove(newDataPath.c_str(), true)) {
            LOG(ERROR) << "Delete " << newDataPath << " failed.";
            return 1;
        }
    }

    // copy the data path to rewrite
    auto copyCmd = folly::stringPrintf("cp -r %s/* %s",
                                       FLAGS_meta_data_path.c_str(),
                                       newDataPath.c_str());
    std::system(copyCmd.c_str());

    auto dataPath = folly::stringPrintf("%s/nebula/0/data", newDataPath.c_str());
    LOG(INFO) << "The new data path is " << dataPath;

    rocksdb::WriteOptions options;
    nebula::meta::MetaDataUpdate updater;
    auto status = updater.initDB(dataPath);
    if (!status.ok()) {
        LOG(ERROR) << "InitDB from `" << FLAGS_meta_data_path << "' failed: " << status;
        return 1;
    }
    auto iter = updater.getDbIter();
    if (!iter) {
        LOG(ERROR) << "Get nullptr iter from rocksdb.";
        return 1;
    }

    iter->SeekToFirst();
    while (iter->Valid()) {
        auto key = folly::StringPiece(iter->key().data(), iter->key().size());
        auto val = folly::StringPiece(iter->value().data(), iter->value().size());

        if (key.startsWith(nebula::oldmeta::kSpacesTable)) {
            if (FLAGS_print_info) { updater.printSpaces(val); }
            status = updater.rewriteSpaces(key, val);
        } else if (key.startsWith(nebula::oldmeta::kPartsTable)) {
            if (FLAGS_print_info) { updater.printParts(key, val); }
            status = updater.rewriteParts(key, val);
        } else if (key.startsWith(nebula::oldmeta::kHostsTable)) {
            if (FLAGS_print_info) { updater.printHosts(key, val); }
            status = updater.rewriteHosts(key, val);
        } else if (key.startsWith(nebula::oldmeta::kLeadersTable)) {
            if (FLAGS_print_info) { updater.printLeaders(key); }
            status = updater.rewriteLeaders(key, val);
        } else if (key.startsWith(nebula::oldmeta::kTagsTable)
                   || key.startsWith(nebula::oldmeta::kEdgesTable)) {
            if (FLAGS_print_info) { updater.printSchemas(val); }
            status = updater.rewriteSchemas(key, val);
        } else if (key.startsWith(nebula::oldmeta::kIndexesTable)) {
            if (FLAGS_print_info) { updater.printIndexes(val); }
            status = updater.rewriteIndexes(key, val);
        } else if (key.startsWith(nebula::oldmeta::kConfigsTable)) {
            if (FLAGS_print_info) { updater.printConfigs(key, val); }
            status = updater.rewriteConfigs(key, val);
        } else if (key.startsWith(nebula::oldmeta::kDefaultTable)) {
            status = updater.deleteDefault(key);
        }
        if (!status.ok()) {
            LOG(ERROR) << status;
            // remove the new data directory
            if (!nebula::fs::FileUtils::remove(newDataPath.c_str(), true)) {
                LOG(ERROR) << "Delete " << newDataPath << " failed.";
                return 1;
            }
        }
        iter->Next();
    }
    return 0;
}
