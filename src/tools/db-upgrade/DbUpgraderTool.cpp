/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "tools/db-upgrade/DbUpgrader.h"
#include "kvstore/RocksEngineConfig.h"


DECLARE_string(rocksdb_column_family_options);

void printHelp() {
    fprintf(stderr,
           R"(  ./db_upgrade --src_db_path=<path to rocksdb> --dst_db_path=<path to rocksdb>  --upgrade_meta_server=<ip:port,...>

desc:
        This tool is used to upgrade data from nebula 1.0 or the previous versions of nebula 2.0 RC
        to nebula 2.0 GA version.

required:
       --src_db_path=<path to rocksdb>
         Source data path(data_path in storage 1.0 conf) to the rocksdb data directory.
         This is an absolute path, multi paths should be split by comma.
         If nebula 1.0 was installed in /usr/local/nebula,
         the db_path would be /usr/local/nebula/data/storage
         Default: ""

       --dst_db_path=<path to rocksdb>
         Destination data path(data_path in storage 2.0 conf) to the rocksdb data directory.
         This is an absolute path, multi paths should be split by comma.
         If nebula 2.0 was installed in /usr/local/nebulav2,
         the db_path would be /usr/local/nebulav2/data/storage
         Default: ""

         note:
         The number of paths in src_db_path is equal to the number of paths in dst_db_path, and
         src_db_path and dst_db_path must be different.

       --upgrade_meta_server=<ip:port,...>
         A list of meta severs' ip:port seperated by comma.
         Default: 127.0.0.1:45500

 optional:
       --write_batch_num=<N>
         The size of the batch written to rocksdb.
         Default: 100

       --update_v1=<true|false>
         This tool can only upgrade 1.0 data or 2.0 rc data.
         The data to be upgraded is 1.0 when true.
         he data to be upgraded is 2.0 rc when false.
         Default: true

       --compactions=<true|false>
         When the data upgrade finished, whether to compact all data.
         Default: true
)");
}


void printParams() {
    std::cout << "===========================PARAMS============================\n";
    std::cout << "meta server: " << FLAGS_upgrade_meta_server << "\n";
    std::cout << "source data path: " << FLAGS_src_db_path << "\n";
    std::cout << "destination data path: " << FLAGS_dst_db_path << "\n";
    std::cout << "The size of the batch written: " << FLAGS_write_batch_num << "\n";
    std::cout << "upgrade data version variable(true is v1.0, false is v2 rc): "
              << FLAGS_update_v1 << "\n";
    std::cout << "whether to compact all data: " << FLAGS_compactions << "\n";
    std::cout << "===========================PARAMS============================\n\n";
}


int main(int argc, char *argv[]) {
    // When begin to upgrade the data, close compaction
    // When upgrade finished, manually perform compaction.
    FLAGS_rocksdb_column_family_options = R"({
            "disable_auto_compactions":"true"
    })";

    if (argc == 1) {
        printHelp();
        return EXIT_FAILURE;
    } else {
        folly::init(&argc, &argv, true);
    }

    google::SetStderrLogging(google::INFO);

    printParams();

    // handle arguments
    LOG(INFO) << "Prepare phase begin";
    if (FLAGS_src_db_path.empty() || FLAGS_dst_db_path.empty()) {
        LOG(ERROR) << "Source data path(1.0) or destination data path(2.0) should not empty.";
        return EXIT_FAILURE;
    }

    std::vector<std::string> srcPaths;
    folly::split(",", FLAGS_src_db_path, srcPaths, true);
    std::transform(srcPaths.begin(), srcPaths.end(), srcPaths.begin(), [](auto& p) {
        return folly::trimWhitespace(p).str();
    });
    if (srcPaths.empty()) {
        LOG(ERROR) << "Bad src data_path format: " << FLAGS_src_db_path;
        return EXIT_FAILURE;
    }

    std::vector<std::string> dstPaths;
    folly::split(",", FLAGS_dst_db_path, dstPaths, true);
    std::transform(dstPaths.begin(), dstPaths.end(), dstPaths.begin(), [](auto& p) {
        return folly::trimWhitespace(p).str();
    });
    if (dstPaths.empty()) {
        LOG(ERROR) << "Bad dst data_path format: " << FLAGS_dst_db_path;
        return EXIT_FAILURE;
    }

    if (srcPaths.size() != dstPaths.size()) {
        LOG(ERROR) << "The size of src data paths is not equal the size of dst data paths";
        return EXIT_FAILURE;
    }

    auto addrs = nebula::network::NetworkUtils::toHosts(FLAGS_upgrade_meta_server);
    if (!addrs.ok()) {
        LOG(ERROR) << "Get host address failed " << FLAGS_upgrade_meta_server;
        return EXIT_FAILURE;
    }

    auto ioExecutor = std::make_shared<folly::IOThreadPoolExecutor>(1);
    nebula::meta::MetaClientOptions options;
    options.skipConfig_ = true;
    auto metaClient = std::make_unique<nebula::meta::MetaClient>(ioExecutor,
                                                     std::move(addrs.value()),
                                                     options);
    CHECK_NOTNULL(metaClient);
    if (!metaClient->waitForMetadReady(1)) {
        LOG(ERROR) << "Meta is not ready: " << FLAGS_upgrade_meta_server;
        return EXIT_FAILURE;
    }

    auto schemaMan = nebula::meta::ServerBasedSchemaManager::create(metaClient.get());
    auto indexMan = nebula::meta::ServerBasedIndexManager::create(metaClient.get());
    CHECK_NOTNULL(schemaMan);
    CHECK_NOTNULL(indexMan);
    LOG(INFO) << "Prepare phase end";

    // Upgrade data
    LOG(INFO) << "Upgrade phase bengin";
    std::vector<std::thread> threads;
    for (size_t i = 0; i < srcPaths.size(); i++) {
        threads.emplace_back(std::thread([mclient = metaClient.get(),
                                          sMan = schemaMan.get(),
                                          iMan = indexMan.get(),
                                          srcPath = srcPaths[i],
                                          dstPath = dstPaths[i]] {
            LOG(INFO) << "Upgrade from path " << srcPath << " to path "
                      << dstPath << " begin";
            nebula::storage::DbUpgrader upgrader;
            auto ret = upgrader.init(mclient, sMan, iMan, srcPath, dstPath);
            if (!ret.ok()) {
                LOG(ERROR) << "Upgrader init failed from " << srcPath << " to path "
                           << dstPath;
                return;
            }
            upgrader.run();
            LOG(INFO) << "Upgrade from path " << srcPath << " to path "
                      << dstPath << " end";
        }));
    }

    // Wait for all threads to finish
    for (auto& t : threads) {
        t.join();
    }

    LOG(INFO) << "Upgrade phase end";
    return 0;
}

