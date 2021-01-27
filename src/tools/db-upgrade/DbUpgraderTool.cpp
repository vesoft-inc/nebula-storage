/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "tools/db-upgrade/DbUpgrader.h"


void printHelp() {
    fprintf(stderr,
           R"(  ./db_upgrade --src_db_path=<path to rocksdb> --dst_db_path=<path to rocksdb>  --upgrade_meta_server=<ip:port,...>

required:
       --src_db_path=<path to rocksdb>
         Source data path(data_path in storage 1.0 conf) to the rocksdb data directory,
         multi paths should be split by comma.
         If nebula 1.0 was installed in /usr/local/nebula,
         the db_path would be /usr/local/nebula/data/storage
         note: db_path not /usr/local/nebula/data/storage/nebula
         Default: ""

       --dst_db_path=<path to rocksdb>
         Destination data path(data_path in storage 1.0 conf) to the rocksdb data directory,
         multi paths should be split by comma.
         If nebula 2.0  was installed in /usr/local/nebula,
         the db_path would be /usr/local/nebula/data/storage
         note: db_path not /usr/local/nebula/data/storage/nebula
         The number of paths in src_db_path is equal to the number of paths in dst_db_path.
         Default: ""

       --upgrade_meta_server=<ip:port,...>
         A list of meta severs' ip:port seperated by comma.
         Default: 127.0.0.1:45500
)");
}


void printParams() {
    std::cout << "===========================PARAMS============================\n";
    std::cout << "meta server: " << FLAGS_upgrade_meta_server << "\n";
    std::cout << "source data path: " << FLAGS_src_db_path << "\n";
    std::cout << "destination data path: " << FLAGS_dst_db_path << "\n";
    std::cout << "===========================PARAMS============================\n\n";
}


int main(int argc, char *argv[]) {
    if (argc == 3) {
        printHelp();
        return EXIT_FAILURE;
    } else {
        folly::init(&argc, &argv, true);
    }

    google::SetStderrLogging(google::INFO);

    printParams();

    // handle arguments
    LOG(INFO) << "Prepare begin ";
    if (FLAGS_src_db_path.empty() || FLAGS_dst_db_path.empty()) {
        LOG(ERROR) << "Source data path(1.0) or destination data path(2.0) should not empty";
        return EXIT_FAILURE;
    }

    std::vector<std::string> srcPaths;
    folly::split(",", FLAGS_src_db_path, srcPaths, true);
    std::transform(srcPaths.begin(), srcPaths.end(), srcPaths.begin(), [](auto& p) {
        return folly::trimWhitespace(p).str();
    });
    if (srcPaths.empty()) {
        LOG(ERROR) << "Bad src data_path format:" << FLAGS_src_db_path;
        return EXIT_FAILURE;
    }

    std::vector<std::string> dstPaths;
    folly::split(",", FLAGS_dst_db_path, dstPaths, true);
    std::transform(dstPaths.begin(), dstPaths.end(), dstPaths.begin(), [](auto& p) {
        return folly::trimWhitespace(p).str();
    });
    if (dstPaths.empty()) {
        LOG(ERROR) << "Bad dst data_path format:" << FLAGS_dst_db_path;
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
    LOG(INFO) << "Prepare end ";

    // Upgrade data
    LOG(INFO) << "Upgrade bengin.";
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
                LOG(ERROR) << "Upgrader init failed " << srcPath << " " <<  dstPath;
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

    LOG(INFO) << "Upgrade end.";
    return 0;
}

