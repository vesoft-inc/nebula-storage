/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "tools/db-upgrade/DbUpgrader.h"


void printHelp() {
    fprintf(stderr,
           R"(  ./db_upgrade --upgrade_db_path=<path to rocksdb> --upgrade_meta_server=<ip:port,...>

required:
       --upgrade_db_path=<path to rocksdb>
         Path to the rocksdb data directory. If nebula was installed in /usr/local/nebula,
         the db_path would be /usr/local/nebula/data/storage/
         note: db_path not /usr/local/nebula/data/storage/nebula/
         Default: ./

       --upgrade_meta_server=<ip:port,...>
         A list of meta severs' ip:port seperated by comma.
         Default: 127.0.0.1:45500
)");
}


void printParams() {
    std::cout << "===========================PARAMS============================\n";
    std::cout << "meta server: " << FLAGS_upgrade_meta_server << "\n";
    std::cout << "path: " << FLAGS_upgrade_db_path << "\n";
    std::cout << "===========================PARAMS============================\n\n";
}


int main(int argc, char *argv[]) {
    if (argc == 2) {
        printHelp();
        return EXIT_FAILURE;
    } else {
        folly::init(&argc, &argv, true);
    }

    google::SetStderrLogging(google::INFO);

    printParams();

    nebula::storage::DbUpgrader upgrader;
    auto status = upgrader.init();
    if (!status.ok()) {
      std::cerr << "Error: " << status << "\n\n";
      return EXIT_FAILURE;
    }
    upgrader.run();
    return 0;
}

