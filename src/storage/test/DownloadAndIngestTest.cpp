/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "mock/MockCluster.h"
#include "storage/admin/AdminTaskManager.h"

#include <gtest/gtest.h>

namespace nebula {
namespace storage {

class DownloadAndIngestTest : public ::testing::Test {
protected:
    static void SetUpTestCase() {
        LOG(INFO) << "SetUp Download And Ingest TestCase";
        rootPath_ = std::make_unique<fs::TempDir>("/tmp/DownloadAndIngestTest.XXXXXX");
        cluster_ = std::make_unique<nebula::mock::MockCluster>();
        cluster_->initStorageKV(rootPath_->path());
        env_ = cluster_->storageEnv_.get();
        manager_ = AdminTaskManager::instance();
        manager_->init();
    }

    static void TearDownTestCase() {
        LOG(INFO) << "TearDown Download And Ingest TestCase";
        manager_->shutdown();
        cluster_.reset();
        rootPath_.reset();
    }

    void SetUp() override {}

    void TearDown() override {}

    static StorageEnv* env_;
    static AdminTaskManager* manager_;

private:
    static std::unique_ptr<fs::TempDir> rootPath_;
    static std::unique_ptr<nebula::mock::MockCluster> cluster_;
};

StorageEnv* DownloadAndIngestTest::env_{nullptr};
AdminTaskManager* DownloadAndIngestTest::manager_{nullptr};
std::unique_ptr<fs::TempDir> DownloadAndIngestTest::rootPath_{nullptr};
std::unique_ptr<nebula::mock::MockCluster> DownloadAndIngestTest::cluster_{nullptr};

TEST_F(DownloadAndIngestTest, NormalCondition) {
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
