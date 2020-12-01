/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/webservice/Router.h"
#include "common/webservice/WebService.h"
#include "common/webservice/test/TestUtils.h"
#include "common/http/HttpClient.h"
#include <gtest/gtest.h>
#include "storage/http/StorageHttpAdminHandler.h"
#include "storage/test/TestUtils.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"

namespace nebula {
namespace storage {

class StorageHttpAdminHandlerTestEnv : public ::testing::Environment {
public:
    void SetUp() override {
        FLAGS_ws_http_port = 0;
        FLAGS_ws_h2_port = 0;
        rootPath_ = std::make_unique<fs::TempDir>("/tmp/StorageHttpAdminHandler.XXXXXX");
        cluster_ = std::make_unique<mock::MockCluster>();
        cluster_->initStorageKV(rootPath_->path());

        VLOG(1) << "Starting web service...";
        webSvc_ = std::make_unique<WebService>();
        auto& router = webSvc_->router();
        router.get("/admin").handler([this](nebula::web::PathParams&&) {
            return new storage::StorageHttpAdminHandler(cluster_->storageEnv_->schemaMan_,
                                                        cluster_->storageEnv_->kvstore_);
        });
        auto status = webSvc_->start();
        ASSERT_TRUE(status.ok()) << status;
    }

    void TearDown() override {
        cluster_.reset();
        webSvc_.reset();
        rootPath_.reset();
        VLOG(1) << "Web service stopped";
    }

protected:
    std::unique_ptr<mock::MockCluster>   cluster_{nullptr};
    std::unique_ptr<WebService>          webSvc_{nullptr};
    std::unique_ptr<fs::TempDir>         rootPath_{nullptr};
};


TEST(StoragehHttpAdminHandlerTest, AdminTest) {
    {
        auto url = "/admin";
        auto request = folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(),
                                           FLAGS_ws_http_port, url);
        auto resp = http::HttpClient::get(request);
        ASSERT_TRUE(resp.ok());
        ASSERT_EQ(0, resp.value().find("Space should not be empty"));
    }
    {
        auto url = "/admin?space=xx";
        auto request = folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(),
                                           FLAGS_ws_http_port, url);
        auto resp = http::HttpClient::get(request);
        ASSERT_TRUE(resp.ok());
        ASSERT_EQ(0, resp.value().find("Op should not be empty"));
    }
    {
        auto url = "/admin?space=xx&op=yy";
        auto request = folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(),
                                           FLAGS_ws_http_port, url);
        auto resp = http::HttpClient::get(request);
        ASSERT_TRUE(resp.ok());
        ASSERT_EQ(0, resp.value().find("Can't find space xx"));
    }
    {
        auto url = "/admin?space=1&op=yy";
        auto request = folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(),
                                           FLAGS_ws_http_port, url);
        auto resp = http::HttpClient::get(request);
        ASSERT_TRUE(resp.ok());
        ASSERT_EQ(0, resp.value().find("Unknown operation yy"));
    }
    {
        auto url = "/admin?space=1&op=flush";
        auto request = folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(),
                                           FLAGS_ws_http_port, url);
        auto resp = http::HttpClient::get(request);
        ASSERT_TRUE(resp.ok());
        ASSERT_EQ("ok", resp.value());
    }
    {
        auto url = "/admin?space=1&op=compact";
        auto request = folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(),
                                           FLAGS_ws_http_port, url);
        auto resp = http::HttpClient::get(request);
        ASSERT_TRUE(resp.ok());
        ASSERT_EQ("ok", resp.value());
    }
}

}  // namespace storage
}  // namespace nebula


int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    ::testing::AddGlobalTestEnvironment(new nebula::storage::StorageHttpAdminHandlerTestEnv());

    return RUN_ALL_TESTS();
}
