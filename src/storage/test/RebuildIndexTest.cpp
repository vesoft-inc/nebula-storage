/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include <gtest/gtest.h>
#include "mock/MockCluster.h"
#include "mock/MockData.h"
#include "storage/admin/AdminTaskManager.h"
#include "storage/admin/RebuildTagIndexTask.h"
#include "storage/admin/RebuildEdgeIndexTask.h"
#include "storage/mutate/AddEdgesProcessor.h"
#include "storage/mutate/AddVerticesProcessor.h"
#include "storage/mutate/DeleteVerticesProcessor.h"
#include "storage/mutate/DeleteEdgesProcessor.h"
#include "storage/test/TestUtils.h"

namespace nebula {
namespace storage {

class RebuildIndexTest : public ::testing::Test {
protected:
    static void SetUpTestCase() {
        LOG(INFO) << "SetUp RebuildIndexTest TestCase";
        rootPath_ = std::make_unique<fs::TempDir>("/tmp/RebuildIndexTest.XXXXXX");
        cluster_ = std::make_unique<nebula::mock::MockCluster>();
        cluster_->initStorageKV(rootPath_->path());
        env_ = cluster_->storageEnv_.get();
        manager_ = AdminTaskManager::instance();
        manager_->init();
    }

    static void TearDownTestCase() {
        LOG(INFO) << "TearDown RebuildIndexTest TestCase";
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

StorageEnv* RebuildIndexTest::env_{nullptr};
AdminTaskManager* RebuildIndexTest::manager_{nullptr};
std::unique_ptr<fs::TempDir> RebuildIndexTest::rootPath_{nullptr};
std::unique_ptr<nebula::mock::MockCluster> RebuildIndexTest::cluster_{nullptr};

TEST_F(RebuildIndexTest, RebuildTagIndexOnlineWithDelete) {
    auto writer = std::make_unique<thread::GenericWorker>();
    EXPECT_TRUE(writer->start());

    auto deleteVertices = [&] () mutable {
        LOG(INFO) << "Start Background Writer & Delete is Running";
        cpp2::DeleteVerticesRequest req = mock::MockData::mockDeleteVerticesReq();
        auto* processor = DeleteVerticesProcessor::instance(RebuildIndexTest::env_, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
    };

    // Add Vertices
    auto* processor = AddVerticesProcessor::instance(RebuildIndexTest::env_, nullptr);
    cpp2::AddVerticesRequest req = mock::MockData::mockAddVerticesReq();
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());

    cpp2::TaskPara parameter;
    parameter.set_space_id(1);
    std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};
    parameter.set_parts(std::move(parts));
    std::vector<std::string> taskParameters = {"11", "online"};
    parameter.set_task_specfic_paras(std::move(taskParameters));

    cpp2::AddAdminTaskRequest request;
    request.set_cmd(meta::cpp2::AdminCmd::REBUILD_TAG_INDEX);
    request.set_job_id(1);
    request.set_task_id(11);
    request.set_para(std::move(parameter));

    auto callback = [](cpp2::ErrorCode) {};
    TaskContext context(request, callback);

    writer->addTask(deleteVertices).get();

    auto task = std::make_shared<RebuildTagIndexTask>(RebuildIndexTest::env_, std::move(context));
    manager_->addAsyncTask(task);

    // Wait for the task finished
    do {
        usleep(50);
    } while (!manager_->isFinished(context.jobId_, context.taskId_));

    LOG(INFO) << "Check rebuild tag index...";
    for (auto& key : mock::MockData::mockPlayerIndexKeys()) {
        std::string value;
        auto code = RebuildIndexTest::env_->kvstore_->get(1, key.first, key.second, &value);
        if (code == kvstore::ResultCode::SUCCEEDED) {
            LOG(INFO) << "Check Key " << key.first << " " << key.second;
        }
        EXPECT_EQ(kvstore::ResultCode::ERR_KEY_NOT_FOUND, code);
    }

    RebuildIndexTest::env_->rebuildIndexGuard_->clear();
    writer->stop();
}

TEST_F(RebuildIndexTest, RebuildTagIndexOnlineWithAppend) {
    auto writer = std::make_unique<thread::GenericWorker>();
    EXPECT_TRUE(writer->start());

    auto appendVertices = [&] () mutable {
        LOG(INFO) << "Start Background Writer & Append is Running";
        auto* processor = AddVerticesProcessor::instance(RebuildIndexTest::env_, nullptr);
        cpp2::AddVerticesRequest req = mock::MockData::mockAddVerticesReq();
        auto fut = processor->getFuture();
        processor->process(req);
        std::move(fut).get();
    };

    // Add Vertices
    auto* processor = AddVerticesProcessor::instance(RebuildIndexTest::env_, nullptr);
    cpp2::AddVerticesRequest req = mock::MockData::mockAddVerticesReq(true);
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());

    cpp2::TaskPara parameter;
    parameter.set_space_id(1);
    std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};
    parameter.set_parts(std::move(parts));
    std::vector<std::string> taskParameters = {"11", "online"};
    parameter.set_task_specfic_paras(std::move(taskParameters));

    cpp2::AddAdminTaskRequest request;
    request.set_cmd(meta::cpp2::AdminCmd::REBUILD_TAG_INDEX);
    request.set_job_id(2);
    request.set_task_id(12);
    request.set_para(std::move(parameter));

    auto callback = [](cpp2::ErrorCode) {};
    TaskContext context(request, callback);

    auto task = std::make_shared<RebuildTagIndexTask>(RebuildIndexTest::env_, std::move(context));
    manager_->addAsyncTask(task);
    writer->addTask(appendVertices).get();

    // Wait for the task finished
    do {
        usleep(50);
    } while (!manager_->isFinished(context.jobId_, context.taskId_));

    LOG(INFO) << "Check rebuild tag index...";
    for (auto& key : mock::MockData::mockPlayerIndexKeys(true)) {
        std::string value;
        auto code = RebuildIndexTest::env_->kvstore_->get(1, key.first, key.second, &value);
        EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, code);
    }

    RebuildIndexTest::env_->rebuildIndexGuard_->clear();
    writer->stop();
}

TEST_F(RebuildIndexTest, RebuildTagIndexOffline) {
    // Add Vertices
    auto* processor = AddVerticesProcessor::instance(RebuildIndexTest::env_, nullptr);
    cpp2::AddVerticesRequest req = mock::MockData::mockAddVerticesReq();
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());

    cpp2::TaskPara parameter;
    parameter.set_space_id(1);
    std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};
    parameter.set_parts(std::move(parts));
    std::vector<std::string> taskParameters = {"11", "offline"};
    parameter.set_task_specfic_paras(std::move(taskParameters));

    cpp2::AddAdminTaskRequest request;
    request.set_cmd(meta::cpp2::AdminCmd::REBUILD_TAG_INDEX);
    request.set_job_id(3);
    request.set_task_id(13);
    request.set_para(std::move(parameter));

    auto callback = [](cpp2::ErrorCode) {};
    TaskContext context(request, callback);

    auto task = std::make_shared<RebuildTagIndexTask>(RebuildIndexTest::env_, std::move(context));
    manager_->addAsyncTask(task);

    // Wait for the task finished
    do {
        usleep(50);
    } while (!manager_->isFinished(context.jobId_, context.taskId_));

    // Check the result
    LOG(INFO) << "Check rebuild tag index...";
    for (auto& key : mock::MockData::mockPlayerIndexKeys()) {
        std::string value;
        auto code = RebuildIndexTest::env_->kvstore_->get(1, key.first, key.second, &value);
        EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, code);
    }

    RebuildIndexTest::env_->rebuildIndexGuard_->clear();
}

TEST_F(RebuildIndexTest, RebuildEdgeIndexOnlineWithDelete) {
    auto writer = std::make_unique<thread::GenericWorker>();
    EXPECT_TRUE(writer->start());

    auto deleteEdges = [&] () mutable {
        cpp2::DeleteEdgesRequest req = mock::MockData::mockDeleteEdgesReq();
        auto* processor = DeleteEdgesProcessor::instance(RebuildIndexTest::env_, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
    };

    // Add Edges
    auto* processor = AddEdgesProcessor::instance(RebuildIndexTest::env_, nullptr);
    cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq();
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());

    cpp2::TaskPara parameter;
    parameter.set_space_id(1);
    std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};
    parameter.set_parts(std::move(parts));
    std::vector<std::string> taskParameters = {"12", "online"};
    parameter.set_task_specfic_paras(std::move(taskParameters));

    cpp2::AddAdminTaskRequest request;
    request.set_cmd(meta::cpp2::AdminCmd::REBUILD_EDGE_INDEX);
    request.set_job_id(4);
    request.set_task_id(14);
    request.set_para(std::move(parameter));

    auto callback = [](cpp2::ErrorCode) {};
    TaskContext context(request, callback);

    writer->addTask(deleteEdges).get();

    auto task = std::make_shared<RebuildEdgeIndexTask>(RebuildIndexTest::env_, std::move(context));
    manager_->addAsyncTask(task);

    // Wait for the task finished
    do {
        usleep(50);
    } while (!manager_->isFinished(context.jobId_, context.taskId_));

    // Check the result
    LOG(INFO) << "Check rebuild edge index...";
    for (auto& key : mock::MockData::mockServeIndexKeys()) {
        std::string value;
        auto code = RebuildIndexTest::env_->kvstore_->get(1, key.first, key.second, &value);
        if (code == kvstore::ResultCode::SUCCEEDED) {
            LOG(INFO) << "Check Key " << key.first << " " << key.second;
        }
        EXPECT_EQ(kvstore::ResultCode::ERR_KEY_NOT_FOUND, code);
    }

    RebuildIndexTest::env_->rebuildIndexGuard_->clear();
    writer->stop();
}

TEST_F(RebuildIndexTest, RebuildEdgeIndexOnlineWithAppend) {
    auto writer = std::make_unique<thread::GenericWorker>();
    EXPECT_TRUE(writer->start());

    auto appendEdges = [&] () mutable {
        auto* processor = AddEdgesProcessor::instance(RebuildIndexTest::env_, nullptr);
        cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq(true);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
    };
    writer->addTask(appendEdges);

    // Add Edges
    auto* processor = AddEdgesProcessor::instance(RebuildIndexTest::env_, nullptr);
    cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq();
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());

    cpp2::TaskPara parameter;
    parameter.set_space_id(1);
    std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};
    parameter.set_parts(std::move(parts));
    std::vector<std::string> taskParameters = {"12", "online"};
    parameter.set_task_specfic_paras(std::move(taskParameters));

    cpp2::AddAdminTaskRequest request;
    request.set_cmd(meta::cpp2::AdminCmd::REBUILD_EDGE_INDEX);
    request.set_job_id(5);
    request.set_task_id(15);
    request.set_para(std::move(parameter));

    auto callback = [](cpp2::ErrorCode) {};
    TaskContext context(request, callback);
    auto task = std::make_shared<RebuildEdgeIndexTask>(RebuildIndexTest::env_, std::move(context));
    manager_->addAsyncTask(task);

    // Wait for the task finished
    do {
        usleep(50);
    } while (!manager_->isFinished(context.jobId_, context.taskId_));

    // Check the result
    LOG(INFO) << "Check rebuild tag index...";
    for (auto& key : mock::MockData::mockServeIndexKeys()) {
        std::string value;
        auto code = RebuildIndexTest::env_->kvstore_->get(1, key.first, key.second, &value);
        EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, code);
    }

    RebuildIndexTest::env_->rebuildIndexGuard_->clear();
    writer->stop();
}

TEST_F(RebuildIndexTest, RebuildEdgeIndexOffline) {
    // Add Edges
    auto* processor = AddEdgesProcessor::instance(RebuildIndexTest::env_, nullptr);
    cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq();
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());

    cpp2::TaskPara parameter;
    parameter.set_space_id(1);
    std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};
    parameter.set_parts(std::move(parts));
    std::vector<std::string> taskParameters = {"12", "offline"};
    parameter.set_task_specfic_paras(std::move(taskParameters));

    cpp2::AddAdminTaskRequest request;
    request.set_cmd(meta::cpp2::AdminCmd::REBUILD_EDGE_INDEX);
    request.set_job_id(6);
    request.set_task_id(16);
    request.set_para(std::move(parameter));

    auto callback = [](cpp2::ErrorCode) {};
    TaskContext context(request, callback);
    auto task = std::make_shared<RebuildEdgeIndexTask>(RebuildIndexTest::env_, std::move(context));
    manager_->addAsyncTask(task);

    // Wait for the task finished
    do {
        usleep(50);
    } while (!manager_->isFinished(context.jobId_, context.taskId_));

    // Check the result
    LOG(INFO) << "Check rebuild edge index...";
    for (auto& key : mock::MockData::mockServeIndexKeys()) {
        std::string value;
        auto code = RebuildIndexTest::env_->kvstore_->get(1, key.first, key.second, &value);
        EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, code);
    }
}

}  // namespace storage
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
