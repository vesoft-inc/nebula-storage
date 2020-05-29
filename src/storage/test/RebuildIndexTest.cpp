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

TEST(RebuildIndexTest, RebuildTagIndexOffline) {
    fs::TempDir rootPath("/tmp/RebuildTagIndexOfflineTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();

    // Add Vertices
    auto* processor = AddVerticesProcessor::instance(env, nullptr);
    cpp2::AddVerticesRequest req = mock::MockData::mockAddVerticesReq();
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());
    checkAddVerticesData(req, env, 81, 0);

    auto manager = AdminTaskManager::instance();
    EXPECT_TRUE(manager->init());

    cpp2::TaskPara parameter;
    parameter.set_space_id(1);
    std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};
    parameter.set_parts(std::move(parts));
    std::vector<std::string> taskParameters = {"11", "offline"};
    parameter.set_task_specfic_paras(std::move(taskParameters));

    cpp2::AddAdminTaskRequest request;
    request.set_cmd(meta::cpp2::AdminCmd::REBUILD_TAG_INDEX);
    request.set_job_id(1);
    request.set_task_id(1);
    request.set_para(std::move(parameter));
    request.set_concurrency(3);

    auto callback = [](cpp2::ErrorCode) {};
    TaskContext context(request, callback);

    auto task = std::make_shared<RebuildTagIndexTask>(env, std::move(context));
    manager->addAsyncTask(task);

    // Wait for the task finished
    do {
        sleep(1);
    } while (!manager->isFinished(context.jobId_, context.taskId_));

    // Check the result
    LOG(INFO) << "Check rebuild tag index...";
    for (auto& key : mock::MockData::mockPlayerIndexKeys()) {
        std::string value;
        auto code = env->kvstore_->get(1, key.first, key.second, &value);
        EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, code);
    }

    manager->shutdown();
}

TEST(RebuildIndexTest, RebuildTagIndexOnlineWithAppend) {
    fs::TempDir rootPath("/tmp/RebuildTagIndexOnlineTestWithAppend.XXXXXX");
    auto writer = std::make_unique<thread::GenericWorker>();
    EXPECT_TRUE(writer->start());

    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();

    auto appendVertices = [&] () mutable {
        LOG(INFO) << "Start Background Writer";
        {
            LOG(INFO) << "Append is Running";
            auto* processor = AddVerticesProcessor::instance(env, nullptr);
            cpp2::AddVerticesRequest req = mock::MockData::mockAddVerticesReq(true);
            auto fut = processor->getFuture();
            processor->process(req);
            auto resp = std::move(fut).get();
            EXPECT_EQ(0, resp.result.failed_parts.size());
        }
    };

    // Add Vertices
    auto* processor = AddVerticesProcessor::instance(env, nullptr);
    cpp2::AddVerticesRequest req = mock::MockData::mockAddVerticesReq(true);
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());

    auto manager = AdminTaskManager::instance();
    manager->init();

    cpp2::TaskPara parameter;
    parameter.set_space_id(1);
    std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};
    parameter.set_parts(std::move(parts));
    std::vector<std::string> taskParameters = {"11", "online"};
    parameter.set_task_specfic_paras(std::move(taskParameters));

    cpp2::AddAdminTaskRequest request;
    request.set_cmd(meta::cpp2::AdminCmd::REBUILD_TAG_INDEX);
    request.set_job_id(1);
    request.set_task_id(1);
    request.set_para(std::move(parameter));
    request.set_concurrency(3);

    auto callback = [](cpp2::ErrorCode) {};
    TaskContext context(request, callback);

    auto task = std::make_shared<RebuildTagIndexTask>(env, std::move(context));
    manager->addAsyncTask(task);

    writer->addTask(appendVertices).get();

    // Wait for the task finished
    do {
        sleep(1);
    } while (!manager->isFinished(context.jobId_, context.taskId_));

    LOG(INFO) << "Check rebuild tag index...";
    for (auto& key : mock::MockData::mockPlayerIndexKeys(true)) {
        std::string value;
        auto code = env->kvstore_->get(1, key.first, key.second, &value);
        EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, code);
    }

    writer->stop();
    manager->shutdown();
}

TEST(RebuildIndexTest, RebuildTagIndexOnlineWithDelete) {
    fs::TempDir rootPath("/tmp/RebuildTagIndexOnlineWithDelete.XXXXXX");
    auto writer = std::make_unique<thread::GenericWorker>();
    EXPECT_TRUE(writer->start());

    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();

    auto deleteVertices = [&] () mutable {
        LOG(INFO) << "Start Background Writer & Delete is Running";
        cpp2::DeleteVerticesRequest req = mock::MockData::mockDeleteVerticesReq();
        auto* processor = DeleteVerticesProcessor::instance(env, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
    };

    // Add Vertices
    auto* processor = AddVerticesProcessor::instance(env, nullptr);
    cpp2::AddVerticesRequest req = mock::MockData::mockAddVerticesReq();
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());

    auto manager = AdminTaskManager::instance();
    manager->init();

    cpp2::TaskPara parameter;
    parameter.set_space_id(1);
    std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};
    parameter.set_parts(std::move(parts));
    std::vector<std::string> taskParameters = {"11", "online"};
    parameter.set_task_specfic_paras(std::move(taskParameters));

    cpp2::AddAdminTaskRequest request;
    request.set_cmd(meta::cpp2::AdminCmd::REBUILD_TAG_INDEX);
    request.set_job_id(1);
    request.set_task_id(1);
    request.set_para(std::move(parameter));
    request.set_concurrency(3);

    auto callback = [](cpp2::ErrorCode) {};
    TaskContext context(request, callback);

    auto task = std::make_shared<RebuildTagIndexTask>(env, std::move(context));
    manager->addAsyncTask(task);

    writer->addTask(deleteVertices).get();

    // Wait for the task finished
    do {
        sleep(1);
    } while (!manager->isFinished(context.jobId_, context.taskId_));

    LOG(INFO) << "Check rebuild tag index...";
    for (auto& key : mock::MockData::mockPlayerIndexKeys(true)) {
        std::string value;
        auto code = env->kvstore_->get(1, key.first, key.second, &value);
        EXPECT_EQ(kvstore::ResultCode::ERR_KEY_NOT_FOUND, code);
    }

    writer->stop();
    manager->shutdown();
}

TEST(RebuildIndexTest, RebuildEdgeIndexOffline) {
    fs::TempDir rootPath("/tmp/RebuildEdgeIndexOfflineTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();

    // Add Edges
    auto* processor = AddEdgesProcessor::instance(env, nullptr);
    cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq();
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());
    checkAddEdgesData(req, env, 334, 0);

    auto manager = AdminTaskManager::instance();
    manager->init();

    cpp2::TaskPara parameter;
    parameter.set_space_id(1);
    std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};
    parameter.set_parts(std::move(parts));
    std::vector<std::string> taskParameters = {"12", "offline"};
    parameter.set_task_specfic_paras(std::move(taskParameters));

    cpp2::AddAdminTaskRequest request;
    request.set_cmd(meta::cpp2::AdminCmd::REBUILD_EDGE_INDEX);
    request.set_job_id(2);
    request.set_task_id(1);
    request.set_para(std::move(parameter));
    request.set_concurrency(3);

    auto callback = [](cpp2::ErrorCode) {};
    TaskContext context(request, callback);
    auto task = std::make_shared<RebuildEdgeIndexTask>(env, std::move(context));
    manager->addAsyncTask(task);

    // Wait for the task finished
    do {
        sleep(1);
    } while (!manager->isFinished(context.jobId_, context.taskId_));

    // Check the result
    LOG(INFO) << "Check rebuild tag index...";
    for (auto& key : mock::MockData::mockServeIndexKeys()) {
        std::string value;
        auto code = env->kvstore_->get(1, key.first, key.second, &value);
        EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, code);
    }
    manager->shutdown();
}

TEST(RebuildIndexTest, RebuildEdgeIndexOnlineWithAppend) {
    fs::TempDir rootPath("/tmp/RebuildEdgeIndexOnlineWithAppendTest.XXXXXX");
    auto writer = std::make_unique<thread::GenericWorker>();
    EXPECT_TRUE(writer->start());

    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();

    auto appendEdges = [&] () mutable {
        {
            auto* processor = AddEdgesProcessor::instance(env, nullptr);
            cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq(true);
            auto fut = processor->getFuture();
            processor->process(req);
            auto resp = std::move(fut).get();
            EXPECT_EQ(0, resp.result.failed_parts.size());
        }
    };
    writer->addTask(appendEdges);

    // Add Edges
    auto* processor = AddEdgesProcessor::instance(env, nullptr);
    cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq();
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());

    auto manager = AdminTaskManager::instance();
    manager->init();

    cpp2::TaskPara parameter;
    parameter.set_space_id(1);
    std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};
    parameter.set_parts(std::move(parts));
    std::vector<std::string> taskParameters = {"12", "online"};
    parameter.set_task_specfic_paras(std::move(taskParameters));

    cpp2::AddAdminTaskRequest request;
    request.set_cmd(meta::cpp2::AdminCmd::REBUILD_EDGE_INDEX);
    request.set_job_id(2);
    request.set_task_id(1);
    request.set_para(std::move(parameter));
    request.set_concurrency(3);

    auto callback = [](cpp2::ErrorCode) {};
    TaskContext context(request, callback);
    auto task = std::make_shared<RebuildEdgeIndexTask>(env, std::move(context));
    manager->addAsyncTask(task);

    // Wait for the task finished
    do {
        sleep(1);
    } while (!manager->isFinished(context.jobId_, context.taskId_));

    // Check the result
    LOG(INFO) << "Check rebuild tag index...";
    for (auto& key : mock::MockData::mockServeIndexKeys()) {
        std::string value;
        auto code = env->kvstore_->get(1, key.first, key.second, &value);
        EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, code);
    }

    writer->stop();
    manager->shutdown();
}

TEST(RebuildIndexTest, RebuildEdgeIndexOnlineWithDelete) {
    fs::TempDir rootPath("/tmp/RebuildEdgeIndexOnlineWithDeleteTest.XXXXXX");
    auto writer = std::make_unique<thread::GenericWorker>();
    EXPECT_TRUE(writer->start());

    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();

    auto deleteEdges = [&] () mutable {
        cpp2::DeleteEdgesRequest req = mock::MockData::mockDeleteEdgesReq();
        auto* processor = DeleteEdgesProcessor::instance(env, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
    };

    // Add Edges
    auto* processor = AddEdgesProcessor::instance(env, nullptr);
    cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq();
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());

    auto manager = AdminTaskManager::instance();
    EXPECT_TRUE(manager->init());

    cpp2::TaskPara parameter;
    parameter.set_space_id(1);
    std::vector<PartitionID> parts = {1, 2, 3, 4, 5, 6};
    parameter.set_parts(std::move(parts));
    std::vector<std::string> taskParameters = {"12", "online"};
    parameter.set_task_specfic_paras(std::move(taskParameters));

    cpp2::AddAdminTaskRequest request;
    request.set_cmd(meta::cpp2::AdminCmd::REBUILD_EDGE_INDEX);
    request.set_job_id(2);
    request.set_task_id(1);
    request.set_para(std::move(parameter));
    request.set_concurrency(3);

    writer->addTask(deleteEdges).get();

    auto callback = [](cpp2::ErrorCode) {};
    TaskContext context(request, callback);
    auto task = std::make_shared<RebuildEdgeIndexTask>(env, std::move(context));
    manager->addAsyncTask(task);

    // Wait for the task finished
    do {
        sleep(1);
    } while (!manager->isFinished(context.jobId_, context.taskId_));

    // Check the result
    LOG(INFO) << "Check rebuild edge index...";
    for (auto& key : mock::MockData::mockServeIndexKeys()) {
        std::string value;
        auto code = env->kvstore_->get(1, key.first, key.second, &value);
        EXPECT_EQ(kvstore::ResultCode::ERR_KEY_NOT_FOUND, code);
    }

    writer->stop();
    manager->shutdown();
}

}  // namespace storage
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
