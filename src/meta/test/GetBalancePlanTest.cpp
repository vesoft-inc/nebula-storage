/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2021.2022 License,
 * attached with Common Clause Condition 2023.2024, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include "common/fs/TempDir.h"
#include "meta/test/TestUtils.h"
#include "meta/test/MockAdminClient.h"
#include "meta/processors/jobMan/JobManager.h"
#include "meta/processors/jobMan/BalancePlan.h"
#include "meta/processors/jobMan/GetBalancePlanProcessor.h"

namespace nebula {
namespace meta {

using ::testing::_;
using ::testing::ByMove;
using ::testing::Return;
using ::testing::DefaultValue;
using ::testing::NiceMock;
using ::testing::SetArgPointee;

class GetBalancePlanTest : public ::testing::Test {
protected:
    void SetUp() override {
        rootPath_ = std::make_unique<fs::TempDir>("/tmp/GetBalancePlanTest.XXXXXX");
        mock::MockCluster cluster;
        kv_ = cluster.initMetaKV(rootPath_->path());

        DefaultValue<folly::Future<Status>>::SetFactory([] {
            return folly::Future<Status>(Status::OK());
        });

        jobMgr = JobManager::getInstance();
        jobMgr->status_ = JobManager::JbmgrStatus::NOT_START;
        jobMgr->init(kv_.get());
    }

    void TearDown() override {
        jobMgr->shutDown();
        kv_.reset();
        rootPath_.reset();
    }

    std::unique_ptr<fs::TempDir> rootPath_{nullptr};
    std::unique_ptr<kvstore::KVStore> kv_{nullptr};
    JobManager* jobMgr{nullptr};
};

TEST_F(GetBalancePlanTest, BalancePlanJob) {
    ASSERT_TRUE(TestUtils::createSomeHosts(kv_.get()));
    GraphSpaceID spaceId = 1;
    TestUtils::assembleSpace(kv_.get(), spaceId, 1);
    std::vector<std::string> paras{"false", "test_space"};
    JobDescription balanceJob(12, cpp2::AdminCmd::DATA_BALANCE, paras);
    NiceMock<MockAdminClient> adminClient;
    jobMgr->adminClient_ = &adminClient;
    auto result = jobMgr->save(balanceJob.jobKey(), balanceJob.jobVal());
    ASSERT_EQ(result, nebula::cpp2::ErrorCode::SUCCEEDED);

    auto jobRet = JobDescription::loadJobDescription(balanceJob.id_, kv_.get());
    ASSERT_TRUE(nebula::ok(jobRet));
    auto job = nebula::value(jobRet);
    ASSERT_EQ(balanceJob.id_, job.id_);
    ASSERT_EQ(cpp2::JobStatus::QUEUE, job.status_);

    auto setStatusRet = job.setStatus(cpp2::JobStatus::RUNNING);
    ASSERT_TRUE(setStatusRet);

    BalancePlan plan = BalancePlan(balanceJob.id_, spaceId, kv_.get(), nullptr);

    for (int32_t i = 0; i < 3; i++) {
        plan.addTask(BalanceTask(balanceJob.id_, i, spaceId, i,
                                 HostAddr(folly::to<std::string>(i), i),
                                 HostAddr(folly::to<std::string>(i + 1), i + 1),
                                 kv_.get(), nullptr));
    }

    plan.status_ = cpp2::JobStatus::RUNNING;
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, plan.saveJobStatus());

    {
        cpp2::GetBalancePlanReq req;
        req.set_space_id(spaceId);
        auto* processor = GetBalancePlanProcessor::instance(kv_.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        auto planItem = resp.get_plan();
        ASSERT_NE(cpp2::JobStatus::FINISHED, planItem.get_status());
    }

    ASSERT_TRUE(jobMgr->runJobInternal(balanceJob));
    plan.status_ = cpp2::JobStatus::FINISHED;
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, plan.saveJobStatus());

    ASSERT_TRUE(job.setStatus(cpp2::JobStatus::FINISHED));
    result = jobMgr->save(job.jobKey(), job.jobVal());
    ASSERT_EQ(result, nebula::cpp2::ErrorCode::SUCCEEDED);

    {
        cpp2::GetBalancePlanReq req;
        req.set_space_id(spaceId);
        auto* processor = GetBalancePlanProcessor::instance(kv_.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        auto planItem = resp.get_plan();
        ASSERT_EQ(cpp2::JobStatus::FINISHED, planItem.get_status());
        ASSERT_EQ(3, planItem.get_tasks().size());
    }
}

}  // namespace meta
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
