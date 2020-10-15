/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "common/interface/gen-cpp2/common_types.h"
#include "utils/NebulaKeyUtils.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "storage/mutate/AddEdgesProcessor.h"
#include "storage/mutate/UpdateEdgeProcessor.h"
#include "storage/mutate/DeleteEdgesProcessor.h"
#include "storage/query/GetEdgetypeEdgesProcessor.h"
#include "storage/test/TestUtils.h"
#include "storage/admin/AdminTaskManager.h"
#include "storage/admin/RebuildEdgeIndexTask.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"
#include "common/expression/ConstantExpression.h"

namespace nebula {
namespace storage {

cpp2::GetEdgetypeEdgesRequest
buildGetEdgetypeEdgesRequest(int32_t parts, IndexID indexId) {
    cpp2::GetEdgetypeEdgesRequest req;
    req.space_id = 1;
    for (int32_t i = 1; i <= parts; i++) {
        req.parts.emplace_back(i);
    }
    req.index = indexId;
    return req;
}

// Add MockData::serves_ and MockData::teammates_ data
cpp2::AddEdgesRequest buildAddEdgesRequest(int32_t parts) {
    cpp2::AddEdgesRequest req;
    req.space_id = 1;
    req.overwritable = true;

    auto retRecs = mock::MockData::mockMultiEdges();
    for (auto& rec : retRecs) {
        nebula::storage::cpp2::NewEdge newEdge;
        nebula::storage::cpp2::EdgeKey edgeKey;
        auto partId = std::hash<std::string>()(rec.srcId_) % parts + 1;

        edgeKey.src = rec.srcId_;
        edgeKey.edge_type = rec.type_;
        edgeKey.ranking = rec.rank_;
        edgeKey.dst = rec.dstId_;

        newEdge.set_key(std::move(edgeKey));
        newEdge.set_props(std::move(rec.props_));

        req.parts[partId].emplace_back(std::move(newEdge));
    }
    return req;
}

TEST(GetEdgetypeEdgesTest, SimpleTest) {
    fs::TempDir rootPath("/tmp/GetEdgetypeEdgesTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();
    GraphSpaceID spaceId = 1;
    AdminTaskManager* manager = AdminTaskManager::instance();
    manager->init();

    // Empty edge data
    {
        // Get all edge in edge 101
        auto* processor = GetEdgetypeEdgesProcessor::instance(env, nullptr);
        LOG(INFO) << "Build GetEdgetypeEdgesRequest...";
        cpp2::GetEdgetypeEdgesRequest req = buildGetEdgetypeEdgesRequest(parts, 104);

        LOG(INFO) << "Test GetEdgetypeEdgesProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        EXPECT_EQ(0, resp.edges.rowSize());
    }
    {
        // Get all edge in edge 102
        auto* processor = GetEdgetypeEdgesProcessor::instance(env, nullptr);
        LOG(INFO) << "Build GetEdgetypeEdgesRequest...";
        cpp2::GetEdgetypeEdgesRequest req = buildGetEdgetypeEdgesRequest(parts, 105);

        LOG(INFO) << "Test GetEdgetypeEdgesProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        EXPECT_EQ(0, resp.edges.rowSize());
    }
    // Add edge data
    {
        auto* processor = AddEdgesProcessor::instance(env, nullptr);
        LOG(INFO) << "Build AddEdgesRequest...";
        cpp2::AddEdgesRequest req = buildAddEdgesRequest(parts);;

        LOG(INFO) << "Test AddEdgesProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());

        LOG(INFO) << "Check data in kv store...";
        // The number of data in serves and teammates is 370
        checkAddEdgesData(req, env, 370, 0);
    }
    {
        // Get all edge in edge 101
        auto* processor = GetEdgetypeEdgesProcessor::instance(env, nullptr);
        LOG(INFO) << "Build GetEdgetypeEdgesRequest...";
        cpp2::GetEdgetypeEdgesRequest req = buildGetEdgetypeEdgesRequest(parts, 104);

        LOG(INFO) << "Test GetEdgetypeEdgesProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        EXPECT_EQ(167, resp.edges.rowSize());
        EXPECT_EQ(3, resp.edges.rows[0].size());
    }
    {
        // Get all edge in edge 102
        auto* processor = GetEdgetypeEdgesProcessor::instance(env, nullptr);
        LOG(INFO) << "Build GetEdgetypeEdgesRequest...";
        cpp2::GetEdgetypeEdgesRequest req = buildGetEdgetypeEdgesRequest(parts, 105);

        LOG(INFO) << "Test GetEdgetypeEdgesProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        EXPECT_EQ(18, resp.edges.rowSize());
        EXPECT_EQ(3, resp.edges.rows[0].size());
    }
    // update edge data
    {
        LOG(INFO) << "Build UpdateEdgeRequest...";
        cpp2::UpdateEdgeRequest req;
        auto partId = std::hash<std::string>()("Tim Duncan") % parts + 1;
        req.set_space_id(spaceId);
        req.set_part_id(partId);
        // src = Tim Duncan, edge_type = 101, ranking = 1997, dst = Spurs
        VertexID srcId = "Tim Duncan";
        VertexID dstId = "Spurs";
        EdgeRanking rank = 1997;
        EdgeType edgeType = 101;
        storage::cpp2::EdgeKey edgeKey;
        edgeKey.set_src(srcId);
        edgeKey.set_edge_type(edgeType);
        edgeKey.set_ranking(rank);
        edgeKey.set_dst(dstId);
        req.set_edge_key(edgeKey);

        LOG(INFO) << "Build updated props...";
        std::vector<cpp2::UpdatedProp> props;
        // int: 101.teamCareer = 20
        cpp2::UpdatedProp uProp1;
        uProp1.set_name("teamCareer");
        ConstantExpression val1(20);
        uProp1.set_value(Expression::encode(val1));
        props.emplace_back(uProp1);

        // bool: 101.type = trade
        cpp2::UpdatedProp uProp2;
        uProp2.set_name("type");
        std::string colnew("trade");
        ConstantExpression val2(colnew);
        uProp2.set_value(Expression::encode(val2));
        props.emplace_back(uProp2);
        req.set_updated_props(std::move(props));
        req.set_insertable(false);

        LOG(INFO) << "Test UpdateEdgeRequest...";
        auto* processor = UpdateEdgeProcessor::instance(env, nullptr);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();

        LOG(INFO) << "Check the results...";
        EXPECT_EQ(0, resp.result.failed_parts.size());
    }
    {
        // Get all edge in edge 101
        auto* processor = GetEdgetypeEdgesProcessor::instance(env, nullptr);
        LOG(INFO) << "Build GetEdgetypeEdgesRequest...";
        cpp2::GetEdgetypeEdgesRequest req = buildGetEdgetypeEdgesRequest(parts, 104);

        LOG(INFO) << "Test GetEdgetypeEdgesProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        EXPECT_EQ(167, resp.edges.rowSize());
        EXPECT_EQ(3, resp.edges.rows[0].size());
    }
    {
        // Get all edge in edge 102
        auto* processor = GetEdgetypeEdgesProcessor::instance(env, nullptr);
        LOG(INFO) << "Build GetEdgetypeEdgesRequest...";
        cpp2::GetEdgetypeEdgesRequest req = buildGetEdgetypeEdgesRequest(parts, 105);

        LOG(INFO) << "Test GetEdgetypeEdgesProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        EXPECT_EQ(18, resp.edges.rowSize());
        EXPECT_EQ(3, resp.edges.rows[0].size());
    }
    // upsert new edge
    {
        LOG(INFO) << "Build UpdateEdgeRequest...";
        cpp2::UpdateEdgeRequest req;

        auto partId = std::hash<std::string>()("Brandon Ingram") % parts + 1;
        req.set_space_id(spaceId);
        req.set_part_id(partId);
        VertexID srcId = "Brandon Ingram";
        VertexID dstId = "Lakers";
        EdgeRanking rank = 2016;
        EdgeType edgeType = 101;
        // src = Brandon Ingram, edge_type = 101, ranking = 2016, dst = Lakers
        storage::cpp2::EdgeKey edgeKey;
        edgeKey.set_src(srcId);
        edgeKey.set_edge_type(edgeType);
        edgeKey.set_ranking(rank);
        edgeKey.set_dst(dstId);
        req.set_edge_key(edgeKey);

        LOG(INFO) << "Build updated props...";
        std::vector<cpp2::UpdatedProp> props;

        // string: 101.playerName = Brandon Ingram
        cpp2::UpdatedProp uProp1;
        uProp1.set_name("playerName");
        std::string col1new("Brandon Ingram");
        ConstantExpression val1(col1new);
        uProp1.set_value(Expression::encode(val1));
        props.emplace_back(uProp1);

        // string: 101.teamName = Lakers
        cpp2::UpdatedProp uProp2;
        uProp2.set_name("teamName");
        std::string col2new("Lakers");
        ConstantExpression val2(col2new);
        uProp2.set_value(Expression::encode(val2));
        props.emplace_back(uProp2);

        // int: 101.startYear = 2016
        cpp2::UpdatedProp uProp3;
        uProp3.set_name("startYear");
        ConstantExpression val3(2016L);
        uProp3.set_value(Expression::encode(val3));
        props.emplace_back(uProp3);

        // int: 101.teamCareer = 1
        cpp2::UpdatedProp uProp4;
        uProp4.set_name("teamCareer");
        ConstantExpression val4(1L);
        uProp4.set_value(Expression::encode(val4));
        props.emplace_back(uProp4);
        req.set_updated_props(std::move(props));
        req.set_insertable(true);

        LOG(INFO) << "Test UpdateEdgeRequest...";
        auto* processor = UpdateEdgeProcessor::instance(env, nullptr);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();

        LOG(INFO) << "Check the results...";
        EXPECT_EQ(0, resp.result.failed_parts.size());
    }
    {
        // Get all edge in edge 101
        auto* processor = GetEdgetypeEdgesProcessor::instance(env, nullptr);
        LOG(INFO) << "Build GetEdgetypeEdgesRequest...";
        cpp2::GetEdgetypeEdgesRequest req = buildGetEdgetypeEdgesRequest(parts, 104);

        LOG(INFO) << "Test GetEdgetypeEdgesProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        EXPECT_EQ(168, resp.edges.rowSize());
        EXPECT_EQ(3, resp.edges.rows[0].size());
    }
    {
        // Get all edge in edge 102
        auto* processor = GetEdgetypeEdgesProcessor::instance(env, nullptr);
        LOG(INFO) << "Build GetEdgetypeEdgesRequest...";
        cpp2::GetEdgetypeEdgesRequest req = buildGetEdgetypeEdgesRequest(parts, 105);

        LOG(INFO) << "Test GetEdgetypeEdgesProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        EXPECT_EQ(18, resp.edges.rowSize());
        EXPECT_EQ(3, resp.edges.rows[0].size());
    }
    // rebuild index
    {
        cpp2::TaskPara parameter;
        parameter.set_space_id(1);
        std::vector<PartitionID> partitions = {1, 2, 3, 4, 5, 6};
        parameter.set_parts(std::move(partitions));

        cpp2::AddAdminTaskRequest request;
        request.set_cmd(meta::cpp2::AdminCmd::REBUILD_EDGE_INDEX);
        request.set_job_id(8);
        request.set_task_id(18);
        request.set_para(std::move(parameter));

        auto callback = [](cpp2::ErrorCode) {};
        TaskContext context(request, callback);
        auto task = std::make_shared<RebuildEdgeIndexTask>(env, std::move(context));
        manager->addAsyncTask(task);

        // Wait for the task finished
        do {
            usleep(50);
        } while (!manager->isFinished(context.jobId_, context.taskId_));

        env->rebuildIndexGuard_->clear();
        sleep(1);
    }
    {
        // Get all edge in edge 101
        auto* processor = GetEdgetypeEdgesProcessor::instance(env, nullptr);
        LOG(INFO) << "Build GetEdgetypeEdgesRequest...";
        cpp2::GetEdgetypeEdgesRequest req = buildGetEdgetypeEdgesRequest(parts, 104);

        LOG(INFO) << "Test GetEdgetypeEdgesProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        EXPECT_EQ(168, resp.edges.rowSize());
        EXPECT_EQ(3, resp.edges.rows[0].size());
    }
    {
        // Get all edge in edge 102
        auto* processor = GetEdgetypeEdgesProcessor::instance(env, nullptr);
        LOG(INFO) << "Build GetEdgetypeEdgesRequest...";
        cpp2::GetEdgetypeEdgesRequest req = buildGetEdgetypeEdgesRequest(parts, 105);

        LOG(INFO) << "Test GetEdgetypeEdgesProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        EXPECT_EQ(18, resp.edges.rowSize());
        EXPECT_EQ(3, resp.edges.rows[0].size());
    }
    // Delete serves_ edge data
    {
        auto* processor = DeleteEdgesProcessor::instance(env, nullptr);

        LOG(INFO) << "Build DeleteEdgesRequest...";
        // All the serves_ datas are deleted except upsert data,
        cpp2::DeleteEdgesRequest req = mock::MockData::mockDeleteEdgesReq();

        LOG(INFO) << "Test DeleteEdgesProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
    }
    {
        // Get all edge in edge 101
        auto* processor = GetEdgetypeEdgesProcessor::instance(env, nullptr);
        LOG(INFO) << "Build GetEdgetypeEdgesRequest...";
        cpp2::GetEdgetypeEdgesRequest req = buildGetEdgetypeEdgesRequest(parts, 104);

        LOG(INFO) << "Test GetEdgetypeEdgesProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        EXPECT_EQ(1, resp.edges.rowSize());
        EXPECT_EQ(3, resp.edges.rows[0].size());
    }
    {
        // Get all edge in edge 102
        auto* processor = GetEdgetypeEdgesProcessor::instance(env, nullptr);
        LOG(INFO) << "Build GetEdgetypeEdgesRequest...";
        cpp2::GetEdgetypeEdgesRequest req = buildGetEdgetypeEdgesRequest(parts, 105);

        LOG(INFO) << "Test GetEdgetypeEdgesProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        EXPECT_EQ(18, resp.edges.rowSize());
        EXPECT_EQ(3, resp.edges.rows[0].size());
    }
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

