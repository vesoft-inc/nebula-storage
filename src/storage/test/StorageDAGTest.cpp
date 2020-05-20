
/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "base/Base.h"
#include <gtest/gtest.h>
#include "storage/exec/StorageDAG.h"

namespace nebula {
namespace storage {

class StorageDAGTest : public ::testing::Test {
protected:
    PartitionID partId_ = 0;
    VertexID vId_ = "nebula";

    const nebula::Row& result() {
        resultRow_.columns.emplace_back(vId_);
        resultRow_.columns.emplace_back(NullType::__NULL__);
        return resultRow_;
    }

private:
    nebula::Row resultRow_;
};

TEST_F(StorageDAGTest, SimpleTest) {
    StorageDAG dag;
    auto out = std::make_unique<RelNode>("leaf");
    dag.addNode(std::move(out));
    auto ret = dag.go(partId_, vId_).get();
    ASSERT_EQ(kvstore::ResultCode::SUCCEEDED, ret);
}

TEST_F(StorageDAGTest, ChainTest) {
    StorageDAG dag;
    size_t lastIdx;
    for (size_t i = 0; i < 10; i++) {
        auto node = std::make_unique<RelNode>(folly::to<std::string>(i));
        if (i != 0) {
            node->addDependency(dag.getNode(lastIdx));
        }
        lastIdx = dag.addNode(std::move(node));
    }
    auto out = std::make_unique<RelNode>("leaf");
    out->addDependency(dag.getNode(lastIdx));
    dag.addNode(std::move(out));
    auto ret = dag.go(partId_, vId_).get();
    ASSERT_EQ(kvstore::ResultCode::SUCCEEDED, ret);
}

TEST_F(StorageDAGTest, FanOutInTest) {
    StorageDAG dag;
    auto out = std::make_unique<RelNode>("leaf");
    for (size_t i = 0; i < 10; i++) {
        auto node = std::make_unique<RelNode>(folly::to<std::string>(i));
        auto idx = dag.addNode(std::move(node));
        out->addDependency(dag.getNode(idx));
    }
    dag.addNode(std::move(out));
    auto ret = dag.go(partId_, vId_).get();
    ASSERT_EQ(kvstore::ResultCode::SUCCEEDED, ret);
}

TEST_F(StorageDAGTest, RerunTest) {
    StorageDAG dag;
    auto out = std::make_unique<RelNode>("leaf");
    for (size_t i = 0; i < 10; i++) {
        auto node = std::make_unique<RelNode>(folly::to<std::string>(i));
        auto idx = dag.addNode(std::move(node));
        out->addDependency(dag.getNode(idx));
    }
    dag.addNode(std::move(out));

    // re-run the dag
    for (size_t i = 0; i < 10; i++) {
        auto ret = dag.go(partId_, vId_).get();
        ASSERT_EQ(kvstore::ResultCode::SUCCEEDED, ret);
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
