/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "common/NebulaKeyUtils.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "fs/TempDir.h"
#include "storage/mutate/AddVerticesProcessor.h"
#include "mock/MockCluster.h"
#include "interface/gen-cpp2/storage_types.h"
#include "interface/gen-cpp2/common_types.h"

namespace nebula {
namespace storage {

TEST(AddVerticesTest, SimpleTest) {
    fs::TempDir rootPath("/tmp/AddVerticesTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.startStorage({0, 0}, rootPath.path());
    auto* env = cluster.storageEnv_.get();

    auto* processor = AddVerticesProcessor::instance(env, nullptr);

    LOG(INFO) << "Build AddVerticesRequest...";
    cpp2::AddVerticesRequest req;
    req.space_id = 1;
    req.overwritable = true;

    // partId => List<NeWVertex>
    // NewVertex => {Id, List<NewTag>}
    // NewTag => {tagId, PropDataByRow, optional list<binary> }
    // PropDataByRow => {list<common.Value>}
    for (PartitionID partId = 1; partId < 4; partId++) {
        std::vector<cpp2::NewVertex> vertices;

        for (auto vid = partId * 10; vid < (partId + 1) * 10; vid++) {
            cpp2::NewVertex  newV;
            newV.set_id(folly::to<std::string>(vid));
            std::vector<cpp2::NewTag> nt;

            for (auto tid = 3001; tid < 3006; tid++) {
                cpp2::NewTag newT;
                newT.set_tag_id(tid);

                cpp2::PropDataByRow  row;
                std::vector<nebula::Value> vs;
                for (auto i = 0; i < 5; i++) {
                    nebula::Value v;
                    v.setInt(i);
                    vs.push_back(v);
                }
                for (auto i = 5; i < 10; i++) {
                    nebula::Value v;
                    v.setStr(folly::to<std::string>(i));
                    vs.push_back(v);
                }

                row.set_props(vs);
                newT.set_props(std::move(row));
                nt.push_back(std::move(newT));
            }
            newV.set_tags(std::move(nt));
            vertices.push_back(std::move(newV));
        }
        req.parts.emplace(partId, std::move(vertices));
    }

    LOG(INFO) << "Test AddVerticesProcessor...";
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());

    LOG(INFO) << "Check data in kv store...";
    auto ret = env->schemaMan_->getSpaceVidLen(1);
    auto spaceVidLen = ret.value();

    for (PartitionID partId = 1; partId < 4; partId++) {
        for (auto vid = partId * 10; vid < 10 * (partId + 1); vid++) {
            auto tagId = 3002;
            auto prefix = NebulaKeyUtils::vertexPrefix(spaceVidLen,
                                                       partId,
                                                       folly::to<std::string>(vid),
                                                       tagId);
            std::unique_ptr<kvstore::KVIterator> iter;
            EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, env->kvstore_->prefix(1, partId, prefix, &iter));
            auto schema = env->schemaMan_->getTagSchema(1, tagId);
            EXPECT_TRUE(schema != NULL);

            auto count = 0;
            while (iter->valid()) {
                auto reader = RowReader::getRowReader(schema.get(), iter->val());
                for (auto i = 0; i < 5; i++) {
                    Value val = reader->getValueByIndex(i);
                    EXPECT_EQ(Value::Type::INT, val.type());
                    EXPECT_EQ(i, val.getInt());
                }
                for (auto i = 5; i < 10; i++) {
                    Value val = reader->getValueByIndex(i);
                    EXPECT_EQ(Value::Type::STRING, val.type());
                    EXPECT_EQ(folly::to<std::string>(i), val.getStr());
                }
                count++;
                iter->next();
            }
            EXPECT_EQ(1, count);
        }
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


