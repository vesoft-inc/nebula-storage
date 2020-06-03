/* Copyright (c) 2020 vesoft inc. All rights reserved.
  *
  * This source code is licensed under Apache 2.0 License,
  * attached with Common Clause Condition 1.0, found in the LICENSES directory.
  */

#include "common/base/Base.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "common/fs/TempDir.h"
#include "utils/NebulaKeyUtils.h"
#include "utils/IndexKeyUtils.h"
#include "mock/MockCluster.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "common/interface/gen-cpp2/common_types.h"
#include "storage/index/LookupProcessor.h"
#include "codec/test/RowWriterV1.h"
#include "codec/RowWriterV2.h"

namespace nebula {
namespace storage {

TEST(LookupIndexTest, LookupIndexTestV1) {
    fs::TempDir rootPath("/tmp/LookupIndexTestV1.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto vIdLen = cluster.schemaMan_->getSpaceVidLen(1);
    ASSERT_TRUE(vIdLen.ok());
    // setup v1 data and v2 data
    {
        int64_t vid1 = 1, vid2 = 2;
        std::vector<nebula::kvstore::KV> keyValues;
        // setup V1 row
        auto vId1 = reinterpret_cast<const char*>(&vid1);
        auto schemaV1 = env->schemaMan_->getTagSchema(1, 3, 0);
        RowWriterV1 writer(schemaV1.get());
        writer << true << 1L << 1.1F << 1.1F << "row1";
        writer.encode();
        auto key = NebulaKeyUtils::vertexKey(vIdLen.value(), 1, vId1, 3, 0);
        keyValues.emplace_back(std::move(key), writer.encode());

        // setup V2 row
        auto vId2 = reinterpret_cast<const char*>(&vid2);
        const Date date = {2020, 2, 20};
        const DateTime dt = {2020, 2, 20, 10, 30, 45, -8 * 3600};
        auto schemaV2 = env->schemaMan_->getTagSchema(1, 3, 1);
        RowWriterV2 writer2(schemaV2.get());
        writer2.setValue("col_bool", true);
        writer2.setValue("col_int", 1L);
        writer2.setValue("col_float", 1.1F);
        writer2.setValue("col_double", 1.1F);
        writer2.setValue("col_str", "row1");
        writer2.setValue("col_int8", 8);
        writer2.setValue("col_int16", 16);
        writer2.setValue("col_int32", 32);
        writer2.setValue("col_timestamp", 1L);
        writer2.setValue("col_date", date);
        writer2.setValue("col_datetime", dt);
        writer2.finish();
        key = NebulaKeyUtils::vertexKey(vIdLen.value(), 1, vId2, 3, 0);
        keyValues.emplace_back(std::move(key), writer2.getEncodedStr());

        // setup index key

        std::vector<Value> values;
        values.emplace_back(Value(true));
        values.emplace_back(Value(1L));
        values.emplace_back(Value(1.1F));
        values.emplace_back(Value(1.1F));
        values.emplace_back(Value("row1"));

        key = IndexKeyUtils::vertexIndexKey(vIdLen.value(), 1, 3, vId1, values);
        keyValues.emplace_back(std::move(key), "");

        key = IndexKeyUtils::vertexIndexKey(vIdLen.value(), 1, 3, vId2, values);
        keyValues.emplace_back(std::move(key), "");

        // insert data
        env->kvstore_->asyncMultiPut(1, 1, std::move(keyValues),
                                     [](nebula::kvstore::ResultCode code) {
                                         EXPECT_EQ(nebula::kvstore::ResultCode::SUCCEEDED , code);
                                     });
    }
    {
        auto* processor = LookupProcessor::instance(env, nullptr, nullptr);
        cpp2::LookupIndexRequest req;
        req.set_space_id(1);
        req.set_tag_or_edge_id(3);
        req.set_is_edge(false);
        decltype(req.parts) parts;
        parts.emplace_back(1);
        req.set_parts(std::move(parts));
        decltype(req.return_columns) returnCols;
        returnCols.emplace_back("col_bool");
        returnCols.emplace_back("col_int");
        req.set_return_columns(std::move(returnCols));
        cpp2::IndexColumnHint columnHint;
        columnHint.set_begin_value(Value(true));
        columnHint.set_column_name("col_bool");
        columnHint.set_scan_type(cpp2::ScanType::PREFIX);
        decltype(req.contexts[0].column_hints) columnHints;
        columnHints.emplace_back(std::move(columnHint));
        cpp2::IndexQueryContext context1;
        context1.set_column_hints(std::move(columnHints));
        context1.set_filter("");
        context1.set_index_id(3);
        decltype(req.contexts) contexts;
        contexts.emplace_back(std::move(context1));
        req.set_contexts(std::move(contexts));

        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        std::vector<std::string> expectCols = {"_vid", "col_bool", "col_int"};
        auto columns = resp.get_data()->colNames;
        EXPECT_EQ(expectCols, columns);
        auto rows = resp.get_data()->rows;
        EXPECT_EQ(2, rows.size());
        decltype(resp.get_data()->rows) expectRows;

        int64_t vid1 = 1, vid2 = 2;
        std::string vId1, vId2;
        vId1.append(reinterpret_cast<const char*>(&vid1), sizeof(int64_t))
            .append(vIdLen.value() - sizeof(int64_t), '\0');
        vId2.append(reinterpret_cast<const char*>(&vid2), sizeof(int64_t))
            .append(vIdLen.value() - sizeof(int64_t), '\0');
        Row row1;
        row1.columns.emplace_back(Value(vId1));
        row1.columns.emplace_back(Value(true));
        row1.columns.emplace_back(Value(1L));
        expectRows.emplace_back(Row(row1));

        Row row2;
        row2.columns.emplace_back(Value(vId2));
        row2.columns.emplace_back(Value(true));
        row2.columns.emplace_back(Value(1L));
        expectRows.emplace_back(Row(row2));

        EXPECT_EQ(expectRows[0], rows[0]);
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