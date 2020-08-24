/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "kvstore/KVStore.h"
#include "kvstore/NebulaStore.h"
#include "common/fs/FileUtils.h"
#include "storage/mutate/AddVerticesProcessor.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"
#include <folly/Benchmark.h>
#include "common/interface/gen-cpp2/storage_types.h"
#include "common/interface/gen-cpp2/common_types.h"
#include "utils/NebulaKeyUtils.h"
#include "mock/AdHocIndexManager.h"
#include "mock/AdHocSchemaManager.h"


DEFINE_int32(bulk_insert_size, 1000, "The number of vertices by bulk insert");
DEFINE_int32(total_vertices_size, 1000, "The number of vertices");
DEFINE_string(root_data_path, "/home/bright2star/IndexWritePref", "Engine data path");

namespace nebula {
namespace storage {

using NewVertex = nebula::storage::cpp2::NewVertex;
using NewTag = nebula::storage::cpp2::NewTag;

enum class IndexENV : uint8_t {
    NO_INDEX       = 1,
    ONE_INDEX      = 2,
    MULITPLE_INDEX = 3,
    INVALID_INDEX  = 4,
};

GraphSpaceID spaceId = 1;
TagID tagId = 1;
IndexID indexId = 1;

std::string toVertexId(size_t vIdLen, int32_t vId) {
    std::string id;
    id.reserve(vIdLen);
    id.append(reinterpret_cast<const char*>(&vId), sizeof(vId))
        .append(vIdLen - sizeof(vId), '\0');
    return id;
}

std::unique_ptr<kvstore::MemPartManager> memPartMan(const std::vector<PartitionID>& parts) {
    auto memPartMan = std::make_unique<kvstore::MemPartManager>();
    // GraphSpaceID =>  {PartitionIDs}
    auto& partsMap = memPartMan->partsMap();
    for (auto partId : parts) {
        partsMap[spaceId][partId] = meta::PartHosts();
    }
    return memPartMan;
}

std::vector<nebula::meta::cpp2::ColumnDef> mockTagIndexColumns() {
    std::vector<nebula::meta::cpp2::ColumnDef> cols;
    for (int32_t i = 0; i < 3; i++) {
        nebula::meta::cpp2::ColumnDef col;
        col.name = folly::stringPrintf("col_%d", i);
        col.type = meta::cpp2::PropertyType::INT64;
        cols.emplace_back(std::move(col));
    }
    return cols;
}

std::shared_ptr<meta::NebulaSchemaProvider> mockTagSchema() {
    std::shared_ptr<meta::NebulaSchemaProvider> schema(new meta::NebulaSchemaProvider(0));
    for (int32_t i = 0; i < 3; i++) {
        nebula::meta::cpp2::ColumnDef col;
        col.name = folly::stringPrintf("col_%d", i);
        col.type = meta::cpp2::PropertyType::INT64;
        schema->addField(col.name, col.type);
    }
    return schema;
}

std::unique_ptr<meta::SchemaManager> memSchemaMan() {
    auto schemaMan = std::make_unique<mock::AdHocSchemaManager>();
    schemaMan->addTagSchema(spaceId, tagId, mockTagSchema());
    return schemaMan;
}

std::unique_ptr<meta::IndexManager> memIndexMan(IndexENV type) {
    auto indexMan = std::make_unique<mock::AdHocIndexManager>();
    switch (type) {
        case IndexENV::NO_INDEX :
            break;
        case IndexENV::ONE_INDEX : {
            indexMan->addTagIndex(spaceId, indexId, tagId, mockTagIndexColumns());
            break;
        }
        case IndexENV::INVALID_INDEX : {
            indexMan->addTagIndex(spaceId, indexId, -1, mockTagIndexColumns());
            break;
        }
        case IndexENV::MULITPLE_INDEX : {
            indexMan->addTagIndex(spaceId, indexId, tagId, mockTagIndexColumns());
            indexMan->addTagIndex(spaceId, indexId + 1, tagId, mockTagIndexColumns());
            indexMan->addTagIndex(spaceId, indexId + 2, tagId, mockTagIndexColumns());
            break;
        }
    }
    return indexMan;
}

std::vector<NewVertex> genVertices(size_t vLen, int32_t &vId) {
    std::vector<NewVertex> vertices;
    for (auto i = 0; i < FLAGS_bulk_insert_size; i++) {
        NewVertex newVertex;
        NewTag newTag;
        newTag.set_tag_id(tagId);
        std::vector<Value>  props;
        props.emplace_back(Value(1L + i));
        props.emplace_back(Value(2L + i));
        props.emplace_back(Value(3L + i));
        newTag.set_props(std::move(props));
        std::vector<nebula::storage::cpp2::NewTag> newTags;
        newTags.push_back(std::move(newTag));
        newVertex.set_id(toVertexId(vLen, vId++));
        newVertex.set_tags(std::move(newTags));
        vertices.emplace_back(std::move(newVertex));
    }
    return vertices;
}

bool processVertices(StorageEnv* env, int32_t &vId) {
    cpp2::AddVerticesRequest req;
    BENCHMARK_SUSPEND {
        req.set_space_id(1);
        req.set_overwritable(true);
        auto newVertex = genVertices(32, vId);
        req.parts[1] = std::move(newVertex);
    };

    auto* processor = AddVerticesProcessor::instance(env, nullptr);
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    BENCHMARK_SUSPEND {
        if (!resp.result.failed_parts.empty()) {
            return false;
        }
    };
    return true;
}

void initEnv(IndexENV type,
             const std::string& dataPath,
             std::unique_ptr<storage::StorageEnv>& env,
             std::unique_ptr<kvstore::NebulaStore>& kv,
             std::unique_ptr<meta::SchemaManager>& sm,
             std::unique_ptr<meta::IndexManager>& im) {
    env = std::make_unique<StorageEnv>();
    const std::vector<PartitionID> parts{1};
    kvstore::KVOptions options;
    HostAddr localHost = HostAddr("0", 0);
    LOG(INFO) << "Use meta in memory!";
    options.partMan_ = memPartMan(parts);
    std::vector<std::string> paths;
    paths.emplace_back(folly::stringPrintf("%s/disk1", dataPath.c_str()));
    paths.emplace_back(folly::stringPrintf("%s/disk2", dataPath.c_str()));
    options.dataPaths_ = std::move(paths);
    kv = mock::MockCluster::initKV(std::move(options), localHost);
    mock::MockCluster::waitUntilAllElected(kv.get(), 1, parts);
    sm = memSchemaMan();
    im = memIndexMan(type);
    env->schemaMan_ = std::move(sm).get();
    env->indexMan_ = std::move(im).get();
    env->kvstore_ = std::move(kv).get();
}

void insertVertices(bool withoutIndex) {
    std::unique_ptr<storage::StorageEnv> env;
    std::unique_ptr<kvstore::NebulaStore> kv;
    std::unique_ptr<meta::SchemaManager> sm;
    std::unique_ptr<meta::IndexManager> im;
    int32_t vId = 0;
    BENCHMARK_SUSPEND {
        std::string dataPath = withoutIndex
                               ? folly::stringPrintf("%s/%s", FLAGS_root_data_path.c_str(),
                                                     "withoutIndex")
                               : folly::stringPrintf("%s/%s", FLAGS_root_data_path.c_str(),
                                                     "attachIndex");
        auto type = withoutIndex ? IndexENV::NO_INDEX : IndexENV::ONE_INDEX;
        initEnv(type, dataPath, env, kv, sm, im);
    };

    while (vId < FLAGS_total_vertices_size) {
        if (!processVertices(env.get(), vId)) {
            LOG(ERROR) << "Vertices bulk insert error";
            return;
        }
    }
    BENCHMARK_SUSPEND {
        if (withoutIndex) {
            auto prefix = NebulaKeyUtils::partPrefix(1);
            std::unique_ptr<kvstore::KVIterator> iter;
            auto status = env->kvstore_->prefix(spaceId, 1, prefix, &iter);
            if (status != kvstore::ResultCode::SUCCEEDED) {
                LOG(ERROR) << "Index scan error";
                return;
            }
            int32_t cnt = 0;
            while (iter->valid()) {
                cnt++;
                iter->next();
            }
            if (cnt != FLAGS_total_vertices_size) {
                LOG(ERROR) << "Vertices insert error , expected : "
                           << FLAGS_total_vertices_size
                           << "actual : " << cnt;
            }
        } else {
            {
                auto prefix = NebulaKeyUtils::partPrefix(1);
                std::unique_ptr<kvstore::KVIterator> iter;
                auto status = env->kvstore_->prefix(spaceId, 1, prefix, &iter);
                if (status != kvstore::ResultCode::SUCCEEDED) {
                    LOG(ERROR) << "Index scan error";
                    return;
                }
                int32_t cnt = 0;
                while (iter->valid()) {
                    cnt++;
                    iter->next();
                }
                if (cnt != FLAGS_total_vertices_size) {
                    LOG(ERROR) << "Vertices insert error , expected : "
                               << FLAGS_total_vertices_size
                               << "actual : " << cnt;
                    return;
                }
            }
            {
                auto prefix = IndexKeyUtils::indexPrefix(1, 1);
                std::unique_ptr<kvstore::KVIterator> iter;
                auto status = env->kvstore_->prefix(spaceId, 1, prefix, &iter);
                if (status != kvstore::ResultCode::SUCCEEDED) {
                    LOG(ERROR) << "Index scan error";
                    return;
                }
                int32_t cnt = 0;
                while (iter->valid()) {
                    cnt++;
                    iter->next();
                }
                if (cnt != FLAGS_total_vertices_size) {
                    LOG(ERROR) << "Indexes number error , expected : "
                               << FLAGS_total_vertices_size
                               << "actual : " << cnt;
                    return;
                }
            }
        }
    };
    BENCHMARK_SUSPEND {
        im.reset();
        kv.reset();
        sm.reset();
        env.reset();
        fs::FileUtils::remove(FLAGS_root_data_path.c_str(), true);
    };
}

void insertUnmatchIndex() {
    std::unique_ptr<storage::StorageEnv> env;
    std::unique_ptr<kvstore::NebulaStore> kv;
    std::unique_ptr<meta::SchemaManager> sm;
    std::unique_ptr<meta::IndexManager> im;
    int32_t vId = 0;
    BENCHMARK_SUSPEND {
        std::string dataPath = folly::stringPrintf("%s/%s", FLAGS_root_data_path.c_str(),
                                                   "unmatchIndex");
        initEnv(IndexENV::INVALID_INDEX, dataPath, env, kv, sm, im);
    };

    while (vId < FLAGS_total_vertices_size) {
        if (!processVertices(env.get(), vId)) {
            LOG(ERROR) << "Vertices bulk insert error";
            return;
        }
    }
    BENCHMARK_SUSPEND {
        {
            auto prefix = NebulaKeyUtils::partPrefix(1);
            std::unique_ptr<kvstore::KVIterator> iter;
            auto status = env->kvstore_->prefix(spaceId, 1, prefix, &iter);
            if (status != kvstore::ResultCode::SUCCEEDED) {
                LOG(ERROR) << "Index scan error";
                return;
            }
            int32_t cnt = 0;
            while (iter->valid()) {
                cnt++;
                iter->next();
            }
            if (cnt != FLAGS_total_vertices_size) {
                LOG(ERROR) << "Vertices insert error , expected : "
                           << FLAGS_total_vertices_size
                           << "actual : " << cnt;
                return;
            }
        }
        {
            auto prefix = IndexKeyUtils::indexPrefix(spaceId, 1);
            std::unique_ptr<kvstore::KVIterator> iter;
            auto status = env->kvstore_->prefix(spaceId, 1, prefix, &iter);
            if (status != kvstore::ResultCode::SUCCEEDED) {
                LOG(ERROR) << "Index scan error";
                return;
            }
            int32_t cnt = 0;
            while (iter->valid()) {
                cnt++;
                iter->next();
            }
            if (cnt != 0) {
                LOG(ERROR) << "Indexes number error , expected : "
                           << FLAGS_total_vertices_size
                           << "actual : " << cnt;
                return;
            }
        }
    };
    BENCHMARK_SUSPEND {
        im.reset();
        kv.reset();
        sm.reset();
        env.reset();
        fs::FileUtils::remove(FLAGS_root_data_path.c_str(), true);
    };
}

void insertDupVertices() {
    std::unique_ptr<storage::StorageEnv> env;
    std::unique_ptr<kvstore::NebulaStore> kv;
    std::unique_ptr<meta::SchemaManager> sm;
    std::unique_ptr<meta::IndexManager> im;
    int32_t vId = 0;
    BENCHMARK_SUSPEND {
        std::string dataPath = folly::stringPrintf("%s/%s",
                                                   FLAGS_root_data_path.c_str(),
                                                   "duplicateIndex");
        initEnv(IndexENV::ONE_INDEX, dataPath, env, kv, sm, im);
    };

    while (vId < FLAGS_total_vertices_size) {
        if (!processVertices(env.get(), vId)) {
            LOG(ERROR) << "Vertices bulk insert error";
            return;
        }
    }

    vId = 0;
    while (vId < FLAGS_total_vertices_size) {
        if (!processVertices(env.get(), vId)) {
            LOG(ERROR) << "Vertices bulk insert error";
            return;
        }
    }

    BENCHMARK_SUSPEND {
        {
            auto prefix = NebulaKeyUtils::partPrefix(1);
            std::unique_ptr<kvstore::KVIterator> iter;
            auto status = env->kvstore_->prefix(spaceId, 1, prefix, &iter);
            if (status != kvstore::ResultCode::SUCCEEDED) {
                LOG(ERROR) << "Index scan error";
                return;
            }
            int32_t cnt = 0;
            while (iter->valid()) {
                cnt++;
                iter->next();
            }
            if (cnt != FLAGS_total_vertices_size * 2) {
                LOG(ERROR) << "Vertices insert error , expected : "
                           << FLAGS_total_vertices_size
                           << "actual : " << cnt;
                return;
            }
        }
        {
            auto prefix = IndexKeyUtils::indexPrefix(1, 1);
            std::unique_ptr<kvstore::KVIterator> iter;
            auto status = env->kvstore_->prefix(spaceId, 1, prefix, &iter);
            if (status != kvstore::ResultCode::SUCCEEDED) {
                LOG(ERROR) << "Index scan error";
                return;
            }
            int32_t cnt = 0;
            while (iter->valid()) {
                cnt++;
                iter->next();
            }
            if (cnt != FLAGS_total_vertices_size) {
                LOG(ERROR) << "Indexes number error , expected : "
                           << FLAGS_total_vertices_size
                           << "actual : " << cnt;
                return;
            }
        }
    };
    BENCHMARK_SUSPEND {
        im.reset();
        kv.reset();
        sm.reset();
        env.reset();
        fs::FileUtils::remove(FLAGS_root_data_path.c_str(), true);
    };
}

void insertVerticesMultIndex() {
    std::unique_ptr<storage::StorageEnv> env;
    std::unique_ptr<kvstore::NebulaStore> kv;
    std::unique_ptr<meta::SchemaManager> sm;
    std::unique_ptr<meta::IndexManager> im;
    int32_t vId = 0;
    BENCHMARK_SUSPEND {
        std::string dataPath = folly::stringPrintf("%s/%s",
                                                   FLAGS_root_data_path.c_str(),
                                                   "multIndex");
        initEnv(IndexENV::MULITPLE_INDEX, dataPath, env, kv, sm, im);
    };

    while (vId < FLAGS_total_vertices_size) {
        if (!processVertices(env.get(), vId)) {
            LOG(ERROR) << "Vertices bulk insert error";
            return;
        }
    }

    BENCHMARK_SUSPEND {
        {
            auto prefix = NebulaKeyUtils::partPrefix(1);
            std::unique_ptr<kvstore::KVIterator> iter;
            auto status = env->kvstore_->prefix(spaceId, 1, prefix, &iter);
            if (status != kvstore::ResultCode::SUCCEEDED) {
                LOG(ERROR) << "Index scan error";
                return;
            }
            int32_t cnt = 0;
            while (iter->valid()) {
                cnt++;
                iter->next();
            }
            if (cnt != FLAGS_total_vertices_size) {
                LOG(ERROR) << "Vertices insert error , expected : "
                           << FLAGS_total_vertices_size
                           << "actual : " << cnt;
                return;
            }
        }
        {
            PartitionID partId = 1;
            PartitionID item = (partId << 8) | static_cast<uint32_t>(NebulaKeyType::kIndex);
            std::string prefix;
            prefix.reserve(sizeof(PartitionID));
            prefix.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID));
            std::unique_ptr<kvstore::KVIterator> iter;
            auto status = env->kvstore_->prefix(spaceId, 1, prefix, &iter);
            if (status != kvstore::ResultCode::SUCCEEDED) {
                LOG(ERROR) << "Index scan error";
                return;
            }
            int32_t cnt = 0;
            while (iter->valid()) {
                cnt++;
                iter->next();
            }
            if (cnt != FLAGS_total_vertices_size * 3) {
                LOG(ERROR) << "Indexes number error , expected : "
                           << FLAGS_total_vertices_size * 3
                           << "actual : " << cnt;
                return;
            }
        }
    };
    BENCHMARK_SUSPEND {
        im.reset();
        kv.reset();
        sm.reset();
        env.reset();
        fs::FileUtils::remove(FLAGS_root_data_path.c_str(), true);
    };
}

BENCHMARK(withoutIndex) {
    insertVertices(true);
}

BENCHMARK(unmatchIndex) {
    insertUnmatchIndex();
}

BENCHMARK(attachIndex) {
    insertVertices(false);
}

BENCHMARK(duplicateVerticesIndex) {
    insertDupVertices();
}

BENCHMARK(multipleIndex) {
    insertVerticesMultIndex();
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
    folly::init(&argc, &argv, true);
    folly::runBenchmarks();
    return 0;
}

/**
withoutIndex: Without index, and doesn't through way asyncAtomicOp.
unmatchIndex: Without match index, and through asyncAtomicOp.
attachIndex: One index, the index contains all the columns of tag.
duplicateVerticesIndex: One index, and insert deplicate vertices.
multipleIndex: Three indexes by one tag.
V 1.0
--total_vertices_size=2000000
============================================================================
src/storage/test/StorageIndexWriteBenchmark.cpprelative  time/iter  iters/s
============================================================================
withoutIndex                                                  7.06s  141.71m
unmatchIndex                                                  9.50s  105.25m
attachIndex                                                  57.87s   17.28m
duplicateVerticesIndex                                      2.82min    5.92m
multipleIndex                                               1.96min    8.51m
============================================================================
--total_vertices_size=1000000
============================================================================
src/storage/test/StorageIndexWriteBenchmark.cpprelative  time/iter  iters/s
============================================================================
withoutIndex                                                  3.42s  292.08m
unmatchIndex                                                  4.64s  215.37m
attachIndex                                                  26.83s   37.27m
duplicateVerticesIndex                                      1.35min   12.37m
multipleIndex                                                57.88s   17.28m
============================================================================
--total_vertices_size=100000
============================================================================
src/storage/test/StorageIndexWriteBenchmark.cpprelative  time/iter  iters/s
============================================================================
withoutIndex                                               302.34ms     3.31
unmatchIndex                                               411.87ms     2.43
attachIndex                                                   2.24s  447.23m
duplicateVerticesIndex                                        5.95s  167.96m
multipleIndex                                                 3.36s  297.95m
============================================================================
--total_vertices_size=10000
============================================================================
src/storage/test/StorageIndexWriteBenchmark.cpprelative  time/iter  iters/s
============================================================================
withoutIndex                                                28.02ms    35.69
unmatchIndex                                                42.00ms    23.81
attachIndex                                                233.70ms     4.28
duplicateVerticesIndex                                     578.98ms     1.73
multipleIndex                                              438.78ms     2.28
============================================================================
--total_vertices_size=1000
============================================================================
src/storage/test/StorageIndexWriteBenchmark.cpprelative  time/iter  iters/s
============================================================================
withoutIndex                                                 2.80ms   357.39
unmatchIndex                                                 4.93ms   202.65
attachIndex                                                 29.17ms    34.28
duplicateVerticesIndex                                      75.71ms    13.21
multipleIndex                                               42.66ms    23.44
============================================================================

V 2.0
--total_vertices_size=2000000
============================================================================
src/storage/test/StorageIndexWriteBenchmark.cpprelative  time/iter  iters/s
============================================================================
withoutIndex                                                  5.85s  170.92m
unmatchIndex                                                  6.23s  160.60m
attachIndex                                                  20.57s   48.61m
duplicateVerticesIndex                                      1.01min   16.56m
multipleIndex                                               1.03min   16.20m
============================================================================
--total_vertices_size=1000000
============================================================================
src/storage/test/StorageIndexWriteBenchmark.cpprelative  time/iter  iters/s
============================================================================
withoutIndex                                                  2.75s  364.19m
unmatchIndex                                                  3.52s  284.10m
attachIndex                                                   8.39s  119.24m
duplicateVerticesIndex                                       23.06s   43.37m
multipleIndex                                                26.67s   37.50m
============================================================================
--total_vertices_size=100000
============================================================================
src/storage/test/StorageIndexWriteBenchmark.cpprelative  time/iter  iters/s
============================================================================
withoutIndex                                               249.19ms     4.01
unmatchIndex                                               262.50ms     3.81
attachIndex                                                755.14ms     1.32
duplicateVerticesIndex                                        1.77s  564.09m
multipleIndex                                                 1.23s  813.10m
============================================================================
--total_vertices_size=10000
============================================================================
src/storage/test/StorageIndexWriteBenchmark.cpprelative  time/iter  iters/s
============================================================================
withoutIndex                                                24.60ms    40.65
unmatchIndex                                                28.94ms    34.55
attachIndex                                                 70.85ms    14.11
duplicateVerticesIndex                                     162.35ms     6.16
multipleIndex                                              142.78ms     7.00
============================================================================
--total_vertices_size=1000
============================================================================
src/storage/test/StorageIndexWriteBenchmark.cpprelative  time/iter  iters/s
============================================================================
withoutIndex                                                 2.97ms   336.30
unmatchIndex                                                 4.19ms   238.76
attachIndex                                                 10.24ms    97.65
duplicateVerticesIndex                                      19.40ms    51.55
multipleIndex                                               19.13ms    52.29
============================================================================
**/
