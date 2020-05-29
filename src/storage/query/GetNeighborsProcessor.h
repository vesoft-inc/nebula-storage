/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_QUERY_GETNEIGHBORSPROCESSOR_H_
#define STORAGE_QUERY_GETNEIGHBORSPROCESSOR_H_

#include "common/base/Base.h"
#include <gtest/gtest_prod.h>
#include "storage/query/QueryBaseProcessor.h"
#include "storage/exec/FilterContext.h"
#include "storage/exec/StoragePlan.h"

namespace nebula {
namespace storage {

class GetNeighborsProcessor
    : public QueryBaseProcessor<cpp2::GetNeighborsRequest, cpp2::GetNeighborsResponse> {
    FRIEND_TEST(ScanEdgePropBench, EdgeTypePrefixScanVsVertexPrefixScan);

public:
    static GetNeighborsProcessor* instance(StorageEnv* env,
                                           stats::Stats* stats,
                                           VertexCache* cache) {
        return new GetNeighborsProcessor(env, stats, cache);
    }

    void process(const cpp2::GetNeighborsRequest& req) override;

protected:
    GetNeighborsProcessor(StorageEnv* env,
                          stats::Stats* stats,
                          VertexCache* cache)
        : QueryBaseProcessor<cpp2::GetNeighborsRequest,
                             cpp2::GetNeighborsResponse>(env, stats, cache) {}

    StoragePlan<VertexID> buildDAG(nebula::DataSet* result);

    void onProcessFinished() override;

    cpp2::ErrorCode checkAndBuildContexts(const cpp2::GetNeighborsRequest& req) override;
    cpp2::ErrorCode buildTagContext(const cpp2::GetNeighborsRequest& req);
    cpp2::ErrorCode buildEdgeContext(const cpp2::GetNeighborsRequest& req);

    // add PropContext of stat
    cpp2::ErrorCode handleEdgeStatProps(const std::vector<cpp2::StatProp>& statProps);
    cpp2::ErrorCode checkStatType(const meta::cpp2::PropertyType& fType, cpp2::StatType statType);
    PropContext buildPropContextWithStat(const std::string& name,
                                         size_t idx,
                                         const cpp2::StatType& statType,
                                         const meta::SchemaProviderIf::Field* field);
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_QUERY_GETNEIGHBORSPROCESSOR_H_
