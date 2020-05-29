/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_QUERY_GETPROPPROCESSOR_H_
#define STORAGE_QUERY_GETPROPPROCESSOR_H_

#include "common/base/Base.h"
#include <gtest/gtest_prod.h>
#include "storage/query/QueryBaseProcessor.h"
#include "storage/exec/StorageDAG.h"

namespace nebula {
namespace storage {

class GetPropProcessor
    : public QueryBaseProcessor<cpp2::GetPropRequest, cpp2::GetPropResponse> {
public:
    static GetPropProcessor* instance(StorageEnv* env,
                                      stats::Stats* stats,
                                      VertexCache* cache) {
        return new GetPropProcessor(env, stats, cache);
    }

    void process(const cpp2::GetPropRequest& req) override;

protected:
    GetPropProcessor(StorageEnv* env,
                     stats::Stats* stats,
                     VertexCache* cache)
        : QueryBaseProcessor<cpp2::GetPropRequest,
                             cpp2::GetPropResponse>(env, stats, cache) {}

    StorageDAG<VertexID> buildTagDAG(nebula::DataSet* result);

    StorageDAG<cpp2::EdgeKey> buildEdgeDAG(nebula::DataSet* result);

    void onProcessFinished() override;

    cpp2::ErrorCode checkAndBuildContexts(const cpp2::GetPropRequest& req) override;

    cpp2::ErrorCode checkColumnNames(const std::vector<std::string>& colNames);

    cpp2::ErrorCode buildTagContext(const cpp2::GetPropRequest& req);

    cpp2::ErrorCode buildEdgeContext(const cpp2::GetPropRequest& req);

    kvstore::ResultCode processOneVertex(PartitionID partId, const std::string& prefix);

private:
    bool isEdge_ = false;                   // true for edge, false for tag
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_QUERY_GETPROPPROCESSOR_H_
