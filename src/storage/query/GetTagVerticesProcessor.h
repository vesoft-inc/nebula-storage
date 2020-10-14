/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_QUERY_GETTAGVERTICESPROCESSOR_H_
#define STORAGE_QUERY_GETTAGVERTICESPROCESSOR_H_

#include "common/base/Base.h"
#include "storage/query/QueryBaseProcessor.h"

namespace nebula {
namespace storage {

class GetTagVerticesProcessor
    : public QueryBaseProcessor<cpp2::GetTagVerticesRequest, cpp2::GetTagVerticesResponse> {
public:
    static GetTagVerticesProcessor* instance(StorageEnv* env,
                                             stats::Stats* stats) {
        return new GetTagVerticesProcessor(env, stats);
    }

    void process(const cpp2::GetTagVerticesRequest& req) override;

private:
    GetTagVerticesProcessor(StorageEnv* env, stats::Stats* stats)
        : QueryBaseProcessor<cpp2::GetTagVerticesRequest,
                             cpp2::GetTagVerticesResponse>(env, stats) {}

    cpp2::ErrorCode
    checkAndBuildContexts(const cpp2::GetTagVerticesRequest& req) override;

    void onProcessFinished() override;

private:
    // return vertexIds
    std::vector<Value>                          vertices_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_QUERY_GETTAGVERTICESPROCESSOR_H_
