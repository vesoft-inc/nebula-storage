/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_QUERY_GETEDGESSTATISPROCESSOR_H_
#define STORAGE_QUERY_GETEDGESSTATISPROCESSOR_H_

#include "common/base/Base.h"
#include "storage/query/QueryBaseProcessor.h"

namespace nebula {
namespace storage {

class GetEdgesStatisProcessor
    : public QueryBaseProcessor<cpp2::GetEdgesStatisRequest, cpp2::GetStatisResponse> {
public:
    static GetEdgesStatisProcessor* instance(StorageEnv* env,
                                             stats::Stats* stats) {
        return new GetEdgesStatisProcessor(env, stats);
    }

    void process(const cpp2::GetEdgesStatisRequest& req) override;

private:
    GetEdgesStatisProcessor(StorageEnv* env, stats::Stats* stats)
        : QueryBaseProcessor<cpp2::GetEdgesStatisRequest,
                             cpp2::GetStatisResponse>(env, stats) {}

    cpp2::ErrorCode checkAndBuildContexts(const cpp2::GetEdgesStatisRequest& req) override;

    void onProcessFinished() override;

private:
    nebula::meta::cpp2::IndexType               indexType_;
    // return edge count
    int64_t                                     retCount_{0};
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_QUERY_GETEDGESSTATISPROCESSOR_H_
