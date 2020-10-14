/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_QUERY_GETEDGETYPEEDGESPROCESSOR_H_
#define STORAGE_QUERY_GETEDGETYPEEDGESPROCESSOR_H_

#include "common/base/Base.h"
#include "storage/query/QueryBaseProcessor.h"

namespace nebula {
namespace storage {

class GetEdgetypeEdgesProcessor
    : public QueryBaseProcessor<cpp2::GetEdgetypeEdgesRequest, cpp2::GetEdgetypeEdgesResponse> {
public:
    static GetEdgetypeEdgesProcessor* instance(StorageEnv* env,
                                               stats::Stats* stats) {
        return new GetEdgetypeEdgesProcessor(env, stats);
    }

    void process(const cpp2::GetEdgetypeEdgesRequest& req) override;

private:
    GetEdgetypeEdgesProcessor(StorageEnv* env, stats::Stats* stats)
        : QueryBaseProcessor<cpp2::GetEdgetypeEdgesRequest,
                             cpp2::GetEdgetypeEdgesResponse>(env, stats) {}

    cpp2::ErrorCode checkAndBuildContexts(const cpp2::GetEdgetypeEdgesRequest& req) override;

    void onProcessFinished() override;

private:
    // return edges
    nebula::DataSet                    edges_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_QUERY_GETEDGETYPEEDGESPROCESSOR_H_
