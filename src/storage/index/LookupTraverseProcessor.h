/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_QUERY_LOOKUP_TRAVERSE_H_
#define STORAGE_QUERY_LOOKUP_TRAVERSE_H_

#include "common/base/Base.h"
#include "storage/index/LookupBaseProcessor.h"
#include "storage/query/GetNeighborsProcessor.h"

namespace nebula {
namespace storage {

class LookupTraverseProcessor
    : public LookupBaseProcessor<cpp2::LookupAndTraverseRequest, cpp2::GetNeighborsResponse> {

public:
    static LookupTraverseProcessor* instance(StorageEnv* env,
                                             stats::Stats* stats,
                                             VertexCache* cache) {
        return new LookupTraverseProcessor(env, stats, cache);
    }

    void process(const cpp2::LookupAndTraverseRequest& req) override;

protected:
    LookupTraverseProcessor(StorageEnv* env,
                            stats::Stats* stats,
                            VertexCache* cache)
        : LookupBaseProcessor<cpp2::LookupAndTraverseRequest,
                              cpp2::GetNeighborsResponse>(env, stats, cache)
        , traverse_(env, stats, cache) {}

    void onProcessFinished() override;

private:
    GetNeighborsProcessor traverse_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_QUERY_LOOKUP_H_
