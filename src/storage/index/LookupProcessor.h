/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_QUERY_LOOKUP_H_
#define STORAGE_QUERY_LOOKUP_H_

#include "common/base/Base.h"
#include "storage/index/LookupBaseProcessor.h"

namespace nebula {
namespace storage {

class LookupProcessor
    : public LookupBaseProcessor<cpp2::LookupIndexRequest, cpp2::LookupIndexResp> {

public:
    static LookupProcessor* instance(StorageEnv* env,
                                     stats::Stats* stats,
                                     VertexCache* cache) {
        return new LookupProcessor(env, stats, cache);
    }

    void process(const cpp2::LookupIndexRequest& req) override;

protected:
    LookupProcessor(StorageEnv* env,
                    stats::Stats* stats,
                    VertexCache* cache)
        : LookupBaseProcessor<cpp2::LookupIndexRequest,
                              cpp2::LookupIndexResp>(env, stats, cache) {}

    void onProcessFinished() override;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_QUERY_LOOKUP_H_
