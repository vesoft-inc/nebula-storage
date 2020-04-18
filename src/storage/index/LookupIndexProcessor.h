/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_LOOKUPINDEXPROCESSOR_H
#define STORAGE_LOOKUPINDEXPROCESSOR_H

#include "storage/index/IndexExecutor.h"

namespace nebula {
namespace storage {

class LookupIndexProcessor: public IndexExecutor<cpp2::LookupIndexResp> {
public:
    static LookupIndexProcessor* instance(StorageEnv* env,
                                          stats::Stats* stats,
                                          VertexCache* cache) {
        return new LookupIndexProcessor(env, stats, cache);
    }

    void process(const cpp2::LookupIndexRequest& req);

private:
    explicit LookupIndexProcessor(StorageEnv* env,
                                  stats::Stats* stats,
                                  VertexCache* cache)
        : IndexExecutor<cpp2::LookupIndexResp>(env, stats, cache) {}
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_LOOKUPINDEXPROCESSOR_H
