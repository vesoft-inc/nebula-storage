/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_SIMPLECONCURRENTJOBEXECUTOR_H_
#define META_SIMPLECONCURRENTJOBEXECUTOR_H_

#include "common/interface/gen-cpp2/common_types.h"
#include "meta/processors/jobMan/MetaJobExecutor.h"

namespace nebula {
namespace meta {

class SimpleConcurrentJobExecutor : public MetaJobExecutor {
public:
    SimpleConcurrentJobExecutor(JobID jobId,
                                kvstore::KVStore* kvstore,
                                AdminClient* adminClient,
                                std::vector<std::string> params);

    bool check() override;

    cpp2::ErrorCode prepare() override;

    cpp2::ErrorCode stop() override;

protected:
    int32_t        taskId_{0};
    int32_t        concurrency_{INT_MAX};
};

}  // namespace meta
}  // namespace nebula

#endif  // META_SIMPLECONCURRENTJOBEXECUTOR_H_
