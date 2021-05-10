/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_INGESTJOBEXECUTOR_H_
#define META_INGESTJOBEXECUTOR_H_

#include "meta/processors/jobMan/SimpleConcurrentJobExecutor.h"

namespace nebula {
namespace meta {

class IngestJobExecutor : public SimpleConcurrentJobExecutor {
public:
    IngestJobExecutor(JobID jobId,
                     kvstore::KVStore* kvstore,
                     AdminClient* adminClient,
                     const std::vector<std::string>& params);

    bool check() override;

    nebula::cpp2::ErrorCode prepare() override;

    folly::Future<Status> executeInternal(HostAddr&& address,
                                          std::vector<PartitionID>&& parts) override;

private:
    std::set<std::string> storageHosts_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_INGESTJOBEXECUTOR_H_
