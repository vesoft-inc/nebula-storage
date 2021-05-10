/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_DOWNLOADJOBEXECUTOR_H_
#define META_DOWNLOADJOBEXECUTOR_H_

#include "common/hdfs/HdfsCommandHelper.h"
#include "meta/processors/jobMan/MetaJobExecutor.h"

namespace nebula {
namespace meta {

class DownloadJobExecutor : public MetaJobExecutor {
public:
    DownloadJobExecutor(JobID jobId,
                        kvstore::KVStore* kvstore,
                        AdminClient* adminClient,
                        const std::vector<std::string>& params);

    bool check() override;

    nebula::cpp2::ErrorCode prepare() override;

    nebula::cpp2::ErrorCode stop() override;

protected:
    folly::Future<Status> executeInternal(HostAddr&& address,
                                          std::vector<PartitionID>&& parts) override;

private:
    std::unique_ptr<std::string>                host_;
    int32_t                                     port_;
    std::unique_ptr<std::string>                path_;
    std::unique_ptr<nebula::hdfs::HdfsHelper>   helper_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_DOWNLOADJOBEXECUTOR_H_
