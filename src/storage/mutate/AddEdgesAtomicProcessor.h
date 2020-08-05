/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_MUTATE_ADDEDGESATOMICPROCESSOR_H_
#define STORAGE_MUTATE_ADDEDGESATOMICPROCESSOR_H_

#include "common/base/Base.h"
#include "storage/BaseProcessor.h"
#include "storage/StorageFlags.h"
#include "kvstore/LogEncoder.h"

namespace nebula {
namespace storage {

using UUID = int64_t;

class AddEdgesAtomicProcessor : public BaseProcessor<cpp2::ExecResponse> {
public:
    static AddEdgesAtomicProcessor* instance(StorageEnv* env,
                                       stats::Stats* stats) {
        return new AddEdgesAtomicProcessor(env, stats);
    }

    void process(const cpp2::AddEdgesRequest& req);

    folly::Future<cpp2::ErrorCode>
    processPartition(int spaceId,
                     int partId,
                     int spaceVidLen,
                     std::vector<cpp2::NewEdge> edges, int version);

    folly::SemiFuture<cpp2::ErrorCode>
    addSingleEdge(int spaceId, int partId, int spaceVidLen, cpp2::NewEdge edge, int ver);

private:
    AddEdgesAtomicProcessor(StorageEnv* env, stats::Stats* stats)
        : BaseProcessor<cpp2::ExecResponse>(env, stats) {}

    void appendEdgeInfo(cpp2::TransactionReq& req, const cpp2::NewEdge& e);

    std::vector<std::string> propNames_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_MUTATE_ADDEDGESATOMICPROCESSOR_H_
