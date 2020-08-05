/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_TRANSACTION_TRANSACTIONProcessor_H_
#define STORAGE_TRANSACTION_TRANSACTIONProcessor_H_

#include <folly/FBVector.h>
#include "common/base/Base.h"
#include "storage/BaseProcessor.h"
#include "storage/StorageFlags.h"
#include "kvstore/LogEncoder.h"

namespace nebula {
namespace storage {

using UUID = int64_t;

class TransactionProcessor : public BaseProcessor<cpp2::ExecResponse> {
public:
    static TransactionProcessor* instance(StorageEnv* env,
                                       stats::Stats* stats) {
        return new TransactionProcessor(env, stats);
    }

    void process(const cpp2::TransactionReq& req);

    // folly::Future<cpp2::ErrorCode> prepare(const cpp2::TransactionReq& req);

    // folly::Future<cpp2::ErrorCode> commit(const cpp2::TransactionReq& req);

private:
    TransactionProcessor(StorageEnv* env, stats::Stats* stats)
        : BaseProcessor<cpp2::ExecResponse>(env, stats) {}
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_TRANSACTION_TRANSACTIONProcessor_H_
