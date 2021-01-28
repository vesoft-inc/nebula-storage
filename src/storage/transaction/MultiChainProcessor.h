/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_TRANSACTION_MULTICHAINPROCESSOR_H_
#define STORAGE_TRANSACTION_MULTICHAINPROCESSOR_H_

#include "storage/transaction/BaseChainProcessor.h"

namespace nebula {
namespace storage {

class MultiChainProcessor : public BaseChainProcessor {
public:
    static MultiChainProcessor* instance(Callback&& cb) {
        return new MultiChainProcessor(nullptr, std::move(cb));
    }

    MultiChainProcessor(StorageEnv* env, Callback&& cb)
        : BaseChainProcessor(env, std::move(cb)) {}

    folly::SemiFuture<cpp2::ErrorCode> prepareLocal() override;

    folly::SemiFuture<cpp2::ErrorCode> processRemote(cpp2::ErrorCode code) override;

    folly::SemiFuture<cpp2::ErrorCode> processLocal(cpp2::ErrorCode code) override;

    void onFinished() override;

    void cleanup() override;

    void addChainProcessor(BaseChainProcessor* processor);

protected:
    std::vector<BaseChainProcessor*> processors_;
    std::vector<folly::SemiFuture<cpp2::ErrorCode>> results_;
};

}   // namespace storage
}   // namespace nebula

#endif  // STORAGE_TRANSACTION_MULTICHAINPROCESSOR_H_
