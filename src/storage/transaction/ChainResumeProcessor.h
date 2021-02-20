/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "utils/NebulaKeyUtils.h"
#include "storage/transaction/TransactionTypes.h"
#include "storage/transaction/TransactionManager.h"
#include "storage/transaction/BaseChainProcessor.h"
#include "common/clients/storage/InternalStorageClient.h"

namespace nebula {
namespace storage {

class ChainResumeProcessor : public BaseChainProcessor {
    using LockGuard = nebula::MemoryLockGuard<std::string>;

public:
    static ChainResumeProcessor* instance(StorageEnv* env,
                                          Callback&& cb,
                                          int32_t vIdLen,
                                          GraphSpaceID spaceId,
                                          std::shared_ptr<PendingLock>& lock) {
        return new ChainResumeProcessor(env, std::move(cb), vIdLen, spaceId, lock);
    }

    ChainResumeProcessor(StorageEnv* env,
                         Callback&& cb,
                         int32_t vIdLen,
                         GraphSpaceID spaceId,
                         std::shared_ptr<PendingLock> lock)
        : BaseChainProcessor(env, std::move(cb)), vIdLen_(vIdLen), spaceId_(spaceId), lock_(lock) {
            partId_ = NebulaKeyUtils::getPart(lock_->lockKey);
            iClient_ = env_->txnMan_->getInternalClient();
        }

    folly::SemiFuture<cpp2::ErrorCode> prepareLocal() override;

    folly::SemiFuture<cpp2::ErrorCode> processRemote(cpp2::ErrorCode code) override;

    folly::SemiFuture<cpp2::ErrorCode> processLocal(cpp2::ErrorCode code) override;

protected:
    int32_t vIdLen_;
    GraphSpaceID spaceId_{-1};
    PartitionID partId_{-1};
    std::shared_ptr<PendingLock> lock_;
    InternalStorageClient* iClient_;
    std::unique_ptr<LockGuard> lk_;
};

}  // namespace storage
}  // namespace nebula
