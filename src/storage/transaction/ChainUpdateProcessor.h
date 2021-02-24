/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "storage/transaction/BaseChainProcessor.h"
#include "storage/transaction/TransactionManager.h"

namespace nebula {
namespace storage {

class ChainUpdateEdgeProcessor : public BaseChainProcessor {
    using LockGuard = nebula::MemoryLockGuard<std::string>;
    using Getter = TransactionManager::UpdateEdgeGetter;

public:
    ChainUpdateEdgeProcessor(StorageEnv* env,
                             Callback&& cb,
                             GraphSpaceID spaceId,
                             PartitionID partId,
                             cpp2::EdgeKey edgeKey,
                             std::vector<cpp2::UpdatedProp> updateProps,
                             bool insertable,
                             std::vector<std::string> returnProps,
                             std::string condition,
                             meta::cpp2::PropertyType spaceVidType,
                             Getter&& getter)
        : BaseChainProcessor(std::move(cb), env),
          spaceId_(spaceId),
          partId_(partId),
          inEdgeKey_(std::move(edgeKey)),
          updateProps_(std::move(updateProps)),
          insertable_(insertable),
          returnProps_(std::move(returnProps)),
          condition_(std::move(condition)),
          spaceVidType_(spaceVidType),
          getter_(std::move(getter)) {}

    folly::SemiFuture<cpp2::ErrorCode> prepareLocal() override;

    folly::SemiFuture<cpp2::ErrorCode> processRemote(cpp2::ErrorCode code) override;

    folly::SemiFuture<cpp2::ErrorCode> processLocal(cpp2::ErrorCode code) override;

    static ChainUpdateEdgeProcessor* instance(StorageEnv* env,
                                              Callback&& cb,
                                              GraphSpaceID spaceId,
                                              PartitionID partId,
                                              const cpp2::EdgeKey& edgeKey,
                                              const std::vector<cpp2::UpdatedProp>& updateProps,
                                              bool insertable,
                                              const std::vector<std::string>& returnProps,
                                              const std::string& condition,
                                              meta::cpp2::PropertyType spaceVidType,
                                              Getter&& getter) {
        return new ChainUpdateEdgeProcessor(env,
                                            std::move(cb),
                                            spaceId,
                                            partId,
                                            edgeKey,
                                            updateProps,
                                            insertable,
                                            returnProps,
                                            condition,
                                            spaceVidType,
                                            std::move(getter));
    }

private:
    void updateRemoteEdge(folly::Promise<cpp2::ErrorCode>&& promise) noexcept;

protected:
    GraphSpaceID spaceId_{-1};
    PartitionID partId_{-1};
    cpp2::EdgeKey inEdgeKey_;
    std::vector<cpp2::UpdatedProp> updateProps_;
    bool insertable_;
    std::vector<std::string> returnProps_;
    std::string condition_;
    meta::cpp2::PropertyType spaceVidType_;
    Getter getter_;
    std::string sLockKey_;
    folly::Optional<std::string> optVal_;
    std::unique_ptr<LockGuard> lk_;
};

}  // namespace storage
}  // namespace nebula
