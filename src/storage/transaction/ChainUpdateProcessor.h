/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "storage/transaction/BaseChainProcessor.h"

namespace nebula {
namespace storage {

class ChainUpdateEdgeProcessor : public BaseChainProcessor {
    using AtomicGetFunc = std::function<folly::Optional<std::string>()>;

public:
    static ChainUpdateEdgeProcessor* instance(StorageEnv* env,
                                              Callback&& cb,
                                              GraphSpaceID spaceId,
                                              PartitionID partId,
                                              const cpp2::EdgeKey& edgeKey,
                                              const std::vector<cpp2::UpdatedProp>& updateProps,
                                              bool insertable,
                                              const std::vector<std::string>& returnProps,
                                              const std::string& condition,
                                              AtomicGetFunc&& getter) {
        return new ChainUpdateEdgeProcessor(env,
                                            std::move(cb),
                                            spaceId,
                                            partId,
                                            edgeKey,
                                            updateProps,
                                            insertable,
                                            returnProps,
                                            condition,
                                            std::move(getter));
    }

    ChainUpdateEdgeProcessor(StorageEnv* env,
                             Callback&& cb,
                             GraphSpaceID spaceId,
                             PartitionID partId,
                             cpp2::EdgeKey edgeKey,
                             std::vector<cpp2::UpdatedProp> updateProps,
                             bool insertable,
                             std::vector<std::string> returnProps,
                             std::string condition,
                             AtomicGetFunc&& getter)
        : BaseChainProcessor(env, std::move(cb)),
          spaceId_(spaceId),
          partId_(partId),
          edgeKey_(std::move(edgeKey)),
          updateProps_(std::move(updateProps)),
          insertable_(insertable),
          returnProps_(std::move(returnProps)),
          condition_(std::move(condition)),
          getter_(std::move(getter)) {
              std::swap(edgeKey_.src, edgeKey_.dst);
              edgeKey_.edge_type = 0 - edgeKey_.edge_type;
          }

    folly::SemiFuture<cpp2::ErrorCode> prepareLocal() override;

    folly::SemiFuture<cpp2::ErrorCode> processRemote(cpp2::ErrorCode code) override;

    folly::SemiFuture<cpp2::ErrorCode> processLocal(cpp2::ErrorCode code) override;

    void cleanup() override {}

    void updateRemoteEdge(folly::Promise<cpp2::ErrorCode>&& promise) noexcept;

protected:
    GraphSpaceID spaceId_{-1};
    PartitionID partId_{-1};
    bool needUnlock_{false};
    cpp2::EdgeKey edgeKey_;
    std::vector<cpp2::UpdatedProp> updateProps_;
    bool insertable_;
    std::vector<std::string> returnProps_;
    std::string condition_;
    AtomicGetFunc getter_;
    std::string sLockKey_;
};

}  // namespace storage
}  // namespace nebula
