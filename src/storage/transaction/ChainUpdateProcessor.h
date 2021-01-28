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
                                              cpp2::UpdateEdgeRequest&& req,
                                              AtomicGetFunc&& getter) {
        return new ChainUpdateEdgeProcessor(env, std::move(cb), req, std::move(getter));
    }

    ChainUpdateEdgeProcessor(StorageEnv* env,
                             Callback&& cb,
                             cpp2::UpdateEdgeRequest req,
                             AtomicGetFunc&& getter)
        : BaseChainProcessor(env, std::move(cb)),
          req_(std::move(req)), getter_(std::move(getter)) {}

    folly::SemiFuture<cpp2::ErrorCode> prepareLocal() override;

    folly::SemiFuture<cpp2::ErrorCode> processRemote(cpp2::ErrorCode code) override;

    folly::SemiFuture<cpp2::ErrorCode> processLocal(cpp2::ErrorCode code) override;

    void cleanup() override {}

protected:
    GraphSpaceID spaceId_{-1};
    PartitionID partId_{-1};
    cpp2::UpdateEdgeRequest req_;
    AtomicGetFunc getter_;
    bool needUnlock_{false};
};

}  // namespace storage
}  // namespace nebula
