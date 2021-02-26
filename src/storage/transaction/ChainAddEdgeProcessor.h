/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "storage/transaction/BaseChainProcessor.h"

namespace nebula {
namespace storage {

class ChainAddEdgesProcessor : public BaseChainProcessor {
    using Encoder = std::function<std::pair<std::string, cpp2::ErrorCode>(const cpp2::NewEdge& e)>;

public:
    ChainAddEdgesProcessor(Callback cb,
                           StorageEnv* env,
                           int32_t vIdLen,
                           GraphSpaceID spaceId,
                           PartitionID partId,
                           std::vector<cpp2::NewEdge>&& edges,
                           std::vector<std::string> propNames,
                           bool overwritable,
                           meta::cpp2::PropertyType spaceVidType,
                           Encoder encoder)
        : BaseChainProcessor(std::move(cb), env),
          spaceId_(spaceId),
          partId_(partId),
          inEdges_(std::move(edges)),
          propNames_(std::move(propNames)),
          overwritable_(overwritable),
          spaceVidType_(spaceVidType),
          encoder_(encoder) {
        vIdLen_ = vIdLen;
    }

    folly::SemiFuture<cpp2::ErrorCode> prepareLocal() override;

    folly::SemiFuture<cpp2::ErrorCode> processRemote(cpp2::ErrorCode code) override;

    folly::SemiFuture<cpp2::ErrorCode> processLocal(cpp2::ErrorCode code) override;

    static ChainAddEdgesProcessor* instance(Callback&& cb,
                                            StorageEnv* env,
                                            int32_t vIdLen,
                                            GraphSpaceID spaceId,
                                            PartitionID partId,
                                            std::vector<cpp2::NewEdge>& edges,
                                            std::vector<std::string>& propNames,
                                            bool overwritable,
                                            meta::cpp2::PropertyType spaceVidType,
                                            Encoder&& encoder) {
        return new ChainAddEdgesProcessor(std::move(cb),
                                          env,
                                          vIdLen,
                                          spaceId,
                                          partId,
                                          std::move(edges),
                                          propNames,
                                          overwritable,
                                          spaceVidType,
                                          std::move(encoder));
    }

private:
    void doRpc(std::vector<cpp2::NewEdge>& outEdges,
               folly::Promise<cpp2::ErrorCode>&& promise) noexcept;

protected:
    GraphSpaceID                spaceId_{-1};
    PartitionID                 partId_{-1};
    std::vector<cpp2::NewEdge>  inEdges_;
    std::vector<std::string>    propNames_;
    bool                        overwritable_{true};
    meta::cpp2::PropertyType    spaceVidType_;
    Encoder                     encoder_;
    std::unique_ptr<LockGuard>  lk_;
};

}  // namespace storage
}  // namespace nebula
