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
    using LockGuard = nebula::MemoryLockGuard<std::string>;

public:
    using Encoder = std::function<std::pair<std::string, cpp2::ErrorCode>(const cpp2::NewEdge& e)>;
    static ChainAddEdgesProcessor* instance(StorageEnv* env,
                                            const cpp2::AddEdgesRequest& req,
                                            Callback&& cb) {
        return new ChainAddEdgesProcessor(env, req, std::move(cb));
    }

    ChainAddEdgesProcessor(StorageEnv* env, cpp2::AddEdgesRequest req, Callback cb)
        : BaseChainProcessor(env, std::move(cb)), request_(std::move(req)) {
            CHECK(req.__isset.space_id);
            CHECK(req.__isset.parts);
            spaceId_ = request_.get_space_id();
            partId_ = request_.get_parts().begin()->first;
        }

    folly::SemiFuture<cpp2::ErrorCode> prepareLocal() override;

    folly::SemiFuture<cpp2::ErrorCode> processRemote(cpp2::ErrorCode code) override;

    folly::SemiFuture<cpp2::ErrorCode> processLocal(cpp2::ErrorCode code) override;

    void setEncoder(Encoder&& encoder) {
        encoder_ = std::move(encoder);
    }

    void setConvertVid(bool convertVid) {
        convertVid_ = convertVid;
    }

    void addRemoteEdges(GraphSpaceID space,
                        std::vector<cpp2::NewEdge>& edges,
                        const std::vector<std::string>& propNames,
                        bool overwritable,
                        folly::Promise<cpp2::ErrorCode>&& promise) noexcept;

protected:
    GraphSpaceID spaceId_{-1};
    PartitionID partId_{-1};
    cpp2::AddEdgesRequest request_;
    Encoder encoder_;
    bool convertVid_{false};
    std::unique_ptr<LockGuard> lk_;
};

}  // namespace storage
}  // namespace nebula
