/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/transaction/ChainUpdateProcessor.h"
#include "storage/transaction/TransactionUtils.h"
#include "storage/transaction/TransactionManager.h"

namespace nebula {
namespace storage {

folly::SemiFuture<cpp2::ErrorCode> ChainUpdateEdgeProcessor::prepareLocal() {
    LOG(INFO) << "enter ChainUpdateEdgeProcessor::prepareLocal()";

    std::string strLock = TransactionUtils::lockKey(vIdLen_, req_.part_id, req_.edge_key);
    if (!env_->txnMan_->getMemoryLock()->lock(strLock)) {
        return cpp2::ErrorCode::E_SET_MEM_LOCK_FAILED;
    }
    std::vector<KV> data{{strLock, ""}};
    auto c = folly::makePromiseContract<cpp2::ErrorCode>();
    env_->kvstore_->asyncMultiPut(spaceId_,
                                  req_.part_id,
                                  data,
                                  [p = std::move(c.first)](kvstore::ResultCode rc) mutable {
                                      p.setValue(CommonUtils::to(rc));
                                  });
    LOG(INFO) << "exit ChainUpdateEdgeProcessor::prepareLocal()";
    return std::move(c.second);
}

folly::SemiFuture<cpp2::ErrorCode> ChainUpdateEdgeProcessor::processRemote(cpp2::ErrorCode code) {
    LOG(INFO) << "enter ChainUpdateEdgeProcessor::processRemote()";
    setErrorCode(code);
    if (code_ != cpp2::ErrorCode::SUCCEEDED) {
        return code_;
    }
    auto* sClient = env_->txnMan_->getStorageClient();
    auto c = folly::makePromiseContract<cpp2::ErrorCode>();
    sClient
        ->updateEdge(spaceId_,
                     req_.get_edge_key(),
                     req_.get_updated_props(),
                     req_.get_insertable(),
                     *req_.get_return_props(),
                     *req_.get_condition())
        // .via(env_->txnMan_->getExecutor())
        .thenTry([p = std::move(c.first)](auto&& ret) mutable {
            auto rc = ret.hasValue() && ret.value().ok()
                          ? cpp2::ErrorCode::SUCCEEDED
                          : cpp2::ErrorCode::E_FORWARD_REQUEST_ERR;
            p.setValue(rc);
        });
    LOG(INFO) << "exit ChainUpdateEdgeProcessor::processRemote()";
    return std::move(c.second);
}

folly::SemiFuture<cpp2::ErrorCode> ChainUpdateEdgeProcessor::processLocal(cpp2::ErrorCode code) {
    LOG(INFO) << "enter ChainUpdateEdgeProcessor::processLocal()";
    setErrorCode(code);
    if (code_ != cpp2::ErrorCode::SUCCEEDED) {
        return code_;
    }

    // 1. remove locks
    kvstore::BatchHolder bat;
    bat.remove(TransactionUtils::lockKey(vIdLen_, req_.part_id, req_.edge_key));

    auto optVal = getter_();
    if (optVal) {
        auto val = kvstore::decodeBatchValue(*optVal);
        auto& dataView = val.back().second;
        bat.put(dataView.first.str(), dataView.second.str());
    }
    auto batch = encodeBatchValue(bat.getBatch());
    auto c = folly::makePromiseContract<cpp2::ErrorCode>();
    env_->txnMan_->commitBatch(spaceId_, partId_, batch)
        .via(env_->txnMan_->getExecutor())
        .thenTry([p = std::move(c.first)](auto&& t) mutable {
            auto rc =
                t.hasValue() ? CommonUtils::to(t.value()) : cpp2::ErrorCode::E_KVSTORE_EXCEPTION;
            p.setValue(rc);
        });
    LOG(INFO) << "exit ChainUpdateEdgeProcessor::processLocal()";
    return std::move(c.second);
}

// void cleanup() override;


}  // namespace storage
}  // namespace nebula
