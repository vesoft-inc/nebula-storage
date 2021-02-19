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
    LOG(INFO) << "ChainUpdateEdgeProcessor::prepareLocal()";

    sLockKey_ = TransactionUtils::lockKey(vIdLen_, partId_, edgeKey_);
    if (!env_->txnMan_->getMemoryLock()->lock(sLockKey_)) {
        return cpp2::ErrorCode::E_SET_MEM_LOCK_FAILED;
    }
    std::vector<KV> data{{sLockKey_, ""}};
    auto c = folly::makePromiseContract<cpp2::ErrorCode>();
    LOG(INFO) << "spaceId_=" << spaceId_;
    env_->kvstore_->asyncMultiPut(
        spaceId_, partId_, data, [p = std::move(c.first)](kvstore::ResultCode rc) mutable {
            LOG(INFO) << "~ChainUpdateEdgeProcessor::prepareLocal(), rc = " << static_cast<int>(rc);
            p.setValue(CommonUtils::to(rc));
        });
    LOG(INFO) << "-ChainUpdateEdgeProcessor::prepareLocal()";
    return std::move(c.second);
}

folly::SemiFuture<cpp2::ErrorCode> ChainUpdateEdgeProcessor::processRemote(cpp2::ErrorCode code) {
    LOG(INFO) << "ChainUpdateEdgeProcessor::processRemote(), code = " << CommonUtils::name(code);
    setErrorCode(code);
    if (code_ != cpp2::ErrorCode::SUCCEEDED) {
        return code_;
    }
    TransactionUtils::changeToIntVid(edgeKey_);
    auto c = folly::makePromiseContract<cpp2::ErrorCode>();
    updateRemoteEdge(std::move(c.first));
    LOG(INFO) << "-ChainUpdateEdgeProcessor::processRemote()";
    return std::move(c.second);
}

void ChainUpdateEdgeProcessor::updateRemoteEdge(folly::Promise<cpp2::ErrorCode>&& pro) noexcept {
    auto* sClient = env_->txnMan_->getStorageClient();
    sClient->updateEdge(spaceId_, edgeKey_, updateProps_, insertable_, returnProps_, condition_)
        .thenTry([&, p = std::move(pro)](auto&& t) mutable {
            LOG(INFO) << "~ChainUpdateEdgeProcessor::processRemote()";
            auto rc = t.hasValue() && t.value().ok() ? cpp2::ErrorCode::SUCCEEDED
                                                     : cpp2::ErrorCode::E_FORWARD_REQUEST_ERR;
            p.setValue(rc);
        });
}

folly::SemiFuture<cpp2::ErrorCode> ChainUpdateEdgeProcessor::processLocal(cpp2::ErrorCode code) {
    LOG(INFO) << "ChainUpdateEdgeProcessor::processLocal()";
    setErrorCode(code);
    if (code_ != cpp2::ErrorCode::SUCCEEDED) {
        return code_;
    }

    // 1. remove locks
    kvstore::BatchHolder bat;
    // bat.remove(TransactionUtils::lockKey(vIdLen_, partId_, edgeKey_));
    bat.remove(std::move(sLockKey_));

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
            LOG(INFO) << "~ChainUpdateEdgeProcessor::processLocal()";
            auto rc =
                t.hasValue() ? CommonUtils::to(t.value()) : cpp2::ErrorCode::E_KVSTORE_EXCEPTION;
            p.setValue(rc);
        });
    LOG(INFO) << "-ChainUpdateEdgeProcessor::processLocal()";
    return std::move(c.second);
}

}  // namespace storage
}  // namespace nebula
