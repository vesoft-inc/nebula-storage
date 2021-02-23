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
    LOG(INFO) << "ChainUpdateEdgeProcessor::prepareLocal(), txnId_=" << txnId_;

    sLockKey_ = TransactionUtils::lockKey(vIdLen_, partId_, inEdgeKey_);
    lk_ = std::make_unique<LockGuard>(env_->txnMan_->getMemoryLock(), sLockKey_, txnId_);
    if (!lk_->isLocked()) {
        return cpp2::ErrorCode::E_SET_MEM_LOCK_FAILED;
    }

    optVal_ = getter_(txnId_);
    if (!optVal_) {
        return cpp2::ErrorCode::E_ATOMIC_OP_FAILED;
    }

    std::vector<nebula::kvstore::KV> data{{sLockKey_, ""}};
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
    TransactionUtils::changeToIntVid(inEdgeKey_);
    auto c = folly::makePromiseContract<cpp2::ErrorCode>();
    updateRemoteEdge(std::move(c.first));
    LOG(INFO) << "-ChainUpdateEdgeProcessor::processRemote()";
    return std::move(c.second);
}

void ChainUpdateEdgeProcessor::updateRemoteEdge(folly::Promise<cpp2::ErrorCode>&& pro) noexcept {
    auto* sClient = env_->txnMan_->getStorageClient();
    auto outEdgeKey(inEdgeKey_);
      std::swap(outEdgeKey.src, outEdgeKey.dst);
      outEdgeKey.edge_type = 0 - outEdgeKey.edge_type;

    sClient->updateEdge(spaceId_, outEdgeKey, updateProps_, insertable_, returnProps_, condition_)
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
    LOG(INFO) << "ChainUpdateEdgeProcessor::processLocal(), remove lock="
              << folly::hexlify(sLockKey_);
    bat.remove(std::move(sLockKey_));

    // optVal_ = getter_();
    if (optVal_) {
        auto val = kvstore::decodeBatchValue(*optVal_);
        auto& dataView = val.back().second;
        auto key = dataView.first.str();
        LOG(INFO) << "ChainUpdateEdgeProcessor::processLocal(), dataView.first.str()="
              << folly::hexlify(key);
        if (NebulaKeyUtils::isLock(vIdLen_, key)) {
            key = NebulaKeyUtils::toEdgeKey(key);
        }
        LOG(INFO) << "ChainUpdateEdgeProcessor::processLocal(), put key="
              << folly::hexlify(key);
        bat.put(std::move(key), dataView.second.str());
    }
    auto batch = encodeBatchValue(bat.getBatch());
    auto c = folly::makePromiseContract<cpp2::ErrorCode>();
    env_->txnMan_->commitBatch(spaceId_, partId_, batch)
        .via(env_->txnMan_->getExecutor())
        .thenTry([p = std::move(c.first)](auto&& t) mutable {
            auto rc =
                t.hasValue() ? CommonUtils::to(t.value()) : cpp2::ErrorCode::E_KVSTORE_EXCEPTION;

            LOG(INFO) << "~ChainUpdateEdgeProcessor::processLocal(), rc=" << CommonUtils::name(rc);
            p.setValue(rc);
        });
    LOG(INFO) << "-ChainUpdateEdgeProcessor::processLocal()";
    return std::move(c.second);
}

}  // namespace storage
}  // namespace nebula
