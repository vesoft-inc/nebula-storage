/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/transaction/ChainUpdateProcessor.h"
#include "storage/transaction/TransactionUtils.h"

namespace nebula {
namespace storage {

folly::SemiFuture<cpp2::ErrorCode> ChainUpdateEdgeProcessor::prepareLocal() {
    LOG_IF(INFO, FLAGS_trace_toss) << "prepareLocal(), txnId_=" << txnId_;
    sLockKey_ = TransactionUtils::lockKey(vIdLen_, partId_, inEdgeKey_);
    lk_ = std::make_unique<LockGuard>(env_->txnMan_->getMemoryLock(spaceId_), sLockKey_, txnId_);
    if (!lk_->isLocked()) {
        return cpp2::ErrorCode::E_SET_MEM_LOCK_FAILED;
    }

    optVal_ = getter_(txnId_);
    if (!optVal_) {
        return cpp2::ErrorCode::E_ATOMIC_OP_FAILED;
    }

    std::vector<nebula::kvstore::KV> data{{sLockKey_, ""}};
    auto c = folly::makePromiseContract<cpp2::ErrorCode>();
    env_->kvstore_->asyncMultiPut(
        spaceId_, partId_, data, [p = std::move(c.first)](kvstore::ResultCode rc) mutable {
            p.setValue(CommonUtils::to(rc));
        });
    return std::move(c.second);
}

folly::SemiFuture<cpp2::ErrorCode> ChainUpdateEdgeProcessor::processRemote(cpp2::ErrorCode code) {
    LOG_IF(INFO, FLAGS_trace_toss)
        << "processRemote(), txnId_=" << txnId_ << ", code = " << CommonUtils::name(code);
    setErrorCode(code);
    if (code_ != cpp2::ErrorCode::SUCCEEDED) {
        return code_;
    }
    if (spaceVidType_ == meta::cpp2::PropertyType::INT64) {
        TransactionUtils::changeToIntVid(inEdgeKey_);
    }
    auto c = folly::makePromiseContract<cpp2::ErrorCode>();
    updateRemoteEdge(std::move(c.first));
    return std::move(c.second);
}

void ChainUpdateEdgeProcessor::updateRemoteEdge(folly::Promise<cpp2::ErrorCode>&& pro) noexcept {
    auto* sClient = env_->txnMan_->getStorageClient();
    auto outEdgeKey(inEdgeKey_);
    std::swap(outEdgeKey.src, outEdgeKey.dst);
    outEdgeKey.edge_type = 0 - outEdgeKey.edge_type;

    sClient->updateEdge(spaceId_, outEdgeKey, updateProps_, insertable_, returnProps_, condition_)
        .thenTry([&, p = std::move(pro)](auto&& t) mutable {
            auto rc = t.hasValue() && t.value().ok() ? cpp2::ErrorCode::SUCCEEDED
                                                     : cpp2::ErrorCode::E_FORWARD_REQUEST_ERR;
            p.setValue(rc);
        });
}

folly::SemiFuture<cpp2::ErrorCode> ChainUpdateEdgeProcessor::processLocal(cpp2::ErrorCode code) {
    LOG_IF(INFO, FLAGS_trace_toss)
        << "processLocal(), txnId_=" << txnId_ << ", code = " << CommonUtils::name(code);
    setErrorCode(code);
    if (code_ != cpp2::ErrorCode::SUCCEEDED) {
        return code_;
    }

    // 1. remove locks
    kvstore::BatchHolder bat;
    bat.remove(std::move(sLockKey_));

    auto decoded = kvstore::decodeBatchValue(*optVal_);
    auto& kv = decoded.back().second;
    auto key = kv.first.str();
    if (NebulaKeyUtils::isLock(vIdLen_, key)) {
        key = NebulaKeyUtils::toEdgeKey(key);
    }
    bat.put(std::move(key), kv.second.str());

    auto batch = encodeBatchValue(bat.getBatch());
    auto c = folly::makePromiseContract<cpp2::ErrorCode>();
    env_->txnMan_->commitBatch(spaceId_, partId_, batch)
        .via(env_->txnMan_->getExecutor())
        .thenTry([p = std::move(c.first)](auto&& t) mutable {
            auto rc =
                t.hasValue() ? CommonUtils::to(t.value()) : cpp2::ErrorCode::E_KVSTORE_EXCEPTION;
            p.setValue(rc);
        });
    return std::move(c.second);
}

}  // namespace storage
}  // namespace nebula
