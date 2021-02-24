/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/transaction/ChainResumeProcessor.h"
#include "storage/transaction/TransactionUtils.h"
#include "storage/transaction/TransactionManager.h"

namespace nebula {
namespace storage {

folly::SemiFuture<cpp2::ErrorCode> ChainResumeProcessor::prepareLocal() {
    LOG_IF(INFO, FLAGS_trace_toss) << "prepareLocal(), txnId_ = " << txnId_;
    lk_ =
        std::make_unique<LockGuard>(env_->txnMan_->getMemoryLock(spaceId_), lock_->lockKey, txnId_);
    if (!lk_->isLocked()) {
        LOG(INFO) << "E_SET_MEM_LOCK_FAILED, txnId_ = " << txnId_;
        return cpp2::ErrorCode::E_SET_MEM_LOCK_FAILED;
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

folly::SemiFuture<cpp2::ErrorCode> ChainResumeProcessor::processRemote(cpp2::ErrorCode code) {
    LOG_IF(INFO, FLAGS_trace_toss) << "processRemote(), code = " << CommonUtils::name(code);
    setErrorCode(code);
    if (code_ != cpp2::ErrorCode::SUCCEEDED) {
        return code_;
    }
    auto c = folly::makePromiseContract<cpp2::ErrorCode>();
    auto strOutEdgeKey = env_->txnMan_->remoteEdgeKey(vIdLen_, spaceId_, lock_->lockKey);
    iClient_->getValue(vIdLen_, spaceId_, strOutEdgeKey)
        .via(env_->txnMan_->getExecutor())
        .thenTry([&, p = std::move(c.first)](auto&& t) mutable {
            if (!t.hasValue()) {
                p.setValue(cpp2::ErrorCode::E_KEY_NOT_FOUND);
                return;
            }
            if (!nebula::ok(t.value())) {
                p.setValue(cpp2::ErrorCode::E_KEY_NOT_FOUND);
                return;
            }
            lock_->lockProps = nebula::value(t.value());
            p.setValue(cpp2::ErrorCode::SUCCEEDED);
        });
    return std::move(c.second);
}

folly::SemiFuture<cpp2::ErrorCode> ChainResumeProcessor::processLocal(cpp2::ErrorCode code) {
    kvstore::BatchHolder bat;
    // 1. remove locks
    if (code == cpp2::ErrorCode::E_KEY_NOT_FOUND || code == cpp2::ErrorCode::SUCCEEDED) {
        bat.remove(std::string(lock_->lockKey));
    } else {
        setErrorCode(code);
        return code_;
    }

    // 2. resume in-edge
    if (lock_->lockProps) {
        auto rawKey = NebulaKeyUtils::toEdgeKey(lock_->lockKey);
        bat.put(std::move(rawKey), std::string(*lock_->lockProps));
    }

    auto batch = encodeBatchValue(bat.getBatch());
    auto c = folly::makePromiseContract<cpp2::ErrorCode>();
    env_->txnMan_->commitBatch(spaceId_, partId_, batch)
        .via(env_->txnMan_->getExecutor())
        .thenTry([p = std::move(c.first)](auto&& t) mutable {
            auto rc =
                t.hasValue() ? CommonUtils::to(t.value()) : cpp2::ErrorCode::E_KVSTORE_EXCEPTION;
            LOG_IF(INFO, FLAGS_trace_toss) << "~processLocal() rc = " << CommonUtils::name(rc);
            p.setValue(rc);
        });
    return std::move(c.second);
}

}  // namespace storage
}  // namespace nebula
