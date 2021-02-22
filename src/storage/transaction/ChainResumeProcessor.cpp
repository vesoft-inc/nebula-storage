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
    LOG(INFO) << "ChainResumeProcessor::prepareLocal(), txnId_ = " << txnId_;
    lk_ = std::make_unique<LockGuard>(env_->txnMan_->getMemoryLock(), lock_->lockKey, txnId_);
    if (!lk_->isLocked()) {
        LOG(INFO) << "E_SET_MEM_LOCK_FAILED, txnId_ = " << txnId_;
        return cpp2::ErrorCode::E_SET_MEM_LOCK_FAILED;
    }
    LOG(INFO) << "~ChainResumeProcessor::prepareLocal()";
    return cpp2::ErrorCode::SUCCEEDED;
}

folly::SemiFuture<cpp2::ErrorCode> ChainResumeProcessor::processRemote(cpp2::ErrorCode code) {
    LOG(INFO) << "ChainResumeProcessor::processRemote(), code = " << CommonUtils::name(code);
    setErrorCode(code);
    if (code_ != cpp2::ErrorCode::SUCCEEDED) {
        return code_;
    }
    auto c = folly::makePromiseContract<cpp2::ErrorCode>();

    auto remoteKey0 = NebulaKeyUtils::toEdgeKey(lock_->lockKey);
    LOG(INFO) << "remoteKey 0 = " << folly::hexlify(remoteKey0);
    auto remoteKey = env_->txnMan_->remoteEdgeKey(vIdLen_, spaceId_, lock_->lockKey);
    LOG(INFO) << "remoteKey = " << folly::hexlify(remoteKey);
    iClient_->getValue(vIdLen_, spaceId_, remoteKey)
        .via(env_->txnMan_->getExecutor())
        .thenTry([&, p = std::move(c.first)](auto&& t) mutable {
            LOG(INFO) << "~ChainResumeProcessor::processRemote()";
            if (!t.hasValue()) {
                LOG(INFO) << "!t.hasValue()";
                // return cpp2::ErrorCode::E_KEY_NOT_FOUND;
                p.setValue(cpp2::ErrorCode::E_KEY_NOT_FOUND);
                return;
            }
            if (!nebula::ok(t.value())) {
                LOG(INFO) << "!nebula::ok(t.value())";
                p.setValue(cpp2::ErrorCode::E_KEY_NOT_FOUND);
                return;
            }
            lock_->lockProps = nebula::value(t.value());
            p.setValue(cpp2::ErrorCode::SUCCEEDED);
        });

    LOG(INFO) << "-ChainResumeProcessor::processRemote()";
    return std::move(c.second);
}

folly::SemiFuture<cpp2::ErrorCode> ChainResumeProcessor::processLocal(cpp2::ErrorCode code) {
    LOG(INFO) << "ChainResumeProcessor::processLocal()";
    auto partId = NebulaKeyUtils::getPart(lock_->lockKey);
    if (code == cpp2::ErrorCode::E_KEY_NOT_FOUND || code == cpp2::ErrorCode::SUCCEEDED) {
        // 1. remove locks
        LOG(INFO) << "remove lock: " << folly::hexlify(lock_->lockKey);
        folly::Baton<true, std::atomic> baton1;
        env_->kvstore_->asyncRemove(
            spaceId_, partId, lock_->lockKey, [&](nebula::kvstore::ResultCode rc) {
                LOG(INFO) << "ChainResumeProcessor::processLocal(), remove lock rc = "
                          << static_cast<int>(rc);
                baton1.post();
            });
        baton1.wait();
    } else {
        setErrorCode(code);
        if (code_ != cpp2::ErrorCode::SUCCEEDED) {
            return code_;
        }
    }

    // 2. resume in-edge
    if (lock_->lockProps) {
        auto ekey = NebulaKeyUtils::toEdgeKey(lock_->lockKey);
        LOG(INFO) << "resume in-edge: " << folly::hexlify(ekey);
        std::vector<KV> data;
        data.emplace_back(std::make_pair(ekey, *lock_->lockProps));
        folly::Baton<true, std::atomic> baton2;
        env_->kvstore_->asyncMultiPut(
            spaceId_, partId, std::move(data), [&](kvstore::ResultCode rc) {
                LOG(INFO) << "ChainResumeProcessor::processLocal(), resume in-edge rc = "
                          << static_cast<int>(rc);
                baton2.post();
            });
        baton2.wait();
    }
    // kvstore::BatchHolder bat;
    // LOG(INFO) << "remove lock: " << folly::hexlify(lock_->lockKey);
    // bat.remove(std::string(lock_->lockKey));
    // if (lock_->lockProps) {
    //     // bat.put(remoteKey, remoteVal);
    //     auto rawKey = NebulaKeyUtils::toEdgeKey(lock_->lockKey);
    //     LOG(INFO) << "put edge: " << folly::hexlify(rawKey);
    //     bat.put(std::move(rawKey), std::string(*lock_->lockProps));
    // }

    // auto batch = encodeBatchValue(bat.getBatch());
    // auto c = folly::makePromiseContract<cpp2::ErrorCode>();
    // env_->txnMan_->commitBatch(spaceId_, partId_, batch)
    //     .via(env_->txnMan_->getExecutor())
    //     .thenTry([p = std::move(c.first)](auto&& t) mutable {
    //         auto rc =
    //             t.hasValue() ? CommonUtils::to(t.value()) : cpp2::ErrorCode::E_KVSTORE_EXCEPTION;
    //         LOG(INFO) << "~ChainResumeProcessor::processLocal() rc = " << CommonUtils::name(rc);
    //         p.setValue(rc);
    //     });
    LOG(INFO) << "~ChainResumeProcessor::processLocal()";
    return cpp2::ErrorCode::SUCCEEDED;
    // return std::move(c.second);
}

// folly::SemiFuture<cpp2::ErrorCode> ChainResumeProcessor::processLocal(cpp2::ErrorCode code) {
//     LOG(INFO) << "ChainResumeProcessor::processLocal()";
//     setErrorCode(code);
//     if (code_ != cpp2::ErrorCode::SUCCEEDED) {
//         return code_;
//     }

//     // 1. remove locks
//     kvstore::BatchHolder bat;
//     LOG(INFO) << "remove lock: " << folly::hexlify(lock_->lockKey);
//     bat.remove(std::string(lock_->lockKey));
//     if (lock_->lockProps) {
//         // bat.put(remoteKey, remoteVal);
//         auto rawKey = NebulaKeyUtils::toEdgeKey(lock_->lockKey);
//         LOG(INFO) << "put edge: " << folly::hexlify(rawKey);
//         bat.put(std::move(rawKey), std::string(*lock_->lockProps));
//     }

//     auto batch = encodeBatchValue(bat.getBatch());
//     auto c = folly::makePromiseContract<cpp2::ErrorCode>();
//     env_->txnMan_->commitBatch(spaceId_, partId_, batch)
//         .via(env_->txnMan_->getExecutor())
//         .thenTry([p = std::move(c.first)](auto&& t) mutable {
//             auto rc =
//                 t.hasValue() ? CommonUtils::to(t.value()) : cpp2::ErrorCode::E_KVSTORE_EXCEPTION;
//             LOG(INFO) << "~ChainResumeProcessor::processLocal() rc = " << CommonUtils::name(rc);
//             p.setValue(rc);
//         });
//     LOG(INFO) << "-ChainResumeProcessor::processLocal()";
//     return std::move(c.second);
// }

// void cleanup() override;


}  // namespace storage
}  // namespace nebula
