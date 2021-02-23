/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/transaction/ChainAddEdgeProcessor.h"
#include "storage/transaction/TransactionUtils.h"
#include "storage/transaction/TransactionManager.h"

namespace nebula {
namespace storage {

/**
 * @brief
 *      step 1. lock edges in memory
 *      step 2. commit locks
 */
folly::SemiFuture<cpp2::ErrorCode> ChainAddEdgesProcessor::prepareLocal() {
    LOG_IF(INFO, FLAGS_trace_toss) << "prepareLocal() tid=" << txnId_;

    // step 1. lock edges in memory (use string form of EdgeKey as lock key)
    std::vector<std::string> locks(inEdges_.size());
    std::transform(inEdges_.begin(), inEdges_.end(), locks.begin(), [&](auto& e){
        return TransactionUtils::lockKey(vIdLen_, partId_, e.key);
    });
    lk_ = std::make_unique<LockGuard>(env_->txnMan_->getMemoryLock(), locks);
    if (!lk_->isLocked()) {
        return cpp2::ErrorCode::E_SET_MEM_LOCK_FAILED;
    }

    // step 2. commit locks
    std::vector<nebula::kvstore::KV> data(locks.size());
    std::transform(locks.begin(), locks.end(), data.begin(), [](auto& key){
        return std::make_pair(std::move(key), "");
    });

    auto c = folly::makePromiseContract<cpp2::ErrorCode>();
    env_->kvstore_->asyncMultiPut(
        spaceId_, partId_, data, [&, p = std::move(c.first)](auto rc) mutable {
            LOG_IF(INFO, FLAGS_trace_toss) << "~prepareLocal() tid=" << txnId_;
            p.setValue(CommonUtils::to(rc));
        });
    LOG_IF(INFO, FLAGS_trace_toss) << "-prepareLocal() tid=" << txnId_;
    return std::move(c.second);
}


folly::SemiFuture<cpp2::ErrorCode> ChainAddEdgesProcessor::processRemote(cpp2::ErrorCode code) {
    LOG_IF(INFO, FLAGS_trace_toss) << "processRemote() " << txnId_;
    setErrorCode(code);
    if (code_ != cpp2::ErrorCode::SUCCEEDED) {
        return code_;
    }
    // auto& inEdges = request_.get_parts().begin()->second;
    std::vector<cpp2::NewEdge> outEdges(inEdges_.size());
    std::transform(inEdges_.begin(), inEdges_.end(), outEdges.begin(), [&](auto edge) {
        std::swap(edge.key.src, edge.key.dst);
        edge.key.edge_type = 0 - edge.key.edge_type;
        if (spaceVidType_ == meta::cpp2::PropertyType::INT64) {
            TransactionUtils::changeToIntVid(edge.key);
        }
        return edge;
    });

    auto c = folly::makePromiseContract<cpp2::ErrorCode>();
    doRpc(outEdges, std::move(c.first));
    LOG_IF(INFO, FLAGS_trace_toss) << "-processRemote() " << txnId_;
    return std::move(c.second);
}


/**
 * @brief call sClient::addEdges, if leader changed, retry
 */
void ChainAddEdgesProcessor::doRpc(std::vector<cpp2::NewEdge>& outEdges,
                                   folly::Promise<cpp2::ErrorCode>&& pro) noexcept {
    LOG_IF(INFO, FLAGS_trace_toss) << "doRpc() " << txnId_;
    auto* sClient = env_->txnMan_->getStorageClient();
    sClient->addEdges(spaceId_, outEdges, propNames_, overwritable_)
        .via(env_->txnMan_->getExecutor())
        .thenTry([=, p = std::move(pro)](auto&& ret) mutable {
            auto rc = cpp2::ErrorCode::SUCCEEDED;
            if (ret.hasValue()) {
                StorageRpcResponse<cpp2::ExecResponse>& rpcResp = ret.value();
                if (!rpcResp.failedParts().empty()) {
                    CHECK_EQ(rpcResp.failedParts().size(), 1);
                    rc = rpcResp.failedParts().begin()->second;
                    if (rc == cpp2::ErrorCode::E_LEADER_CHANGED) {
                        LOG_IF(INFO, FLAGS_trace_toss) << "doRpc() leader changed, retry";
                        doRpc(outEdges, std::move(p));
                        return;
                    }
                }
            } else {
                rc = cpp2::ErrorCode::E_FORWARD_REQUEST_ERR;
            }
            LOG_IF(INFO, FLAGS_trace_toss) << "~tid=" << txnId_ << ", rc=" << CommonUtils::name(rc);
            p.setValue(rc);
            return;
        });
}

folly::SemiFuture<cpp2::ErrorCode> ChainAddEdgesProcessor::processLocal(cpp2::ErrorCode code) {
    LOG_IF(INFO, FLAGS_trace_toss) << "processLocal() " << txnId_;
    setErrorCode(code);
    if (code_ != cpp2::ErrorCode::SUCCEEDED) {
        return code_;
    }

    // 1. remove locks
    kvstore::BatchHolder bat;
    std::for_each(inEdges_.begin(), inEdges_.end(), [&](auto& e) {
        bat.remove(TransactionUtils::lockKey(vIdLen_, partId_, e.key));
        auto ret = encoder_(e);
        // if encode failed, processRemote should failed.
        LOG_IF(INFO, FLAGS_trace_toss) << "messi put in-edge key: "
                  << folly::hexlify(TransactionUtils::edgeKey(vIdLen_, partId_, e.key))
                  << ", txnId_ = " << txnId_;
        bat.put(TransactionUtils::edgeKey(vIdLen_, partId_, e.key), std::move(ret.first));
    });

    auto c = folly::makePromiseContract<cpp2::ErrorCode>();
    auto batch = encodeBatchValue(bat.getBatch());
    env_->txnMan_->commitBatch(spaceId_, partId_, batch)
        .via(env_->txnMan_->getExecutor())
        .thenTry([&, p = std::move(c.first)](auto&& t) mutable {
            LOG_IF(INFO, FLAGS_trace_toss) << "~processLocal() " << txnId_;
            auto rc =
                t.hasValue() ? CommonUtils::to(t.value()) : cpp2::ErrorCode::E_KVSTORE_EXCEPTION;
            p.setValue(rc);
        });
    LOG_IF(INFO, FLAGS_trace_toss) << "-processLocal() " << txnId_;
    return std::move(c.second);
}

}  // namespace storage
}  // namespace nebula
