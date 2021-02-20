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

folly::SemiFuture<cpp2::ErrorCode> ChainAddEdgesProcessor::prepareLocal() {
    LOG(INFO) << "ChainAddEdgesProcessor::prepareLocal() " << txnId_;
    // step 1. lock edges in memory (use string form of EdgeKey as lock key)
    auto& newEdges = request_.get_parts().begin()->second;
    std::vector<std::string> locks(newEdges.size());
    std::transform(newEdges.begin(), newEdges.end(), locks.begin(), [&](auto& e){
        return TransactionUtils::lockKey(vIdLen_, partId_, e.key);
    });
    lk_ = std::make_unique<LockGuard>(env_->txnMan_->getMemoryLock(), locks);
    if (!lk_->isLocked()) {
        return cpp2::ErrorCode::E_SET_MEM_LOCK_FAILED;
    }

    // needUnlock_ = true;
    // step 2. commit locks
    std::vector<KV> data(locks.size());
    std::transform(locks.begin(), locks.end(), data.begin(), [](auto& key){
        return std::make_pair(std::move(key), "");
    });
    LOG(INFO) << "messi put lock key: " << folly::hexlify(data[0].first);
    auto c = folly::makePromiseContract<cpp2::ErrorCode>();
    env_->kvstore_->asyncMultiPut(
        spaceId_, partId_, data, [&, p = std::move(c.first)](kvstore::ResultCode rc) mutable {
            LOG(INFO) << "~ChainAddEdgesProcessor::prepareLocal() " << txnId_;
            p.setValue(CommonUtils::to(rc));
        });
    LOG(INFO) << "-ChainAddEdgesProcessor::prepareLocal() " << txnId_;
    return std::move(c.second);
}

/**
 *
 * */
folly::SemiFuture<cpp2::ErrorCode> ChainAddEdgesProcessor::processRemote(cpp2::ErrorCode code) {
    LOG(INFO) << "ChainAddEdgesProcessor::processRemote() " << txnId_;
    setErrorCode(code);
    if (code_ != cpp2::ErrorCode::SUCCEEDED) {
        return code_;
    }
    auto& inEdges = request_.get_parts().begin()->second;
    std::vector<cpp2::NewEdge> outEdges(inEdges.size());
    std::transform(inEdges.begin(), inEdges.end(), outEdges.begin(), [&](auto edge) {
        std::swap(edge.key.src, edge.key.dst);
        edge.key.edge_type = 0 - edge.key.edge_type;
        if (convertVid_) {
            TransactionUtils::changeToIntVid(edge.key);
        }
        return edge;
    });
    auto overwritable = request_.__isset.overwritable ? request_.get_overwritable() : true;

    auto c = folly::makePromiseContract<cpp2::ErrorCode>();
    addRemoteEdges(spaceId_, outEdges, request_.get_prop_names(), overwritable, std::move(c.first));
    LOG(INFO) << "-ChainAddEdgesProcessor::processRemote() " << txnId_;
    return std::move(c.second);
}

void ChainAddEdgesProcessor::addRemoteEdges(GraphSpaceID space,
                                            std::vector<cpp2::NewEdge>& edges,
                                            const std::vector<std::string>& propNames,
                                            bool overwritable,
                                            folly::Promise<cpp2::ErrorCode>&& pro) noexcept {
    LOG(INFO) << "ChainAddEdgesProcessor::addRemoteEdges() " << txnId_;
    auto* sClient = env_->txnMan_->getStorageClient();
    sClient->addEdges(space, edges, propNames, overwritable)
        .via(env_->txnMan_->getExecutor())
        .thenTry([=, p = std::move(pro)](auto&& ret) mutable {
            auto rc = cpp2::ErrorCode::SUCCEEDED;
            if (ret.hasValue()) {
                StorageRpcResponse<cpp2::ExecResponse>& rpcResp = ret.value();
                if (!rpcResp.failedParts().empty()) {
                    CHECK_EQ(rpcResp.failedParts().size(), 1);
                    rc = rpcResp.failedParts().begin()->second;
                    if (rc == cpp2::ErrorCode::E_LEADER_CHANGED) {
                        LOG(INFO) << "messi leader changed, retry";
                        addRemoteEdges(space, edges, propNames, overwritable, std::move(p));
                        return;
                    }
                }
            } else {
                rc = cpp2::ErrorCode::E_FORWARD_REQUEST_ERR;
            }
            LOG(INFO) << "sClient->addEdges(), rc = " << static_cast<int>(rc);
            LOG(INFO) << "ChainAddEdgesProcessor::~processRemote() " << txnId_;
            p.setValue(rc);
            return;
        });
    LOG(INFO) << "ChainAddEdgesProcessor::-addRemoteEdges() " << txnId_;
}

folly::SemiFuture<cpp2::ErrorCode> ChainAddEdgesProcessor::processLocal(cpp2::ErrorCode code) {
    LOG(INFO) << "ChainAddEdgesProcessor::processLocal() " << txnId_;
    setErrorCode(code);
    if (code_ != cpp2::ErrorCode::SUCCEEDED) {
        return code_;
    }

    // 1. remove locks
    kvstore::BatchHolder bat;
    auto partId = request_.get_parts().begin()->first;
    auto& newEdges = request_.get_parts().begin()->second;

    std::for_each(newEdges.begin(), newEdges.end(), [&](auto& e) {
        bat.remove(TransactionUtils::lockKey(vIdLen_, partId, e.key));
        auto ret = encoder_(e);
        // if encode failed, processRemote should failed.
        LOG(INFO) << "messi put in-edge key: "
                  << folly::hexlify(TransactionUtils::edgeKey(vIdLen_, partId, e.key))
                  << ", txnId_ = " << txnId_;
        bat.put(TransactionUtils::edgeKey(vIdLen_, partId, e.key), std::move(ret.first));
    });

    auto c = folly::makePromiseContract<cpp2::ErrorCode>();
    auto batch = encodeBatchValue(bat.getBatch());
    env_->txnMan_->commitBatch(spaceId_, partId_, batch)
        .via(env_->txnMan_->getExecutor())
        .thenTry([&, p = std::move(c.first)](auto&& t) mutable {
            LOG(INFO) << "~ChainAddEdgesProcessor::processLocal() " << txnId_;
            auto rc =
                t.hasValue() ? CommonUtils::to(t.value()) : cpp2::ErrorCode::E_KVSTORE_EXCEPTION;
            p.setValue(rc);
        });
    // VLOG(1) << "exit ChainAddEdgesProcessor::processLocal()";
    LOG(INFO) << "-ChainAddEdgesProcessor::processLocal() " << txnId_;
    return std::move(c.second);
}

// void ChainAddEdgesProcessor::cleanup() {
//     VLOG(1) << "ChainAddEdgesProcessor::cleanup()";
//     // if (needUnlock_) {
//     //     auto& newEdges = request_.get_parts().begin()->second;
//     //     std::vector<std::string> locks(newEdges.size());
//     //     std::transform(newEdges.begin(), newEdges.end(), locks.begin(), [&](auto& e) {
//     //         return TransactionUtils::lockKey(vIdLen_, partId_, e.key);
//     //     });
//     //     env_->txnMan_->getMemoryLock()->unlockBatch(locks);
//     // }
//     VLOG(1) << "~ChainAddEdgesProcessor::cleanup()";
// }

}  // namespace storage
}  // namespace nebula
