/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


#include <thrift/lib/cpp/util/EnumUtils.h>
#include "storage/transaction/ChainAddEdgesProcessorLocal.h"
#include "storage/transaction/ConsistUtil.h"
#include "storage/transaction/TransactionManager.h"
#include "storage/mutate/AddEdgesProcessor.h"
#include "storage/StorageFlags.h"

namespace nebula {
namespace storage {

void ChainAddEdgesProcessorLocal::process(const cpp2::AddEdgesRequest& req) {
    if (!prepareRequest(req)) {
        finish();
        return;
    }
    env_->txnMan_->addChainTask(this);
}

/**
 * @brief
 * 1. check term
 * 2. set mem lock
 * 3. write edge prime(key = edge prime, value = transaction id)
 */
folly::SemiFuture<Code> ChainAddEdgesProcessorLocal::prepareLocal() {
    if (!lockEdges(req_)) {
        return Code::E_WRITE_WRITE_CONFLICT;
    }

    auto [pro, fut] = folly::makePromiseContract<Code>();
    env_->kvstore_->asyncMultiPut(
        spaceId_, localPartId_, makePrime(), [p = std::move(pro)](auto rc) mutable {
            LOG_IF(WARNING, rc != nebula::cpp2::ErrorCode::SUCCEEDED)
                << "kvstore err: " << static_cast<int>(rc);
            p.setValue(rc);
        });
    return std::move(fut);
}

folly::SemiFuture<Code> ChainAddEdgesProcessorLocal::processRemote(Code code) {
    if (code != Code::SUCCEEDED) {
        return code;
    }
    CHECK_EQ(req_.get_parts().size(), 1);
    auto reversedRequest = reverseRequest(req_);
    CHECK_EQ(reversedRequest.get_parts().size(), 1);
    auto [pro, fut] = folly::makePromiseContract<Code>();
    doRpc(std::move(pro), std::move(reversedRequest));
    return std::move(fut);
}

folly::SemiFuture<Code> ChainAddEdgesProcessorLocal::processLocal(Code code) {
    LOG(INFO) << __func__ << "(), code = " << apache::thrift::util::enumNameSafe(code);
    if (!checkTerm(req_)) {
        LOG(WARNING) << "E_OUTDATED_TERM";
        return Code::E_OUTDATED_TERM;
    }

    if (code == Code::E_RPC_FAILURE) {
        kvAppend_ = makeDoublePrime();
        lk_->forceUnlock();
        markDanglingEdge();
    }

    erasePrime();
    if (code == Code::SUCCEEDED || code == Code::E_RPC_FAILURE) {
        return forwardToDelegateProcessor();
    } else {
        abort();
    }
    return code;
}

void ChainAddEdgesProcessorLocal::markDanglingEdge() {
    auto keys = sEdgeKey(req_);
    for (auto& key : keys) {
        env_->txnMan_->markDanglingEdge(spaceId_, key);
    }
}

bool ChainAddEdgesProcessorLocal::prepareRequest(const cpp2::AddEdgesRequest& req) {
    CHECK_EQ(req.get_parts().size(), 1);
    req_ = req;
    spaceId_ = req_.get_space_id();
    localPartId_ = req.get_parts().begin()->first;
    auto localTerm = env_->txnMan_->getTerm(spaceId_, localPartId_);
    if (localTerm.ok()) {
        localTerm_ = localTerm.value();
    } else {
        LOG(WARNING) << "getTerm failed: space = " << spaceId_ << ", part = " << localPartId_;
        pushResultCode(nebula::cpp2::ErrorCode::E_NO_TERM, localPartId_);
        return false;
    }

    auto vidLen = env_->schemaMan_->getSpaceVidLen(spaceId_);
    if (!vidLen.ok()) {
        LOG(ERROR) << "getSpaceVidLen failed, spaceId_: " << spaceId_
                   << ", status: " << vidLen.status();
        setErrorCode(Code::E_INVALID_SPACEVIDLEN);
        return false;
    }
    spaceVidLen_ = vidLen.value();
    return true;
}

folly::SemiFuture<Code> ChainAddEdgesProcessorLocal::forwardToDelegateProcessor() {
    auto* proc = AddEdgesProcessor::instance(env_, nullptr);
    proc->consistOp_ = [&](kvstore::BatchHolder& a, std::vector<kvstore::KV>* b) {
        callbackOfChainOp(a, b);
    };
    proc->process(req_);
    // LOG(ERROR) << "what if proc->process() failed";
    auto futProc = proc->getFuture();
    auto [pro, fut] = folly::makePromiseContract<Code>();
    std::move(futProc).thenValue(
        [&, p = std::move(pro)](auto&& resp) mutable { p.setValue(extractRpcError(resp)); });
    return std::move(fut);
}

Code ChainAddEdgesProcessorLocal::extractRpcError(const cpp2::ExecResponse& resp) {
    Code ret = Code::SUCCEEDED;
    auto& respComn = resp.get_result();
    for (auto& part : respComn.get_failed_parts()) {
        ret = part.code;
    }
    return ret;
}

void ChainAddEdgesProcessorLocal::doRpc(folly::Promise<Code>&& promise,
                                        cpp2::AddEdgesRequest&& req,
                                        int retry) noexcept {
    if (retry > retryLimit_) {
        promise.setValue(Code::E_LEADER_CHANGED);
        return;
    }
    // CHECK_NE(req.get_parts().size(), 1);
    auto* iClient = env_->txnMan_->getInternalClient();
    folly::Promise<Code> p;
    auto f = p.getFuture();
    iClient->chainAddEdges(req, localTerm_, edgeVer_, std::move(p));

    std::move(f).thenTry([=, p = std::move(promise)](auto&& t) mutable {
        auto code = t.hasValue() ? t.value() : Code::E_RPC_FAILURE;
        switch (code) {
            case Code::E_LEADER_CHANGED:
                doRpc(std::move(p), std::move(req), ++retry);
                break;
            default:
                p.setValue(code);
                break;
        }
        return code;
    });
}

void ChainAddEdgesProcessorLocal::callbackOfChainOp(kvstore::BatchHolder& batch,
                                                    std::vector<kvstore::KV>* pData) {
    if (pData != nullptr) {
        for (auto& kv : *pData) {
            batch.put(std::string(kv.first), std::string(kv.second));
        }
    }
    for (auto& key : kvErased_) {
        batch.remove(std::string(key));
    }
    for (auto& kv : kvAppend_) {
        batch.put(std::string(kv.first), std::string(kv.second));
    }
    // LOG(WARNING) << "will do perf optimize later";
}

folly::SemiFuture<Code> ChainAddEdgesProcessorLocal::abort() {
    if (kvErased_.empty()) {
        return Code::SUCCEEDED;
    }
    auto [pro, fut] = folly::makePromiseContract<Code>();
    env_->kvstore_->asyncMultiRemove(req_.get_space_id(),
                                     localPartId_,
                                     std::move(kvErased_),
                                     [p = std::move(pro)](auto rc) mutable {
                                         LOG_IF(WARNING, rc != nebula::cpp2::ErrorCode::SUCCEEDED)
                                             << "error: " << static_cast<int>(rc);
                                         p.setValue(rc);
                                     });
    return std::move(fut);
}

std::vector<kvstore::KV> ChainAddEdgesProcessorLocal::makePrime() {
    std::vector<kvstore::KV> ret;
    for (auto& edge : req_.get_parts().begin()->second) {
        auto key = ConsistUtil::primeKey(spaceVidLen_, localPartId_, edge.get_key());

        auto req = makeSingleEdgeRequest(localPartId_, edge);
        std::string val;
        apache::thrift::CompactSerializer::serialize(req, &val);
        val.append(ConsistUtil::insertIdentifier());

        ret.emplace_back(std::make_pair(std::move(key), std::move(val)));
    }
    return ret;
}

std::vector<kvstore::KV> ChainAddEdgesProcessorLocal::makeDoublePrime() {
    std::vector<kvstore::KV> ret;
    for (auto& edge : req_.get_parts().begin()->second) {
        auto key = ConsistUtil::doublePrime(spaceVidLen_, localPartId_, edge.get_key());

        auto req = makeSingleEdgeRequest(localPartId_, edge);
        std::string val;
        apache::thrift::CompactSerializer::serialize(req, &val);
        val.append(ConsistUtil::insertIdentifier());

        ret.emplace_back(std::make_pair(std::move(key), std::move(val)));
    }
    return ret;
}

void ChainAddEdgesProcessorLocal::erasePrime() {
    auto fn = [&](const cpp2::NewEdge& edge) {
        return ConsistUtil::primeKey(spaceVidLen_, localPartId_, edge.get_key());
    };
    for (auto& edge : req_.get_parts().begin()->second) {
        kvErased_.push_back(fn(edge));
    }
}

void ChainAddEdgesProcessorLocal::eraseDoublePrime() {
    auto fn = [&](const cpp2::NewEdge& edge) {
        return ConsistUtil::doublePrime(spaceVidLen_, localPartId_, edge.get_key());
    };
    for (auto& edge : req_.get_parts().begin()->second) {
        kvErased_.push_back(fn(edge));
    }
}

bool ChainAddEdgesProcessorLocal::lockEdges(const cpp2::AddEdgesRequest& req) {
    std::vector<std::string> keys;
    auto partId = req.get_parts().begin()->first;
    for (auto& edge : req.get_parts().begin()->second) {
        keys.emplace_back(ConsistUtil::edgeKey(spaceVidLen_, partId, edge.get_key()));
    }
    auto* lockCore = env_->txnMan_->getLockCore(req.get_space_id());
    lk_ = std::make_unique<TransactionManager::LockGuard>(lockCore, keys);
    return lk_->isLocked();
}

// we need to check term at both remote phase and local commit
bool ChainAddEdgesProcessorLocal::checkTerm(const cpp2::AddEdgesRequest& req) {
    auto space = req.get_space_id();
    auto partId = req.get_parts().begin()->first;
    auto ret = env_->txnMan_->checkTerm(space, partId, localTerm_);
    LOG_IF(WARNING, !ret) << "check term failed, localTerm_ = " << localTerm_;
    return ret;
}

// check if current edge is not newer than the one trying to resume.
// this function only take effect in resume mode
bool ChainAddEdgesProcessorLocal::checkVersion(const cpp2::AddEdgesRequest& req) {
    auto part = req.get_parts().begin()->first;
    auto sKeys = sEdgeKey(req);
    auto currVer = ConsistUtil::getMultiEdgeVers(env_->kvstore_, spaceId_, part, sKeys);
    for (auto i = 0U; i != currVer.size(); ++i) {
        if (currVer[i] < resumedEdgeVer_) {
            return false;
        }
    }
    return true;
}

std::vector<std::string>
ChainAddEdgesProcessorLocal::sEdgeKey(const cpp2::AddEdgesRequest& req) {
    std::vector<std::string> ret;
    for (auto& edgesOfPart : req.get_parts()) {
        auto partId = edgesOfPart.first;
        for (auto& edge : edgesOfPart.second) {
            ret.emplace_back(ConsistUtil::edgeKey(spaceVidLen_, partId, edge.get_key()));
        }
    }
    return ret;
}

cpp2::AddEdgesRequest
ChainAddEdgesProcessorLocal::reverseRequest(const cpp2::AddEdgesRequest& req) {
    cpp2::AddEdgesRequest reversedRequest;
    for (auto& edgesOfPart : *req.parts_ref()) {
        for (auto& newEdge : edgesOfPart.second) {
            (*reversedRequest.parts_ref())[remotePartId_].emplace_back(newEdge);
            auto& newEdgeRef = (*reversedRequest.parts_ref())[remotePartId_].back();
            ConsistUtil::reverseEdgeKeyInplace(*newEdgeRef.key_ref());
        }
    }
    reversedRequest.set_space_id(req.get_space_id());
    reversedRequest.set_prop_names(req.get_prop_names());
    reversedRequest.set_if_not_exists(req.get_if_not_exists());
    return reversedRequest;
}

void ChainAddEdgesProcessorLocal::finish() {
    pushResultCode(code_, localPartId_);
    onFinished();
}

cpp2::AddEdgesRequest ChainAddEdgesProcessorLocal::makeSingleEdgeRequest(
    PartitionID partId,
    const cpp2::NewEdge& edge) {
    cpp2::AddEdgesRequest req;
    req.set_space_id(req_.get_space_id());
    req.set_prop_names(req_.get_prop_names());
    req.set_if_not_exists(req_.get_if_not_exists());

    std::unordered_map<PartitionID, std::vector<cpp2::NewEdge>> newParts;
    newParts[partId].emplace_back(edge);

    req.set_parts(newParts);
    return req;
}

}  // namespace storage
}  // namespace nebula
