/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <thrift/lib/cpp/util/EnumUtils.h>
#include "storage/transaction/ChainUpdateEdgeProcessorLocal.h"
#include "storage/transaction/ConsistUtil.h"
#include "storage/transaction/TransactionManager.h"
#include "storage/mutate/UpdateEdgeProcessor.h"
#include "storage/StorageFlags.h"

namespace nebula {
namespace storage {

using ECode = ::nebula::cpp2::ErrorCode;

void ChainUpdateEdgeProcessorLocal::process(const cpp2::UpdateEdgeRequest& req) {
    processInternal(req, ChainProcessType::NORMAL);
}

void ChainUpdateEdgeProcessorLocal::resumeChain(folly::StringPiece val) {
    req_ = ConsistUtil::parseUpdateRequest(val);
    ver_ = getVersion(req_);
    processInternal(req_, ChainProcessType::RESUME_CHAIN);
}

void ChainUpdateEdgeProcessorLocal::resumeRemote(folly::StringPiece val) {
    req_ = ConsistUtil::parseUpdateRequest(val);

    ver_ = getVersion(req_);
    processInternal(req_, ChainProcessType::RESUME_REMOTE);
}

void ChainUpdateEdgeProcessorLocal::processInternal(const cpp2::UpdateEdgeRequest& req,
                                                    ChainProcessType type) {
    req_ = req;
    processType_ = type;
    spaceId_ = req.get_space_id();
    env_->txnMan_->addChainTask(this);
}

ECode
ChainUpdateEdgeProcessorLocal::checkAndBuildContexts(const cpp2::UpdateEdgeRequest&) {
    LOG(WARNING) << "fake return succeeded";
    return ECode::SUCCEEDED;
}

std::string ChainUpdateEdgeProcessorLocal::edgeKey(const cpp2::UpdateEdgeRequest& req) {
    return ConsistUtil::edgeKey(spaceVidLen_, req.get_part_id(), req.get_edge_key());
}

void ChainUpdateEdgeProcessorLocal::finish() {
    pushResultCode(code_, req_.get_part_id());
    onFinished();
}

/**
 * 1. set mem lock
 * 2. set edge prime
 * */
folly::SemiFuture<ECode> ChainUpdateEdgeProcessorLocal::prepareLocal() {
    auto partId = req_.get_part_id();
    auto _term = env_->txnMan_->getTerm(req_.get_space_id(), partId);
    if (_term.ok()) {
        termOfPrepare_ = _term.value();
    } else {
        return ECode::E_PART_NOT_FOUND;
    }

    if (!setLock()) {
        LOG(INFO) << "set lock failed, return E_DATA_CONFLICT_ERROR";
        return ECode::E_DATA_CONFLICT_ERROR;
    }

    if (processType_ == ChainProcessType::RESUME_REMOTE) {
        // resume will skip prepare local
        LOG(INFO) << "processType_ == ChainProcessType::RESUMEREMOTE";
        auto key = ConsistUtil::doublePrime(spaceVidLen_, partId, req_.get_edge_key());
        kvErased_.emplace_back(key);
        return ECode::SUCCEEDED;
    }

    if (processType_ == ChainProcessType::RESUME_CHAIN) {
        // resume will skip prepare local
        LOG(INFO) << "processType_ == ChainProcessType::RESUMECHAIN";
        auto key = ConsistUtil::edgeKeyPrime(spaceVidLen_, partId, req_.get_edge_key());
        kvErased_.emplace_back(key);
        return ECode::SUCCEEDED;
    }

    auto key = ConsistUtil::edgeKeyPrime(spaceVidLen_, partId, req_.get_edge_key());
    kvErased_.emplace_back(key);

    std::string val;
    apache::thrift::CompactSerializer::serialize(req_, &val);
    val.append(ConsistUtil::updateIdentifier());
    std::vector<nebula::kvstore::KV> data{{key, val}};
    auto c = folly::makePromiseContract<ECode>();
    env_->kvstore_->asyncMultiPut(spaceId_,
                                  partId,
                                  std::move(data),
                                  [p = std::move(c.first)](auto rc) mutable {
                                      p.setValue(rc);
                                  });
    return std::move(c.second);
}

folly::SemiFuture<ECode> ChainUpdateEdgeProcessorLocal::processRemote(ECode code) {
    LOG(INFO) << __func__ << "(), code = " << apache::thrift::util::enumNameSafe(code);
    if (code != ECode::SUCCEEDED) {
        return code;
    }
    auto [pro, fut] = folly::makePromiseContract<ECode>();
    doRpc(std::move(pro));
    return std::move(fut);
}

void ChainUpdateEdgeProcessorLocal::doRpc(folly::Promise<ECode>&& promise,
                                                     int retry) noexcept {
    if (retry > retryLimit_) {
        promise.setValue(ECode::E_LEADER_CHANGED);
        return;
    }
    auto* iClient = env_->txnMan_->getInternalClient();
    folly::Promise<ECode> p;
    auto reversedReq  = reverseRequest(req_);
    iClient->chainUpdateEdge(reversedReq, termOfPrepare_, std::move(p));

    auto f = p.getFuture();
    std::move(f).thenTry([=, p = std::move(promise)](auto&& t) mutable {
        auto code = t.hasValue() ? t.value() : ECode::E_RPC_FAILURE;
        LOG(INFO) << "code = " << apache::thrift::util::enumNameSafe(code);
        switch (code) {
            case ECode::E_LEADER_CHANGED:
                doRpc(std::move(p), ++retry);
                break;
            default:
                p.setValue(code);
                break;
        }
        return code;
    });
}

folly::SemiFuture<ECode> ChainUpdateEdgeProcessorLocal::processLocal(ECode code) {
    LOG(INFO) << __func__ << "(), code = " << apache::thrift::util::enumNameSafe(code);
    if (code != ECode::SUCCEEDED && code_ == ECode::SUCCEEDED) {
        code_ = code;
    }
    if (processType_ == ChainProcessType::RESUME_REMOTE) {
        return processResumeRemoteLocal(code);
    }
    return processNormalLocal(code);
}

folly::SemiFuture<ECode>
ChainUpdateEdgeProcessorLocal::processResumeRemoteLocal(ECode code) {
    LOG(INFO) << __func__ << "(), code = " << apache::thrift::util::enumNameSafe(code);
    if (code == ECode::SUCCEEDED) {
        auto partId = req_.get_part_id();
        auto key = ConsistUtil::doublePrime(spaceVidLen_, partId, req_.get_edge_key());
        kvErased_.emplace_back(key);
        doClean();
    }
    return code;
}

folly::SemiFuture<ECode>
ChainUpdateEdgeProcessorLocal::processNormalLocal(ECode code) {
    LOG(INFO) << "processNormalLocal(), code: " << apache::thrift::util::enumNameSafe(code);
    if (!checkTerm()) {
        LOG(WARNING) << "checkTerm() failed";
        return ECode::E_OUTDATED_TERM;
    }

    if (!checkVersion()) {
        LOG(WARNING) << "checkVersion() failed";
        return ECode::E_OUTDATED_EDGE;
    }

    std::vector<std::pair<std::string, std::string>> kvAppend;
    auto partId = req_.get_part_id();
    if (code == ECode::E_RPC_FAILURE) {
        auto key = ConsistUtil::doublePrime(spaceVidLen_, partId, req_.get_edge_key());
        std::string val;
        apache::thrift::CompactSerializer::serialize(req_, &val);
        val += ConsistUtil::updateIdentifier();
        kvAppend.emplace_back(std::make_pair(std::move(key), std::move(val)));
    }

    if (code == ECode::SUCCEEDED || code == ECode::E_RPC_FAILURE) {
        kUpdateEdgeCounters.init("update_edge");
        UpdateEdgeProcessor::ContextAdjuster fn = [=](EdgeContext& ctx) {
            ctx.kvAppend = std::move(kvAppend);
            ctx.kvErased = std::move(kvErased_);
        };

        auto* proc = UpdateEdgeProcessor::instance(env_);
        proc->adjustContext(std::move(fn));
        auto f = proc->getFuture();
        proc->process(req_);
        auto resp = std::move(f).get();
        code_ = getErrorCode(resp);
        std::swap(resp_, resp);
    } else {
        doClean();
        return code_;
    }

    LOG(INFO) << "~processNormalLocal(), code_: " << apache::thrift::util::enumNameSafe(code);
    return code_;
}

bool ChainUpdateEdgeProcessorLocal::checkTerm() {
    return env_->txnMan_->checkTerm(req_.get_space_id(), req_.get_part_id(), termOfPrepare_);
}

bool ChainUpdateEdgeProcessorLocal::checkVersion() {
    if (!ver_) {
        return true;
    }
    auto [ver, rc] = ConsistUtil::versionOfUpdateReq(env_, req_);
    if (rc != ECode::SUCCEEDED) {
        return false;
    }
    return *ver_ == ver;
}

void ChainUpdateEdgeProcessorLocal::doClean() {
    if (kvErased_.empty()) {
        return;
    }
    folly::Baton<true, std::atomic> baton;
    env_->kvstore_->asyncMultiRemove(
        req_.get_space_id(), req_.get_part_id(), std::move(kvErased_), [&](auto rc) mutable {
            LOG_IF(WARNING, rc != ECode::SUCCEEDED) << "error: " << static_cast<int>(rc);
            baton.post();
        });
    baton.wait();
}

cpp2::UpdateEdgeRequest ChainUpdateEdgeProcessorLocal::reverseRequest(
    const cpp2::UpdateEdgeRequest& req) {
    cpp2::UpdateEdgeRequest reversedRequest(req);
    auto reversedEdgeKey = ConsistUtil::reverseEdgeKey(req.get_edge_key());
    reversedRequest.set_edge_key(reversedEdgeKey);

    auto partsNum = env_->metaClient_->partsNum(req.get_space_id());
    CHECK(partsNum.ok());
    auto srcVid = reversedRequest.get_edge_key().get_src().getStr();
    auto partId = env_->metaClient_->partId(partsNum.value(), srcVid);
    CHECK(partId.ok());
    reversedRequest.set_part_id(partId.value());

    return reversedRequest;
}

bool ChainUpdateEdgeProcessorLocal::setLock() {
    auto spaceId = req_.get_space_id();
    auto* lockCore = env_->txnMan_->getLockCore(spaceId);
    if (lockCore == nullptr) {
        return false;
    }
    auto key = ConsistUtil::edgeKey(spaceVidLen_, req_.get_part_id(), req_.get_edge_key());
    lk_ = std::make_unique<MemoryLockGuard<std::string>>(lockCore, key);
    return lk_->isLocked();
}

int64_t ChainUpdateEdgeProcessorLocal::getVersion(const cpp2::UpdateEdgeRequest& req) {
    int64_t invalidVer = -1;
    auto spaceId = req.get_space_id();
    auto vIdLen = env_->metaClient_->getSpaceVidLen(spaceId);
    if (!vIdLen.ok()) {
        LOG(WARNING) << vIdLen.status().toString();
        return invalidVer;
    }
    auto partId = req.get_part_id();
    auto key = ConsistUtil::edgeKey(vIdLen.value(), partId, req.get_edge_key());
    return ConsistUtil::getSingleEdgeVer(env_->kvstore_, spaceId, partId, key);;
}

nebula::cpp2::ErrorCode ChainUpdateEdgeProcessorLocal::getErrorCode(
    const cpp2::UpdateResponse& resp) {
    auto& respCommon = resp.get_result();
    auto& parts = respCommon.get_failed_parts();
    if (parts.empty()) {
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    }
    return parts.front().get_code();
}

}  // namespace storage
}  // namespace nebula
