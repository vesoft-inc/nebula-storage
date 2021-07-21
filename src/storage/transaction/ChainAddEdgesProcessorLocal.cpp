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

using CODE = ::nebula::cpp2::ErrorCode;
using SVEC = std::vector<std::string>;

void ChainAddEdgesProcessorLocal::process(const cpp2::AddEdgesRequest& req) {
    processInternal(req, ChainProcessType::NORMAL);
}

void ChainAddEdgesProcessorLocal::resumeChain(GraphSpaceID spaceId,
                                              PartitionID partId,
                                              folly::StringPiece val) {
    CHECK(resumeRequest(spaceId, partId, val));
    std::vector<std::string> keys = strEdgekeys(req_);
    edgeVers_ = ConsistUtil::getMultiEdgeVers(env_->kvstore_, spaceId, partId, keys);
    processInternal(req_, ChainProcessType::RESUME_CHAIN);
}

void ChainAddEdgesProcessorLocal::resumeRemote(GraphSpaceID spaceId,
                                               PartitionID partId,
                                               folly::StringPiece val) {
    CHECK(resumeRequest(spaceId, partId, val));
    std::vector<std::string> keys = strEdgekeys(req_);
    edgeVers_ = ConsistUtil::getMultiEdgeVers(env_->kvstore_, spaceId, partId, keys);
    processInternal(req_, ChainProcessType::RESUME_REMOTE);
}

void ChainAddEdgesProcessorLocal::processInternal(const cpp2::AddEdgesRequest& req,
                                                  ChainProcessType type) {
    req_ = req;
    processType_ = type;
    localPartId_ = req.get_parts().begin()->first;
    auto spaceId = req.get_space_id();
    const auto& partEdges = req.get_parts();
    auto ret = env_->schemaMan_->getSpaceVidLen(spaceId);
    if (!ret.ok()) {
        LOG(ERROR) << ret.status();
        for (auto& part : partEdges) {
            pushResultCode(nebula::cpp2::ErrorCode::E_INVALID_SPACEVIDLEN, part.first);
        }
        onFinished();
        return;
    }

    spaceVidLen_ = ret.value();

    if (type != ChainProcessType::NORMAL) {
        std::vector<std::string> keys = strEdgekeys(req);
        edgeVers_ = ConsistUtil::getMultiEdgeVers(env_->kvstore_, spaceId, localPartId_, keys);
    }
    env_->txnMan_->addChainTask(this);
}

/**
 * @brief
 * 1. check term
 * 2. set mem lock
 * 3. write edge prime(key = edge prime, value = transaction id)
 */
folly::SemiFuture<CODE> ChainAddEdgesProcessorLocal::prepareLocal() {
    if (!lockEdges(req_)) {
        return CODE::E_WRITE_WRITE_CONFLICT;
    }
    CHECK_EQ(req_.get_parts().size(), 1);
    auto spaceId = req_.get_space_id();
    localPartId_ = req_.get_parts().begin()->first;
    auto localTerm = env_->txnMan_->getTerm(spaceId, localPartId_);
    if (localTerm.ok()) {
        localTerm_ = localTerm.value();
    } else {
        LOG(WARNING) << "getTerm failed: space = " << spaceId << ", part = " << localPartId_;
        pushResultCode(nebula::cpp2::ErrorCode::E_NO_TERM, localPartId_);
        return nebula::cpp2::ErrorCode::E_NO_TERM;
    }

    if (processType_ == ChainProcessType::RESUME_CHAIN) {
        auto keyGen = [&](const cpp2::NewEdge& edge){
            return ConsistUtil::edgeKeyPrime(spaceVidLen_, localPartId_, edge.get_key());
        };
        for (auto& edge : req_.get_parts().begin()->second) {
            erasedItems_.push_back(keyGen(edge));
        }
        return CODE::SUCCEEDED;
    }

    if (processType_ == ChainProcessType::RESUME_REMOTE) {
        auto keyGen = [&](const cpp2::NewEdge& edge){
            return ConsistUtil::doublePrime(spaceVidLen_, localPartId_, edge.get_key());
        };
        for (auto& edge : req_.get_parts().begin()->second) {
            erasedItems_.push_back(keyGen(edge));
        }
        return CODE::SUCCEEDED;
    }

    txnId_ = ConsistUtil::strUUID();
    auto keyOfRequest = ConsistUtil::tempRequestTable() + txnId_;

    std::string encodedRequest;
    apache::thrift::CompactSerializer::serialize(req_, &encodedRequest);
    std::vector<kvstore::KV> tempData{{keyOfRequest, encodedRequest}};
    for (auto& edge : req_.get_parts().begin()->second) {
        auto key = ConsistUtil::edgeKeyPrime(spaceVidLen_, localPartId_, edge.get_key());
        erasedItems_.push_back(key);
        tempData.emplace_back(std::make_pair(key, txnId_ + ConsistUtil::insertIdentifier()));
    }

    auto [pro, fut] = folly::makePromiseContract<CODE>();
    env_->kvstore_->asyncMultiPut(
        spaceId, localPartId_, std::move(tempData), [p = std::move(pro)](auto rc) mutable {
            LOG_IF(WARNING, rc != nebula::cpp2::ErrorCode::SUCCEEDED)
                << "kvstore err: " << static_cast<int>(rc);
            p.setValue(rc);
        });
    return std::move(fut);
}

folly::SemiFuture<CODE> ChainAddEdgesProcessorLocal::processRemote(CODE code) {
    if (code != CODE::SUCCEEDED) {
        return code;
    }
    CHECK_EQ(req_.get_parts().size(), 1);
    auto reversedRequest = reverseRequest(req_);
    CHECK_EQ(reversedRequest.get_parts().size(), 1);
    auto [pro, fut] = folly::makePromiseContract<CODE>();
    doRpc(std::move(pro), std::move(reversedRequest));
    return std::move(fut);
}

folly::SemiFuture<CODE> ChainAddEdgesProcessorLocal::processLocal(CODE code) {
    if (!checkTerm(req_)) {
        LOG(WARNING) << "E_OUTDATED_TERM";
        return CODE::E_OUTDATED_TERM;
    }

    if (!checkVersion(req_)) {
        LOG(WARNING) << "E_OUTDATED_EDGE";
        return CODE::E_OUTDATED_EDGE;
    }

    switch (processType_) {
        case ChainProcessType::NORMAL:
        case ChainProcessType::RESUME_CHAIN: {
            if (code == CODE::SUCCEEDED) {
                erasedItems_.emplace_back(ConsistUtil::tempRequestTable() + txnId_);
            } else if (code == CODE::E_RPC_FAILURE) {
                for (auto& edgeOfPart : req_.get_parts()) {
                    auto partId = edgeOfPart.first;
                    for (auto& edge : edgeOfPart.second) {
                        auto key = ConsistUtil::doublePrime(spaceVidLen_, partId, edge.get_key());
                        auto val = txnId_ + ConsistUtil::insertIdentifier();
                        appendData_.emplace_back(std::make_pair(key, val));
                    }
                }
            } else {
                // something wrong for remote side.
                erasedItems_.emplace_back(ConsistUtil::tempRequestTable() + txnId_);
                doClean();
                return code;
            }

            auto* proc = AddEdgesProcessor::instance(env_, nullptr);
            proc->consistOp_ = [&](kvstore::BatchHolder& a, std::vector<kvstore::KV>* b) {
                callbackOfChainOp(a, b);
            };
            proc->process(req_);
            LOG(ERROR) << "what if proc->process() failed";
            auto futProc = proc->getFuture();
            auto [pro, fut] = folly::makePromiseContract<CODE>();
            std::move(futProc).thenValue([&, p = std::move(pro)](auto&& resp) mutable {
                p.setValue(extractRpcError(resp));
            });
            return std::move(fut);
        } break;
        case ChainProcessType::RESUME_REMOTE: {
            if (code == CODE::SUCCEEDED) {
                erasedItems_.emplace_back(ConsistUtil::tempRequestTable() + txnId_);
                return doClean();
            } else {
                // do nothing
            }
        } break;
        default:
            LOG(FATAL) << "shoule not happened";
    }
    return code;
}

CODE ChainAddEdgesProcessorLocal::extractRpcError(const cpp2::ExecResponse& resp) {
    CODE ret = CODE::SUCCEEDED;
    auto& respComn = resp.get_result();
    for (auto& part : respComn.get_failed_parts()) {
        ret = part.code;
    }
    return ret;
}

bool ChainAddEdgesProcessorLocal::resumeRequest(GraphSpaceID spaceId,
                                                PartitionID partId,
                                                folly::StringPiece val) {
    txnId_ = val.subpiece(0, val.size() - 1).str();
    auto keyOfRequest = ConsistUtil::tempRequestTable() + txnId_;
    std::string _val;
    auto rc = env_->kvstore_->get(spaceId, partId, keyOfRequest, &_val);
    if (rc != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(WARNING) << "Resume Key " << keyOfRequest << " not exist";
        return false;
    }
    req_ = ConsistUtil::parseAddRequest(_val);
    return true;
}

void ChainAddEdgesProcessorLocal::doRpc(folly::Promise<CODE>&& promise,
                                                cpp2::AddEdgesRequest&& req,
                                                int retry) noexcept {
    if (retry > retryLimit_) {
        promise.setValue(CODE::E_LEADER_CHANGED);
        return;
    }
    CHECK_NE(req.get_parts().size(), 1);
    auto* iClient = env_->txnMan_->getInternalClient();
    folly::Promise<CODE> p;
    auto f = p.getFuture();
    iClient->chainAddEdges(req, localTerm_, std::move(p));

    std::move(f).thenTry([=, p = std::move(promise)](auto&& t) mutable {
        auto code = t.hasValue() ? t.value() : CODE::E_RPC_FAILURE;
        switch (code) {
            case CODE::E_LEADER_CHANGED:
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
    for (auto& key : erasedItems_) {
        batch.remove(std::string(key));
    }
    for (auto& kv : appendData_) {
        batch.put(std::string(kv.first), std::string(kv.second));
    }
    LOG(WARNING) << "will do perf optimize later";
}


folly::SemiFuture<CODE> ChainAddEdgesProcessorLocal::doClean() {
    auto [pro, fut] = folly::makePromiseContract<CODE>();
    env_->kvstore_->asyncMultiRemove(req_.get_space_id(),
                                     localPartId_,
                                     std::move(erasedItems_),
                                     [p = std::move(pro)](auto rc) mutable {
                                         LOG_IF(WARNING, rc != nebula::cpp2::ErrorCode::SUCCEEDED)
                                             << "error: " << static_cast<int>(rc);
                                         p.setValue(rc);
                                     });
    return std::move(fut);
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
    if (processType_ == ChainProcessType::NORMAL) {
        // only the resume mode need this
        return true;
    }
    auto part = req.get_parts().begin()->first;
    auto sKeys = strEdgekeys(req);
    auto spaceId = req.get_space_id();
    auto currVer = ConsistUtil::getMultiEdgeVers(env_->kvstore_, spaceId, part, sKeys);
    for (auto i = 0U; i != currVer.size(); ++i) {
        if (currVer[i] < resumedEdgeVer_) {
            return false;
        }
    }
    return true;
}

SVEC ChainAddEdgesProcessorLocal::strEdgekeys(const cpp2::AddEdgesRequest& req) {
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
    onFinished();
}

}  // namespace storage
}  // namespace nebula
