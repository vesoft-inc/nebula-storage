/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include <folly/container/Enumerate.h>

#include "codec/RowWriterV2.h"
#include "common/clients/storage/InternalStorageClient.h"
#include "storage/CommonUtils.h"
#include "storage/StorageFlags.h"
#include "storage/mutate/AddEdgesProcessor.h"
#include "storage/transaction/TransactionManager.h"
#include "storage/transaction/TransactionUtils.h"
#include "storage/transaction/ChainResumeProcessor.h"
#include "storage/transaction/ChainUpdateProcessor.h"
#include "utils/NebulaKeyUtils.h"

namespace nebula {
namespace storage {

/*
 * edgeKey : thrift data structure
 * rawKey  : NebulaKeyUtils::edgeKey
 * lockKey : rawKey + lock suffix
 * */
TransactionManager::TransactionManager(StorageEnv* env) : env_(env) {
    exec_ = std::make_shared<folly::IOThreadPoolExecutor>(10);
    interClient_ = std::make_unique<storage::InternalStorageClient>(exec_, env_->metaClient_);
    sClient_ = std::make_unique<GraphStorageClient>(exec_, env_->metaClient_);
}

void TransactionManager::processChain(BaseChainProcessor* proc) {
    auto suc = cpp2::ErrorCode::SUCCEEDED;
    proc->prepareLocal()
        .via(exec_.get())
        .thenValue([=](auto&& code) {
            LOG_IF(INFO, code != suc) << "prepareLocal: " << CommonUtils::name(code);
            return proc->processRemote(code);
        })
        .thenValue([=](auto&& code) {
            LOG_IF(INFO, code != suc) << "processRemote() return: " << CommonUtils::name(code);
            return proc->processLocal(code);
        })
        .thenValue([=](auto&& code) {
            LOG_IF(INFO, code != suc) << "processLocal() return: " << CommonUtils::name(code);
            return proc->setErrorCode(code);
        })
        .ensure([=]() {
            proc->onFinished();
        });
}

/**
 * @brief in-edge first, out-edge last, goes this way
 */
folly::Future<cpp2::ErrorCode> TransactionManager::updateEdge2(
    size_t vIdLen,
    GraphSpaceID spaceId,
    PartitionID partId,
    const cpp2::EdgeKey& edgeKey,
    std::vector<cpp2::UpdatedProp> updateProps,
    bool insertable,
    folly::Optional<std::vector<std::string>> optReturnProps,
    folly::Optional<std::string> optCondition,
    GetBatchFunc&& getter) {
    LOG(INFO) << "updateEdgeAtomicNew";
    std::vector<std::string> returnProps;
    if (optReturnProps) {
        returnProps = *optReturnProps;
    }
    std::string condition;
    if (optCondition) {
        condition = *optCondition;
    }

    auto c = folly::makePromiseContract<cpp2::ErrorCode>();
    auto* processor = ChainUpdateEdgeProcessor::instance(
        env_,
        [&, p = std::move(c.first)](cpp2::ErrorCode code) mutable { p.setValue(code); },
        spaceId,
        partId,
        edgeKey,
        updateProps,
        insertable,
        returnProps,
        condition,
        std::move(getter));

    processor->setVidLen(vIdLen);

    processChain(processor);
    LOG(INFO) << "~updateEdgeAtomicNew";
    return std::move(c.second).via(exec_.get());
}

folly::Future<cpp2::ErrorCode> TransactionManager::resumeLock(PlanContext* planCtx,
                                          std::shared_ptr<PendingLock>& lock) {
    LOG(INFO) << "resume lock: " << folly::hexlify(lock->lockKey);
    auto c = folly::makePromiseContract<cpp2::ErrorCode>();
    auto cb = [&, p = std::move(c.first)](cpp2::ErrorCode code) mutable {
        p.setValue(code);
    };
    auto* processor = ChainResumeProcessor::instance(env_, std::move(cb), planCtx, lock);
    processChain(processor);
    return std::move(c.second).via(exec_.get());
}

// combine multi put and remove in a batch
// this may sometimes reduce some raft operation
folly::SemiFuture<kvstore::ResultCode> TransactionManager::commitBatch(GraphSpaceID spaceId,
                                                                       PartitionID partId,
                                                                       std::string& batch) {
    auto c = folly::makePromiseContract<kvstore::ResultCode>();
    env_->kvstore_->asyncAppendBatch(
        spaceId, partId, batch, [pro = std::move(c.first)](kvstore::ResultCode rc) mutable {
            pro.setValue(rc);
        });
    return std::move(c.second);
}

meta::cpp2::IsolationLevel TransactionManager::getSpaceIsolationLvel(GraphSpaceID spaceId) {
    auto ret = env_->metaClient_->getIsolationLevel(spaceId);
    if (!ret.ok()) {
        return meta::cpp2::IsolationLevel::DEFAULT;
    }
    return ret.value();
}

std::string TransactionManager::remoteEdgeKey(size_t vIdLen,
                                              GraphSpaceID spaceId,
                                              const std::string& lockKey) {
    VertexIDSlice dstId = NebulaKeyUtils::getDstId(vIdLen, lockKey);
    auto stPart = env_->metaClient_->partId(spaceId, dstId.str());
    if (!stPart.ok()) {
        return "";
    }
    return TransactionUtils::reverseRawKey(vIdLen, stPart.value(), lockKey);
}

}   // namespace storage
}   // namespace nebula
