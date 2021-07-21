/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <folly/container/Enumerate.h>

#include "codec/RowWriterV2.h"
#include "storage/CommonUtils.h"
#include "storage/transaction/ChainResumeProcessor.h"
#include "storage/transaction/TransactionManager.h"
#include "utils/NebulaKeyUtils.h"

namespace nebula {
namespace storage {

DEFINE_int32(resume_interval_secs, 10, "Resume interval");

ProcessorCounters kForwardTranxCounters;

TransactionManager::TransactionManager(StorageEnv* env) : env_(env) {
    exec_ = std::make_shared<folly::IOThreadPoolExecutor>(10);
    iClient_ = std::make_unique<storage::InternalStorageClient>(exec_, env_->metaClient_);
    resumeThread_ = std::make_unique<thread::GenericWorker>();
}

TransactionManager::LockCore* TransactionManager::getLockCore(GraphSpaceID spaceId) {
    auto it = memLocks_.find(spaceId);
    if (it != memLocks_.end()) {
        return it->second.get();
    }

    auto item = memLocks_.insert(spaceId, std::make_unique<LockCore>());
    return item.first->second.get();
}

StatusOr<TermID> TransactionManager::getTerm(GraphSpaceID spaceId, PartitionID partId) {
    return env_->metaClient_->getTermFromCache(spaceId, partId);
}

bool TransactionManager::checkTerm(GraphSpaceID spaceId, PartitionID partId, TermID term) {
    auto termOfMeta = env_->metaClient_->getTermFromCache(spaceId, partId);
    if (termOfMeta.ok()) {
        if (term < termOfMeta.value()) {
            LOG(WARNING) << "checkTerm() failed: term = " << term
                         << ", termOfMeta = " << termOfMeta.value();
            return false;
        }
    }
    auto partUUID = std::make_pair(spaceId, partId);
    auto it = cachedTerms_.find(partUUID);
    if (it != cachedTerms_.cend()) {
        if (term < it->second) {
            LOG(WARNING) << "term < it->second";
            return false;
        }
    }
    cachedTerms_.assign(partUUID, term);
    LOG(WARNING) << "checkTerm succeed";
    return true;
}

void TransactionManager::resumeThread() {
    SCOPE_EXIT {
        resumeThread_->addDelayTask(
            FLAGS_resume_interval_secs * 1000, &TransactionManager::resumeThread, this);
    };
    ChainResumeProcessor proc(env_);
    proc.process();
}

void TransactionManager::start() {
    resumeThread_->addDelayTask(
        FLAGS_resume_interval_secs * 1000, &TransactionManager::resumeThread, this);
}

void TransactionManager::stop() {
    resumeThread_->stop();
}

std::unique_ptr<TransactionManager::LockGuard> TransactionManager::tryLock(GraphSpaceID spaceId,
                                                       folly::StringPiece key) {
    return std::make_unique<TransactionManager::LockGuard>(getLockCore(spaceId), key.str());
}

}   // namespace storage
}   // namespace nebula
