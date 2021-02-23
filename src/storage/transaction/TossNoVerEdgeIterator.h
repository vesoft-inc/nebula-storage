/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include <folly/ScopeGuard.h>
#include <folly/String.h>

#include "common/base/Base.h"
#include "kvstore/KVIterator.h"
#include "storage/CommonUtils.h"
#include "storage/StorageFlags.h"
#include "storage/exec/StorageIterator.h"
#include "storage/transaction/TransactionManager.h"

namespace nebula {
namespace storage {

class LockIter {
    using Container = std::list<std::shared_ptr<PendingLock>>;
    using DataChecker = std::function<bool(folly::StringPiece)>;

public:
    LockIter(Container& locks, DataChecker checker)
        : locks_(locks), dataChecker_(std::move(checker)) {
        curr_ = locks_.begin();
        if (!check()) {
            next();
        }
    }

    bool valid() const {
        if (locks_.empty()) {
            return false;
        }
        auto valid = curr_ != locks_.end();
        return valid;
    }

    bool check() {
        if (!valid()) {
            return false;
        }
        if (emptyPendLock(curr_->get())) {
            return false;
        }
        auto& lock = *(*curr_);
        auto& val = lock.lockProps ? *lock.lockProps : *lock.edgeProps;
        if (!dataChecker_(val)) {
            return false;
        }
        return true;
    }

    void next() {
        for (++curr_; valid(); ++curr_) {
            if (check()) {
                break;
            }
        }
    }

    bool emptyPendLock(PendingLock* pendingLock) {
        return !(pendingLock->edgeProps || pendingLock->lockProps);
    }

    folly::StringPiece key() {
        std::shared_ptr<PendingLock> splk = *curr_;
        return splk->lockKey;
    }

private:
    Container& locks_;
    Container::iterator curr_;
    DataChecker dataChecker_;
};

class TossNoVerEdgeIterator : public SingleEdgeIterator {
public:
    TossNoVerEdgeIterator(
        PlanContext* planCtx,
        std::unique_ptr<kvstore::KVIterator> iter,
        EdgeType edgeType,
        const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>* schemas,
        const folly::Optional<std::pair<std::string, int64_t>>* ttl,
        bool stopAtFirstEdge)
        : SingleEdgeIterator() {
        planContext_ = planCtx;
        iter_ = std::move(iter);
        edgeType_ = edgeType;
        schemas_ = schemas;
        stopAtFirstEdge_ = stopAtFirstEdge;
        if (ttl->hasValue()) {
            hasTtl_ = true;
            ttlCol_ = ttl->value().first;
            ttlDuration_ = ttl->value().second;
        }
        if (!checkIter()) {
            next();
        }
    }

    folly::StringPiece key() const override {
        return iter_->valid() ? iter_->key() : lockIter_->key();
    }

    bool valid() const override {
        return iter_->valid() || lockIter_->valid();
    }

    void next() override {
        if (stopSearching_) {
            return;
        }

        if (iter_->valid()) {
            for (iter_->next(); iter_->valid(); iter_->next()) {
                if (checkIter()) {
                    return;
                }
            }
        }

        if (!iter_->valid()) {
            if (!lockIter_) {
                folly::collectAll(resumeTasks_).wait();
                lockIter_ =
                    std::make_unique<LockIter>(locks_, [&](auto val) { return checkValue(val); });
            } else {
                if (lockIter_->valid()) {
                    lockIter_->next();
                }
            }
        }
    }

    // check if iter_ point to a valid edge key,
    // and if it points to a lock, try to resume it.
    bool checkIter() {
        if (NebulaKeyUtils::isLock(planContext_->vIdLen_, iter_->key())) {
            LOG_IF(INFO, FLAGS_trace_toss) << "meet a lock: " << folly::hexlify(iter_->key());
            locks_.emplace_back(std::make_shared<PendingLock>(iter_->key()));
            resumeTasks_.emplace_back(
                planContext_->env_->txnMan_->resumeLock(planContext_, locks_.back()));
            checkEdgeHasLock_ = true;
        } else if (NebulaKeyUtils::isEdge(planContext_->vIdLen_, iter_->key())) {
            LOG_IF(INFO, FLAGS_trace_toss) << "meet an edge: " << folly::hexlify(iter_->key());
            if (hasPendingLock(iter_->key())) {
                locks_.back()->edgeProps = iter_->val().str();
                return false;
            }
            if (check()) {
                return true;
            }
        } else {
            LOG(ERROR) << "meet a weird key: " << folly::hexlify(iter_->key());
        }
        return false;
    }

    bool checkValue(folly::StringPiece val) {
        reader_.reset(*schemas_, val);
        if (!reader_) {
            planContext_->resultStat_ = ResultStatus::ILLEGAL_DATA;
            return false;
        }

        if (hasTtl_ && CommonUtils::checkDataExpiredForTTL(
                           schemas_->back().get(), reader_.get(), ttlCol_, ttlDuration_)) {
            reader_.reset();
            return false;
        }
        return true;
    }

    bool hasPendingLock(const folly::StringPiece& ekey) {
        if (!checkEdgeHasLock_) {
            return false;
        }
        checkEdgeHasLock_ = false;

        auto& prevLock = locks_.back()->lockKey;
        folly::StringPiece lockWoPlaceHolder(prevLock.data(), prevLock.size() - 1);
        return ekey.startsWith(lockWoPlaceHolder);
    }

private:
    bool                                                    stopAtFirstEdge_{false};
    std::list<std::shared_ptr<PendingLock>>                 locks_;
    std::list<folly::SemiFuture<cpp2::ErrorCode>>           resumeTasks_;
    std::unique_ptr<LockIter>                               lockIter_;
    // use this for fast check if an edge has a lock(by avoid strcmp).
    bool                                                    checkEdgeHasLock_{false};
};

}   // namespace storage
}   // namespace nebula
