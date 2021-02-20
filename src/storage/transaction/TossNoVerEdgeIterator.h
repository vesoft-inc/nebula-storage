/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include <folly/ScopeGuard.h>
#include <folly/String.h>
#include <boost/stacktrace.hpp>

#include "common/base/Base.h"
#include "kvstore/KVIterator.h"
#include "storage/CommonUtils.h"
#include "storage/StorageFlags.h"
#include "storage/exec/StorageIterator.h"
#include "storage/transaction/TransactionManager.h"

namespace nebula {
namespace storage {

class PendingLockIter {
    using Container = std::list<std::shared_ptr<PendingLock>>;
    using DataChecker = std::function<bool(folly::StringPiece)>;

public:
    PendingLockIter(Container& locks, DataChecker checker)
        : locks_(locks), dataChecker_(std::move(checker)) {
        LOG(INFO) << "PendingLockIter::ctor(), locks_.size() = " << locks_.size();
        curr_ = locks_.begin();
        if (!check()) {
            next();
        }
        LOG(INFO) << "~PendingLockIter::ctor()";
    }

    bool valid() const {
        LOG(INFO) << "PendingLockIter::valid()";
        if (locks_.empty()) {
            return false;
        }
        auto valid = curr_ != locks_.end();
        LOG(INFO) << "~PendingLockIter::valid(), valid = " << valid;
        return valid;
    }

    bool check() {
        LOG(INFO) << "enter PendingLockIter::check()";
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

    // bool check() {
    //     LOG(INFO) << "PendingLockIter::check()";
    //     bool ret = false;
    //     do {
    //         if (!valid()) {
    //             LOG(INFO) << "!valid()";
    //             break;
    //         }
    //         if (emptyPendLock(curr_->get())) {
    //             LOG(INFO) << "emptyPendLock(curr_->get()";
    //             break;
    //         }
    //         auto& lock = *(*curr_);
    //         if (lock.lockProps) {
    //             LOG(INFO) << "lock.lockProps is true " << folly::hexlify(*lock.lockProps);
    //         } else {
    //             LOG(INFO) << "lock.lockProps is false ";
    //         }
    //         if (lock.edgeProps) {
    //             LOG(INFO) << "lock.edgeProps is true " << folly::hexlify(*lock.edgeProps);
    //         } else {
    //             LOG(INFO) << "lock.edgeProps is false ";
    //         }
    //         auto& val = lock.lockProps ? *lock.lockProps : *lock.edgeProps;
    //         if (!dataChecker_(val)) {
    //             LOG(INFO) << "!dataChecker_(val)";
    //             break;
    //         }
    //         ret = true;
    //     } while(0);

    //     LOG(INFO) << "~PendingLockIter::check(), ret = " << ret;
    //     return ret;
    // }

    void next() {
        LOG(INFO) << "PendingLockIter::next()";
        for (++curr_; valid(); ++curr_) {
            if (check()) {
                break;
            }
        }
        LOG(INFO) << "~PendingLockIter::next()";
    }

    bool emptyPendLock(PendingLock* pendingLock) {
        return !(pendingLock->edgeProps || pendingLock->lockProps);
    }

    folly::StringPiece key() {
        std::shared_ptr<PendingLock> splk = *curr_;
        LOG(INFO) << "PendingLockIter::key()=" << folly::hexlify(splk->lockKey);
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
        LOG(INFO) << "TossNoVerEdgeIterator::ctor(), iter->key=" << folly::hexlify(iter->key());
        LOG(INFO) << boost::stacktrace::stacktrace();
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
        LOG(INFO) << "~TossNoVerEdgeIterator::ctor()";
    }

    folly::StringPiece key() const override {
        LOG(INFO) << "enter TossNoVerEdgeIterator::key()";
        return iter_->valid() ? iter_->key() : lockIter_->key();
    }

    bool valid() const override {
        LOG(INFO) << "TossNoVerEdgeIterator::valid()";
        bool valid = iter_->valid() || lockIter_->valid();
        LOG(INFO) << "~TossNoVerEdgeIterator::valid(), valid = " << valid;
        return valid;
    }

    void next() override {
        LOG(INFO) << "TossNoVerEdgeIterator::next()";
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
            LOG(INFO) << "TossNoVerEdgeIterator::next(), !iter_->valid()";
            if (!lockIter_) {
                if (!resumeTasks_.empty()) {
                    folly::collectAll(resumeTasks_).wait();
                }
                auto dataChecker = [&](folly::StringPiece val) {
                    LOG(INFO) << "dataChecker() val=" << folly::hexlify(val);
                    reader_.reset(*schemas_, val);
                    if (!reader_) {
                        planContext_->resultStat_ = ResultStatus::ILLEGAL_DATA;
                        return false;
                    }

                    if (hasTtl_ &&
                        CommonUtils::checkDataExpiredForTTL(
                            schemas_->back().get(), reader_.get(), ttlCol_, ttlDuration_)) {
                        reader_.reset();
                        return false;
                    }

                    return true;
                };
                lockIter_ =
                    std::make_unique<PendingLockIter>(pendingLocks_, std::move(dataChecker));
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
            LOG(INFO) << "meet a lock: " << folly::hexlify(iter_->key());
            pendingLocks_.emplace_back(std::make_shared<PendingLock>(iter_->key()));
            resumeTasks_.emplace_back(planContext_->env_->txnMan_->resumeLock(
                planContext_->vIdLen_, planContext_->spaceId_, pendingLocks_.back()));
            checkEdgeHasLock_ = true;
        } else if (NebulaKeyUtils::isEdge(planContext_->vIdLen_, iter_->key())) {
            LOG(INFO) << "meet an edge: " << folly::hexlify(iter_->key());
            if (hasPendingLock(iter_->key())) {
                pendingLocks_.back()->edgeProps = iter_->val().str();
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

    bool hasPendingLock(const folly::StringPiece& ekey) {
        LOG(INFO) << "enter TossNoVerEdgeIterator::nextEdge()";
        if (!checkEdgeHasLock_) {
            return false;
        }
        checkEdgeHasLock_ = false;

        auto& prevLock = pendingLocks_.back()->lockKey;
        folly::StringPiece lockWoPlaceHolder(prevLock.data(), prevLock.size() - 1);
        return ekey.startsWith(lockWoPlaceHolder);
    }

private:
    bool                                                 stopAtFirstEdge_{false};
    std::list<std::shared_ptr<PendingLock>> pendingLocks_;
    std::list<folly::SemiFuture<cpp2::ErrorCode>> resumeTasks_;
    std::unique_ptr<PendingLockIter> lockIter_;
    // use this for fast check if an edge has a lock(by avoid strcmp).
    bool checkEdgeHasLock_{false};
};

}   // namespace storage
}   // namespace nebula
