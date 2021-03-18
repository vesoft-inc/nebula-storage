/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once


#include <algorithm>
#include "utils/MemoryLockCore.h"

namespace nebula {
namespace storage {

// RAII style to easily control the lock acquire / release
class ResumeLockGuard {
public:
    ResumeLockGuard(TransactionManager* txnMgr, GraphSpaceID spaceId, const std::string& key)
        : key_(key) {
        lockCore_ = txnMgr->getLockCore(spaceId);
        if (txnMgr->takeDanglingEdge(spaceId, key)) {
            locked_ = true;
        } else {
            locked_ = lockCore_->try_lock(key_);
        }
    }

    ResumeLockGuard(const ResumeLockGuard&) = delete;

    ResumeLockGuard(ResumeLockGuard&& lg) noexcept
        : key_(std::move(lg.key_)), lockCore_(lg.lockCore_), locked_(lg.locked_) {}

    ResumeLockGuard& operator=(const ResumeLockGuard&) = delete;

    ResumeLockGuard& operator=(ResumeLockGuard&& lg) noexcept {
        if (this != &lg) {
            lockCore_ = lg.lockCore_;
            key_ = std::move(lg.key_);
            locked_ = lg.locked_;
        }
        return *this;
    }

    ~ResumeLockGuard() {
        if (locked_) {
            lockCore_->unlock(key_);
        }
    }

    bool isLocked() const noexcept {
        return locked_;
    }

    operator bool() const noexcept {
        return isLocked();
    }

protected:
    std::string key_;
    MemoryLockCore<std::string>* lockCore_;
    bool locked_{false};
};

}  // namespace storage
}  // namespace nebula
