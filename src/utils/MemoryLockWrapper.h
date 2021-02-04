/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "utils/MemoryLockCore.h"

namespace nebula {

// RAII style to easily control the lock acquire / release
template<class Key>
class MemoryLockGuard {
public:
    MemoryLockGuard(MemoryLockCore<Key>* lock, const Key& key)
        : MemoryLockGuard(lock, std::vector<Key>{key}) {}

    explicit MemoryLockGuard(MemoryLockCore<Key>* lock, const std::vector<Key>& key)
        : lock_(lock), keys_(key) {
        locked_ = lock->lockBatch(keys_);
    }

    ~MemoryLockGuard() {
        if (locked_) {
            lock_->unlockBatch(keys_);
        }
    }

    bool isLocked() const noexcept {
        return locked_;
    }

    operator bool() const noexcept {
        return isLocked();
    }

protected:
    MemoryLockCore<Key>* lock_;
    std::vector<Key> keys_;
    bool locked_{false};
};

}  // namespace nebula
