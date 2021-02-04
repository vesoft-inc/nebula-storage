/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "storage/transaction/MemoryLockCore.h"

namespace nebula {
namespace storage {

template<class Key>
class SimpleMemoryLock {
public:
    explicit SimpleMemoryLock(const Key& key) : SimpleMemoryLock(std::vector<Key>{key}) {}

    explicit SimpleMemoryLock(const std::vector<Key>& key) : keys_(key) {
        locked_ = GlobalMemoryLock<Key>::inst().lockBatch(keys_);
    }

    ~SimpleMemoryLock() {
        if (locked_) {
            GlobalMemoryLock<Key>::inst().unlockBatch(keys_);
        }
    }

    bool isLock() const noexcept {
        return locked_;
    }

    operator bool() const noexcept {
        return isLock();
    }

protected:
    bool locked_{false};
    std::vector<Key> keys_;
};

}  // namespace storage
}  // namespace nebula
