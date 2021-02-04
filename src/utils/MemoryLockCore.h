/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include <folly/concurrency/ConcurrentHashMap.h>

namespace nebula {

template<typename Key>
class MemoryLockCore {
public:
    MemoryLockCore() = default;

    ~MemoryLockCore() = default;

    // I assume this may have better perf in high contention
    // but may not necessary sort any time.
    // this may be useful while first lock attempt failed,
    // and try to retry.
    bool try_lockSortedBatch(const std::vector<Key>& keys) {
        return lockBatch(keys);
    }

    bool try_lock(const Key& key) {
        return hashMap_.insert(std::make_pair(key, 0)).second;
    }

    void unlock(const Key& key) {
        hashMap_.erase(key);
    }

    bool lockBatch(const std::vector<Key>& keys) {
        bool inserted = false;
        for (auto i = 0U; i != keys.size(); ++i) {
            std::tie(std::ignore, inserted) = hashMap_.insert(std::make_pair(keys[i], 0));
            if (!inserted) {
                unlockBatchN(keys, i);
                return false;
            }
        }
        return true;
    }

    void unlockBatch(const std::vector<Key>& keys) {
        unlockBatchN(keys, keys.size());
    }

    void unlockBatchN(const std::vector<Key>& keys, size_t num) {
        for (auto i = 0U; i != num; ++i) {
            hashMap_.erase(keys[i]);
        }
    }

    void clear() {
        hashMap_.clear();
    }

protected:
    folly::ConcurrentHashMap<Key, int> hashMap_;
};

}  // namespace nebula
