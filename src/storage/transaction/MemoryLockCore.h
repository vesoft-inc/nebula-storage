/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include <folly/concurrency/ConcurrentHashMap.h>

#include "storage/CommonUtils.h"
// #include "common/interface/gen-cpp2/storage_types.h"

namespace nebula {
namespace storage {

template<typename Key>
class MemoryLockCore {
public:
    MemoryLockCore() = default;

    ~MemoryLockCore() = default;

    // TODO(liuyu): this may have better perf in high contention
    // but may not necessary sort any time.
    bool try_lockSortedBatch() {
        return false;
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


template<typename Key>
class GlobalMemoryLock {
public:
    static MemoryLockCore<Key>& inst() {
        static MemoryLockCore<Key> memLock;
        return memLock;
    }

private:
    GlobalMemoryLock() = default;
};

}  // namespace storage
}  // namespace nebula
