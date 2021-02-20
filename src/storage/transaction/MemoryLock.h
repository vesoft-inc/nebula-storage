/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include <folly/concurrency/ConcurrentHashMap.h>

#include "storage/CommonUtils.h"
#include "common/interface/gen-cpp2/storage_types.h"

namespace nebula {
namespace storage {

template<typename TKey>
class MemoryLock {
public:
    MemoryLock() = default;

    ~MemoryLock() = default;

    // TODO(liuyu): this may have better perf in high contention
    // but may not necessary sort any time.
    // bool lockSortedBatch();

    bool lock(const TKey& key) {
        return lockBatch(std::vector<std::string>{key});
    }

    bool lockBatch(const std::vector<TKey>& keys) {
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

    void unlockBatch(const std::vector<TKey>& keys) {
        unlockBatchN(keys, keys.size());
    }

    void unlockBatchN(const std::vector<TKey>& keys, size_t num) {
        for (auto i = 0U; i != num; ++i) {
            hashMap_.erase(keys[i]);
        }
    }

protected:
    folly::ConcurrentHashMap<TKey, int> hashMap_;
};

}  // namespace storage
}  // namespace nebula
