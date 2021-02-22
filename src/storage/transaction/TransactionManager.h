/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_TRANSACTION_TRANSACTIONMGR_H_
#define STORAGE_TRANSACTION_TRANSACTIONMGR_H_

#include <folly/Function.h>
#include <folly/concurrency/ConcurrentHashMap.h>
#include <folly/Synchronized.h>
#include "common/clients/meta/MetaClient.h"
#include "common/clients/storage/GraphStorageClient.h"
#include "common/clients/storage/InternalStorageClient.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "common/meta/SchemaManager.h"
#include "common/thrift/ThriftTypes.h"
#include "kvstore/KVStore.h"
#include "storage/mutate/AddEdgesProcessor.h"
#include "storage/transaction/BaseChainProcessor.h"
#include "storage/transaction/TransactionUtils.h"
#include "storage/transaction/TransactionTypes.h"
#include "storage/transaction/MemoryLock.h"
#include "utils/NebulaKeyUtils.h"
#include "utils/MemoryLockCore.h"

namespace nebula {
namespace storage {

using KV = std::pair<std::string, std::string>;
using MemEdgeLocks = folly::ConcurrentHashMap<std::string, int64_t>;
using ResumedResult = std::shared_ptr<folly::Synchronized<KV>>;

class TransactionManager {
public:
    using GetBatchFunc = std::function<folly::Optional<std::string>(int64_t)>;

public:
    explicit TransactionManager(storage::StorageEnv* env);

    ~TransactionManager() = default;

    void processChain(BaseChainProcessor* processor);

    /**
     * @brief in-edge first, out-edge last, goes this way
     */
    folly::Future<cpp2::ErrorCode> updateEdge2(
        size_t vIdLen,
        GraphSpaceID spaceId,
        PartitionID partId,
        const cpp2::EdgeKey& edgeKey,
        std::vector<cpp2::UpdatedProp> updateProps,
        bool insertable,
        folly::Optional<std::vector<std::string>> returnProps,
        folly::Optional<std::string> condition,
        GetBatchFunc&& batchGetter);

    folly::Future<cpp2::ErrorCode> resumeLock(PlanContext* planCtx,
                                              std::shared_ptr<PendingLock>& lock);

    auto* getMemoryLock() {
        return &mLock_;
    }

    folly::SemiFuture<kvstore::ResultCode> commitBatch(GraphSpaceID spaceId,
                                                       PartitionID partId,
                                                       std::string& batch);

    bool enableToss(GraphSpaceID spaceId) {
        return nebula::meta::cpp2::IsolationLevel::TOSS == getSpaceIsolationLvel(spaceId);
    }

    folly::Executor* getExecutor() { return exec_.get(); }

    // used for perf trace, will remove finally
    std::unordered_map<std::string, std::list<int64_t>> timer_;

    GraphStorageClient* getStorageClient() {
        return sClient_.get();
    }

    InternalStorageClient* getInternalClient() {
        return interClient_.get();
    }

    std::string remoteEdgeKey(size_t vIdLen, GraphSpaceID spaceId, const std::string& lockKey);

    // bool lockBatch(const std::vector<std::string>& keys) {
    //     return mLock_.lockBatch(keys);
    // }

    // void unlockBatch(const std::vector<std::string>& keys) {
    //     mLock_.unlockBatch(keys);
    // }

protected:
    nebula::meta::cpp2::IsolationLevel getSpaceIsolationLvel(GraphSpaceID spaceId);

    // folly::SemiFuture<kvstore::ResultCode> commitEdgeOut(GraphSpaceID spaceId,
    //                                                      PartitionID partId,
    //                                                      std::string&& key,
    //                                                      std::string&& props);

    // folly::SemiFuture<kvstore::ResultCode> commitEdge(GraphSpaceID spaceId,
    //                                                   PartitionID partId,
    //                                                   std::string& key,
    //                                                   std::string& encodedProp);

    // folly::SemiFuture<cpp2::ErrorCode>
    // eraseKey(GraphSpaceID spaceId, PartitionID partId, const std::string& key);

    // void eraseMemoryLock(const std::string& rawKey, int64_t ver);

    // std::string encodeBatch(std::vector<KV>&& data);

protected:
    StorageEnv*                                         env_{nullptr};
    std::shared_ptr<folly::IOThreadPoolExecutor>        exec_;
    std::unique_ptr<GraphStorageClient>                 sClient_;
    std::unique_ptr<InternalStorageClient>              interClient_;
    // MemEdgeLocks                                        memLock_;
    // MemoryLock<std::string>                             mLock_;
    nebula::MemoryLockCore<std::string>                         mLock_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_TRANSACTION_TRANSACTIONMGR_H_
