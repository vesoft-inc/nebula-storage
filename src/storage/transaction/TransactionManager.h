/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

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
#include "utils/NebulaKeyUtils.h"
#include "utils/MemoryLockCore.h"

namespace nebula {
namespace storage {

class TransactionManager {
public:
    using UpdateEdgeGetter = std::function<folly::Optional<std::string>(int64_t)>;

public:
    explicit TransactionManager(storage::StorageEnv* env);

    ~TransactionManager() = default;

    void processChain(BaseChainProcessor* processor);

    folly::Future<cpp2::ErrorCode> resumeLock(PlanContext* planCtx,
                                              std::shared_ptr<PendingLock>& lock);

    /**
     * @brief in-edge first, out-edge last, goes this way
     */
    folly::Future<cpp2::ErrorCode> updateEdge(
        size_t vIdLen,
        GraphSpaceID spaceId,
        PartitionID partId,
        const cpp2::EdgeKey& edgeKey,
        std::vector<cpp2::UpdatedProp> updateProps,
        bool insertable,
        folly::Optional<std::vector<std::string>> returnProps,
        folly::Optional<std::string> condition,
        UpdateEdgeGetter&& getter);

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

protected:
    nebula::meta::cpp2::IsolationLevel getSpaceIsolationLvel(GraphSpaceID spaceId);

protected:
    StorageEnv*                                         env_{nullptr};
    std::shared_ptr<folly::IOThreadPoolExecutor>        exec_;
    std::unique_ptr<GraphStorageClient>                 sClient_;
    std::unique_ptr<InternalStorageClient>              interClient_;
    nebula::MemoryLockCore<std::string>                 mLock_;
};

}  // namespace storage
}  // namespace nebula
