/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "common/clients/meta/MetaClient.h"
#include "common/clients/storage/InternalStorageClient.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "common/meta/SchemaManager.h"
#include "common/thrift/ThriftTypes.h"
#include "kvstore/KVStore.h"
#include "storage/CommonUtils.h"
#include "utils/MemoryLockCore.h"
#include "utils/MemoryLockWrapper.h"
#include <folly/executors/Async.h>
#include <storage/transaction/ChainBaseProcessor.h>


namespace nebula {
namespace storage {
class TransactionManager {
public:
    FRIEND_TEST(ChainUpdateEdgeTest, updateTest1);
    friend class FakeInternalStorageClient;
    using LockGuard = MemoryLockGuard<std::string>;
    using LockCore = MemoryLockCore<std::string>;
    using UPtrLock = std::unique_ptr<LockCore>;

public:
    explicit TransactionManager(storage::StorageEnv* env);

    ~TransactionManager() { LOG(INFO) << "messi ~TransactionManager()"; }

    void addChainTask(ChainBaseProcessor* proc) {
        folly::async([=] {
            proc->prepareLocal()
                .via(exec_.get())
                .thenValue([=](auto&& code) { return proc->processRemote(code); })
                .thenValue([=](auto&& code) { return proc->processLocal(code); })
                .ensure([=]() { proc->finish(); });
        });
    }

    folly::Executor* getExecutor() { return exec_.get(); }

    LockCore* getLockCore(GraphSpaceID spaceId);

    InternalStorageClient* getInternalClient() {
        return iClient_;
    }

    std::unique_ptr<TransactionManager::LockGuard> tryLock(GraphSpaceID spaceId,
                                                           folly::StringPiece key);

    StatusOr<TermID> getTerm(GraphSpaceID spaceId, PartitionID partId);

    bool checkTerm(GraphSpaceID spaceId, PartitionID partId, TermID term);

    void start();

    void stop();

    // leave a record for prime edge, to let resume processor there is one dangling edge
    void markDanglingEdge(GraphSpaceID spaceId, const std::string& edge);

    // return false if there is no "edge" in danglingEdges_
    //        true if there is, and also erase the edge from danglingEdges_.
    bool takeDanglingEdge(GraphSpaceID spaceId, const std::string& edge);

protected:
    void resumeThread();

protected:
    using PartUUID = std::pair<GraphSpaceID, PartitionID>;
    using SEdgeKey = std::string;

    StorageEnv*                                         env_{nullptr};
    std::shared_ptr<folly::IOThreadPoolExecutor>        exec_;
    InternalStorageClient*                              iClient_;
    folly::ConcurrentHashMap<GraphSpaceID, UPtrLock>    memLocks_;
    folly::ConcurrentHashMap<PartUUID, TermID>          cachedTerms_;
    std::unique_ptr<thread::GenericWorker>              resumeThread_;

    /**
     * an update request may re-entered to an existing (double)prime key
     * and wants to have its own (double)prime.
     * also MVCC doesn't work.
     * because (double)prime can't judge if remote side succeeded.
     * to prevent insert/update re
     * */
    folly::ConcurrentHashMap<SEdgeKey, int>             danglingEdges_;
};

}  // namespace storage
}  // namespace nebula
