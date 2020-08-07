/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_TRANSACTION_TRANSACTIONMGR_H_
#define STORAGE_TRANSACTION_TRANSACTIONMGR_H_

#include <folly/Function.h>
#include <folly/concurrency/ConcurrentHashMap.h>

#include "common/clients/meta/MetaClient.h"
#include "common/clients/storage/GraphStorageClient.h"
#include "common/clients/storage/InternalStorageClient.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "common/meta/SchemaManager.h"
#include "common/thrift/ThriftTypes.h"
#include "kvstore/Common.h"
#include "kvstore/KVStore.h"
#include "storage/transaction/TransactionUtils.h"
#include "utils/NebulaKeyUtils.h"

namespace nebula {
namespace storage {

using TransactionId = int64_t;
using StorageClient = storage::GraphStorageClient;
using TransactionMap = folly::ConcurrentHashMap<TransactionId, folly::Function<void()>>;
using LeaderKey = std::pair<GraphSpaceID, PartitionID>;
using LeaderCache = folly::ConcurrentHashMap<LeaderKey, HostAddr>;
using DataMutateLock = folly::ConcurrentHashMap<std::string, int64_t>;

class TransactionManager {
public:
    explicit TransactionManager(meta::MetaClient* mClient) : metaClient_(mClient) {
                            worker_ = std::make_shared<folly::IOThreadPoolExecutor>(4);
                            pool_ = std::make_unique<folly::IOThreadPoolExecutor>(4);
                            storageClient_ = std::make_unique<StorageClient>(
                                                               worker_,
                                                               metaClient_);
                            interClient_ = std::make_unique<storage::InternalStorageClient>(
                                                    worker_,
                                                    metaClient_);
                        }

    HostAddr getLeader(GraphSpaceID spaceId, PartitionID partId);

    folly::Future<cpp2::ErrorCode>
    prepareTransaction(cpp2::TransactionReq&& req);

    // folly::Future<cpp2::ErrorCode>
    // forwardTransaction(cpp2::TransactionReq&& req);

    folly::Future<cpp2::ErrorCode>
    commitTransaction(const cpp2::TransactionReq& req, int idx);

    folly::SemiFuture<cpp2::ErrorCode>
    commitEdge(GraphSpaceID spaceId,
               PartitionID partId,
               std::string&& key,
               std::string&& encodedProp);

    // folly::SemiFuture<cpp2::ErrorCode>
    // lockEdge(GraphSpaceID spaceId, PartitionID partId, const cpp2::EdgeKey& e);

    folly::SemiFuture<cpp2::ErrorCode>
    lockEdge(size_t vIdLen,
             GraphSpaceID spaceId,
             PartitionID partId,
             std::string&& edgeKey);

    template<typename T>
    folly::SemiFuture<cpp2::ErrorCode> resumeTransactionIfAny(size_t vIdLen,
                                                              GraphSpaceID spaceId,
                                                              PartitionID partId,
                                                              T&& edgeKey) {
        LOG(INFO) << "messi [enter] " << __func__;
        auto rawEdgeKey = NebulaKeyUtils::edgeKey(vIdLen,
                                                  partId,
                                                  edgeKey.src,
                                                  edgeKey.edge_type,
                                                  edgeKey.ranking,
                                                  edgeKey.dst,
                                                  0);
        // auto lockKey = TransactionUtils::edgeLockKey(vIdLen, spaceId, partId, edgeKey);
        // if (!hasDangleLock(vIdLen, spaceId, partId, rawEdgeKey)) {
        //     return folly::makeFuture(cpp2::ErrorCode::SUCCEEDED);
        // }
        auto strVer = hasDangleLock(vIdLen, spaceId, partId, rawEdgeKey);
        if (strVer.empty()) {
            return folly::makeFuture(cpp2::ErrorCode::SUCCEEDED);
        }

        rawEdgeKey.replace(rawEdgeKey.size() - sizeof(int64_t), sizeof(int64_t), strVer);
        // auto lockKey = NebulaKeyUtils::keyWithNoVersion(rawEdgeKey);

        nebula::List row = getInEdgeProp(spaceId, std::forward<T>(edgeKey));
        if (row[0].isNull()) {
            return clearDangleTransaction(vIdLen,
                                          spaceId,
                                          partId, std::forward<T>(edgeKey));
        } else {
            return resumeTransaction(spaceId,
                                     partId,
                                     std::forward<T>(edgeKey),
                                     std::move(row.values));
        }
    }

    cpp2::ErrorCode resumeTransactionIfAny(size_t vIdLen,
                                           GraphSpaceID spaceId,
                                           PartitionID partId,
                                           const VertexID& vid,
                                           EdgeType edgeType);

    // bool hasDangleLock(GraphSpaceID spaceId,
    //                    size_t vIdLen,
    //                    PartitionID partId,
    //                    const cpp2::EdgeKey& e);

    std::string hasDangleLock(size_t vIdLen,
                       GraphSpaceID spaceId,
                       PartitionID partId,
                       const std::string& rawKey);


    folly::SemiFuture<cpp2::ErrorCode>
    clearDangleTransaction(size_t vIdLen,
                           GraphSpaceID spaceId,
                           PartitionID partId,
                           const cpp2::EdgeKey& edgeKey);


    folly::SemiFuture<cpp2::ErrorCode>
    resumeTransaction(GraphSpaceID spaceId,
                      PartitionID partId,
                      const cpp2::EdgeKey& e,
                      std::vector<nebula::Value>&& props);

    nebula::List getInEdgeProp(GraphSpaceID spaceId, const cpp2::EdgeKey& e);

    // folly::SemiFuture<cpp2::ErrorCode>
    // unlockEdge(GraphSpaceID spaceId, PartitionID partId, const cpp2::EdgeKey& e);

    folly::SemiFuture<cpp2::ErrorCode>
    unlockEdge(GraphSpaceID spaceId, PartitionID partId, std::string&& rawKey);

    std::pair<PartitionID, HostAddr>
    getPartIdAndLeaderHost(GraphSpaceID spaceId, const VertexID& vid);

public:
    std::shared_ptr<folly::IOThreadPoolExecutor>        worker_;
    std::unique_ptr<folly::IOThreadPoolExecutor>        pool_;
    meta::MetaClient*                                   metaClient_;
    std::unique_ptr<storage::InternalStorageClient>     interClient_;
    std::unique_ptr<StorageClient>                      storageClient_;
    kvstore::KVStore*                                   kvstore_{nullptr};
    meta::SchemaManager*                                schemaMan_{nullptr};

private:
    TransactionMap                                      Transactions;
    std::mutex                                          muLeader_;
    LeaderCache                                         leaderCache_;
    DataMutateLock                                      inMemEdgeLock_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_TRANSACTION_TRANSACTIONMGR_H_
