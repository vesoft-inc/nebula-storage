/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef KVSTORE_LISTENER_H_
#define KVSTORE_LISTENER_H_

#include "common/base/Base.h"
#include "common/meta/SchemaManager.h"
#include "kvstore/Common.h"
#include "kvstore/raftex/RaftPart.h"
#include "kvstore/raftex/Host.h"
#include "kvstore/wal/FileBasedWal.h"

DECLARE_int32(cluster_id);

namespace nebula {
namespace kvstore {

using RaftClient = thrift::ThriftClientManager<raftex::cpp2::RaftexServiceAsyncClient>;

class Listener : public raftex::RaftPart {
public:
    Listener(GraphSpaceID spaceId,
             PartitionID partId,
             HostAddr localAddr,
             const std::string& walPath,
             std::shared_ptr<folly::IOThreadPoolExecutor> ioPool,
             std::shared_ptr<thread::GenericThreadPool> workers,
             std::shared_ptr<folly::Executor> handlers,
             std::shared_ptr<raftex::SnapshotManager> snapshotMan,
             std::shared_ptr<RaftClient> clientMan,
             meta::SchemaManager* schemaMan);

    // Initialize listener, all Listener must call this method
    void start(std::vector<HostAddr>&& peers, bool asLearner = true) override;

    // Stop listener
    void stop() override;

    // Clean the wal and other data of listener
    virtual void cleanup() override = 0;

    int64_t logGap() {
        return wal_->lastLogId() - lastApplyLogId_;
    }

protected:
    // Last commit id and term, need to be persisted
    virtual std::pair<LogID, TermID> lastCommittedLogId() override = 0;

    // Last apply id, need to be persisted
    virtual LogID lastApplyLogId() = 0;

    virtual bool apply(const std::vector<KV>& data) = 0;

    virtual bool persist(LogID, TermID, LogID) = 0;

    void onLostLeadership(TermID) override {
        LOG(FATAL) << "Should not reach here";
    }

    void onElected(TermID) override {
        LOG(FATAL) << "Should not reach here";
    }

    void onDiscoverNewLeader(HostAddr nLeader) override {
        LOG(INFO) << idStr_ << "Find the new leader " << nLeader;
    }

    // For listener, we just return true directly. Another background thread trigger the actual
    // apply work, and do it in worker thread, and update lastApplyLogId_
    bool commitLogs(std::unique_ptr<LogIterator>) override {
        return true;
    }

    // For most of the listeners, just return true is enough. However, if listener need to be aware
    // of membership change, some log type of wal need to be pre-processed, could do it here.
    bool preProcessLog(LogID logId,
                       TermID termId,
                       ClusterID clusterId,
                       const std::string& log) override;

    // If the listener falls behind way to much than leader, the leader will send all its data
    // in snapshot by batch, listener need to implement it if it need handle this case. The return
    // value is a pair of <logs count, logs size> of this batch.
    virtual std::pair<int64_t, int64_t> commitSnapshot(const std::vector<std::string>& data,
                                                       LogID committedLogId,
                                                       TermID committedLogTerm,
                                                       bool finished) override = 0;

    void doApply();

protected:
    LogID lastApplyLogId_ = 0;
    meta::SchemaManager* schemaMan_{nullptr};
};

}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_LISTENER_H_
