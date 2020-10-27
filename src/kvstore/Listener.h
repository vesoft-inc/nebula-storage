/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef KVSTORE_LISTENER_H_
#define KVSTORE_LISTENER_H_

#include "common/base/Base.h"
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
             std::shared_ptr<RaftClient> clientMan);

    void setCallback(std::function<bool(LogID, folly::StringPiece)> commitLogFunc,
                     std::function<bool(LogID, TermID)> updateCommitFunc);

    // Initialize listener, all Listener must call this method
    void start(std::vector<HostAddr>&& peers, bool) override;

    // Stop listener
    void stop() override;

    // Clean the wal and other data of listener
    void cleanup() override;

    int64_t logGap() {
        return wal_->lastLogId() - lastCommittedLogId().first;
    }

protected:
    // Last commit id and term, need to be persisted
    // virtual std::pair<LogID, TermID> lastCommittedLogId() override = 0;

    void onLostLeadership(TermID) override {
        LOG(FATAL) << "Should not reach here";
    }

    void onElected(TermID) override {
        LOG(FATAL) << "Should not reach here";
    }

    void onDiscoverNewLeader(HostAddr nLeader) override {
        LOG(INFO) << idStr_ << "Find the new leader " << nLeader;
    }

    // Commit the logs which are accepted by quorum, for example, sync to remote cluster
    // The empty log in iterator could be skipped, which is the raft heartbeat.
    // TODO(doodle): need to handle the case when updateCommit_
    bool commitLogs(std::unique_ptr<LogIterator> iter) override;

    // If some wal need to be pre-process, could do it here. For most of the listeners,
    // just return true is enough.
    bool preProcessLog(LogID, TermID, ClusterID, const std::string&) override {
        return true;
    }

    // If the listener falls behind way to much than leader, the leader will send all its data
    // in snapshot by batch, listener need to implement it if it need handle this case. The return
    // value is a pair of <logs count, logs size> of this batch.
    /*
    virtual std::pair<int64_t, int64_t> commitSnapshot(const std::vector<std::string>& data,
                                                       LogID committedLogId,
                                                       TermID committedLogTerm,
                                                       bool finished) override = 0;
    */

private:
    std::function<bool(LogID, folly::StringPiece)> commitLog_;
    std::function<bool(LogID, TermID)> updateCommit_;
};

}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_LISTENER_H_
