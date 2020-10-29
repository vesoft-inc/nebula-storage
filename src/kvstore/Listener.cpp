/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "kvstore/Listener.h"

namespace nebula {
namespace kvstore {

Listener::Listener(GraphSpaceID spaceId,
                   PartitionID partId,
                   HostAddr localAddr,
                   const std::string& walPath,
                   std::shared_ptr<folly::IOThreadPoolExecutor> ioPool,
                   std::shared_ptr<thread::GenericThreadPool> workers,
                   std::shared_ptr<folly::Executor> handlers,
                   std::shared_ptr<raftex::SnapshotManager> snapshotMan,
                   std::shared_ptr<RaftClient> clientMan)
    : RaftPart(FLAGS_cluster_id, spaceId, partId, localAddr, walPath,
               ioPool, workers, handlers, snapshotMan, clientMan) {
}

void Listener::setCallback(std::function<bool(LogID, folly::StringPiece)> commitLogFunc,
                           std::function<bool(LogID, TermID)> updateCommitFunc) {
    commitLog_ = std::move(commitLogFunc);
    updateCommit_ = std::move(updateCommitFunc);
}

void Listener::start(std::vector<HostAddr>&& peers, bool) {
    std::lock_guard<std::mutex> g(raftLock_);

    lastLogId_ = wal_->lastLogId();
    lastLogTerm_ = wal_->lastLogTerm();
    term_ = proposedTerm_ = lastLogTerm_;

    // Set the quorum number
    quorum_ = (peers.size() + 1) / 2;

    auto logIdAndTerm = lastCommittedLogId();
    committedLogId_ = logIdAndTerm.first;

    if (lastLogId_ < committedLogId_) {
        LOG(INFO) << idStr_ << "Reset lastLogId " << lastLogId_
                << " to be the committedLogId " << committedLogId_;
        lastLogId_ = committedLogId_;
        lastLogTerm_ = term_;
        wal_->reset();
    }

    LOG(INFO) << idStr_ << "Listener start"
                        << ", there are " << peers.size() << " peer hosts"
                        << ", lastLogId " << lastLogId_
                        << ", lastLogTerm " << lastLogTerm_
                        << ", committedLogId " << committedLogId_
                        << ", term " << term_;

    // As for listener, we don't need Host actually. However, listener need to be aware of
    // membership change, it can be handled in preProcessLog.
    for (auto& addr : peers) {
        auto hostPtr = std::make_shared<raftex::Host>(addr, shared_from_this());
        hosts_.emplace_back(hostPtr);
    }

    status_ = Status::RUNNING;
    role_ = Role::LEARNER;
}

void Listener::stop() {
    LOG(INFO) << "Stop listener [" << spaceId_ << ", " << partId_ << "] on " << addr_;
    {
        std::unique_lock<std::mutex> lck(raftLock_);
        status_ = Status::STOPPED;
        leader_ = {"", 0};
    }
}

bool Listener::commitLogs(std::unique_ptr<LogIterator> iter) {
    LogID lastId = -1, prevId = -1;
    TermID lastTerm = -1, prevTerm = -1;
    // todo(doodle): as for listener, we could commit in batch
    while (iter->valid()) {
        if (lastId != -1) {
            prevId = lastId;
            prevTerm = lastTerm;
        }
        lastId = iter->logId();
        lastTerm = iter->logTerm();

        auto log = iter->logMsg();
        if (log.empty()) {
            // skip the heartbeat
            ++(*iter);
            continue;
        }

        // try to decode the log and replicate to external source
        if (!commitLog_(iter->logId(), iter->logMsg())) {
            // If commit failed, will try it to commit later, update the commited
            // log id and term to (prevId, prevTerm)
            if (prevId != -1) {
                updateCommit_(prevId, prevTerm);
            }
            return true;
        }
        ++(*iter);
    }

    if (lastId >= 0) {
        updateCommit_(lastId, lastTerm);
    }
    return true;
}

}  // namespace kvstore
}  // namespace nebula
