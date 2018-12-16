/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef RAFTEX_RAFTPART_H_
#define RAFTEX_RAFTPART_H_

#include "base/Base.h"
#include <folly/futures/SharedPromise.h>
#include "gen-cpp2/raftex_types.h"
#include "time/Duration.h"
#include "thread/GenericThreadPool.h"
#include "raftex/LogIterator.h"

namespace folly {
class IOThreadPoolExecutor;
class EventBase;
}  // namespace folly;

namespace nebula {
namespace raftex {

class FileBasedWal;
class BufferFlusher;
class Host;

class RaftPart : public std::enable_shared_from_this<RaftPart> {
public:
    enum class AppendLogResult {
        SUCCEEDED = 0,
        E_NOT_A_LEADER = -1,
        E_STOPPED = -2,
        E_NOT_READY = -3,
        E_BUFFER_OVERFLOW = -4,
        E_WAL_FAILURE = -5,
    };


    virtual ~RaftPart();

    bool isRunning() const {
        std::lock_guard<std::mutex> g(raftLock_);
        return status_ == Status::RUNNING;
    }

    bool isStopped() const {
        std::lock_guard<std::mutex> g(raftLock_);
        return status_ == Status::STOPPED;
    }

    bool isLeader() const {
        std::lock_guard<std::mutex> g(raftLock_);
        return role_ == Role::LEADER;
    }

    bool isFollower() const {
        std::lock_guard<std::mutex> g(raftLock_);
        return role_ == Role::FOLLOWER;
    }

    ClusterID clusterId() const {
        return clusterId_;
    }

    GraphSpaceID spaceId() const {
        return spaceId_;
    }

    PartitionID partitionId() const {
        return partId_;
    }

    const HostAddr& address() const {
        return addr_;
    }

    HostAddr leader() const {
        std::lock_guard<std::mutex> g(raftLock_);
        return leader_;
    }

    std::shared_ptr<FileBasedWal> wal() const {
        return wal_;
    }

    // Change the partition status to RUNNING. This is called
    // by the inherited class, when it's ready to server
    virtual void start(std::vector<HostAddr>&& peers);

    // Change the partition status to STOPPED. This is called
    // by the inherited class, when it's about to stop
    virtual void stop();


    /*****************************************************************
     * Asynchronously append a log
     *
     * This is the **PUBLIC** Log Append API, used by storage
     * service
     *
     * The method will take the ownership of the log and returns
     * as soon as possible. Internally it will asynchronously try
     * to send the log to all followers. It will keep trying until
     * majority of followers accept the log, then the future will
     * be fulfilled
     *
     * If the source == -1, the current clusterId will be used
     ****************************************************************/
    folly::Future<AppendLogResult> appendLogsAsync(
        ClusterID source,
        std::vector<std::string>&& logMsgs);

    /*****************************************************
     *
     * Methods to process incoming raft requests
     *
     ****************************************************/
    // Process the incoming leader election request
    void processAskForVoteRequest(
        const cpp2::AskForVoteRequest& req,
        cpp2::AskForVoteResponse& resp);

    // Process appendLog request
    void processAppendLogRequest(
        const cpp2::AppendLogRequest& req,
        cpp2::AppendLogResponse& resp);


protected:
    // Private constructor to prevent from instantiating directly
    RaftPart(ClusterID clusterId,
             GraphSpaceID spaceId,
             PartitionID partId,
             HostAddr localAddr,
             const folly::StringPiece walRoot,
             BufferFlusher* flusher,
             std::shared_ptr<folly::IOThreadPoolExecutor> pool,
             std::shared_ptr<thread::GenericThreadPool> workers);

    const char* idStr() const {
        return idStr_.c_str();
    }

    // The method will be invoked by start()
    //
    // Inherited classes can implement this method to provide the last
    // committed log id
    virtual LogID lastCommittedLogId() {
        return 0;
    }

    // This method is called when this partition's leader term
    // is finished, either by receiving a new leader election
    // request, or a new leader heartbeat
    virtual void onLostLeadership(TermID term) = 0;

    // This method is called when this partition is elected as
    // a new leader
    virtual void onElected(TermID term) = 0;

    // The inherited classes need to implement this method to commit
    // a batch of log messages
    virtual bool commitLogs(std::unique_ptr<LogIterator> iter) = 0;


private:
    enum class Status {
        STARTING = 0,   // The part is starting, not ready for service
        RUNNING,        // The part is running
        STOPPED         // The part has been stopped
    };

    enum class Role {
        LEADER = 1,     // the leader
        FOLLOWER,       // following a leader
        CANDIDATE       // Has sent AskForVote request
    };

    // A list of <idx, resp>
    // idx  -- the index of the peer
    // resp -- AskForVoteResponse
    using ElectionResponses = std::vector<cpp2::AskForVoteResponse>;
    // A list of <idx, resp>
    // idx  -- the index of the peer
    // resp -- AppendLogResponse
    using AppendLogResponses = std::vector<cpp2::AppendLogResponse>;


    /****************************************************
     *
     * Private methods
     *
     ***************************************************/
    const char* roleStr(Role role) const;

    cpp2::ErrorCode verifyLeader(const cpp2::AppendLogRequest& req,
                                 std::lock_guard<std::mutex>& lck);

    /*****************************************************************
     * Asynchronously send a heartbeat (An empty log entry)
     *
     * The code path is similar to appendLog() and the heartbeat will
     * be put into the log batch, but will not be added to WAL
     ****************************************************************/
    folly::Future<AppendLogResult> sendHeartbeat();

    /****************************************************
     *
     * Methods used by the status polling logic
     *
     ***************************************************/
    bool needToSendHeartbeat();

    bool needToStartElection();

    void statusPolling();

    // The method sends out AskForVote request
    // It return true if a leader is elected, otherwise returns false
    bool leaderElection();

    // The methed will fill up the request object and return TRUE
    // if the election should continue. Otherwise the method will
    // return FALSE
    bool prepareElectionRequest(cpp2::AskForVoteRequest& req);

    // The method returns the partition's role after the election
    Role processElectionResponses(const ElectionResponses& results);

    // Check whether new logs can be appended
    // Pre-condition: The caller needs to hold the raftLock_
    AppendLogResult canAppendLogs(std::lock_guard<std::mutex>& lck);

    void appendLogsInternal(
        std::vector<std::tuple<ClusterID, TermID, std::string>>&& logs);

    folly::Future<AppendLogResponses> replicateLogs(
        folly::EventBase* eb,
        TermID currTerm,
        LogID lastLogId,
        LogID committedId,
        TermID prevLogTerm,
        LogID prevLogId);

    void processAppendLogResponses(
        const AppendLogResponses& resps,
        folly::EventBase* eb,
        TermID currTerm,
        LogID lastLogId,
        LogID committedId,
        TermID prevLogTerm,
        LogID prevLogId);


private:
    const std::string idStr_;

    const ClusterID clusterId_;
    const GraphSpaceID spaceId_;
    const PartitionID partId_;
    const HostAddr addr_;
    std::shared_ptr<std::unordered_map<HostAddr, std::shared_ptr<Host>>>
        peerHosts_;
    size_t quorum_{0};

    // Partition level lock to synchronize the access of the partition
    mutable std::mutex raftLock_;

    bool replicatingLogs_{false};
    folly::SharedPromise<AppendLogResult> cachingPromise_;
    folly::SharedPromise<AppendLogResult> sendingPromise_;
    std::vector<std::tuple<ClusterID, TermID, std::string>> logs_;

    Status status_;
    Role role_;

    // When the partition is the leader, the leader_ is same as addr_
    HostAddr leader_;

    // The current term id
    //
    // When the partition voted for someone, termId will be set to
    // the term id proposed by that candidate
    TermID term_{0};
    // During normal operation, proposedTerm_ is equal to term_,
    // when the partition becomes a candidate, proposedTerm_ will be
    // bumped up by 1 every time when sending out the AskForVote
    // Request
    TermID proposedTerm_{0};

    // The id and term of the last-sent log
    LogID lastLogId_{0};
    TermID lastLogTerm_{0};
    // The id for the last globally committed log (from the leader)
    LogID committedLogId_{0};

    // To record how long ago when the last leader message received
    time::Duration lastMsgRecvDur_;
    // To record how long ago when the last log message or heartbeat
    // was sent
    time::Duration lastMsgSentDur_;

    // Write-ahead Log
    std::shared_ptr<FileBasedWal> wal_;

    // IO Thread pool
    std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool_;
    // Shared worker thread pool
    std::shared_ptr<thread::GenericThreadPool> workers_;
};

}  // namespace raftex
}  // namespace nebula
#endif  // RAFTEX_RAFTPART_H_
