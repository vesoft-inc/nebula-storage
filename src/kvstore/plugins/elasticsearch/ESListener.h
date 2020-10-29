/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef KVSTORE_PLUGINS_ES_LISTENER_H_
#define KVSTORE_PLUGINS_ES_LISTENER_H_

#include "kvstore/Listener.h"

namespace nebula {
namespace kvstore {

class ESListener : public Listener {
public:
    ESListener(GraphSpaceID spaceId,
              PartitionID partId,
              HostAddr localAddr,
              const std::string& walPath,
              std::shared_ptr<folly::IOThreadPoolExecutor> ioPool,
              std::shared_ptr<thread::GenericThreadPool> workers,
              std::shared_ptr<folly::Executor> handlers,
              std::shared_ptr<raftex::SnapshotManager> snapshotMan,
              std::shared_ptr<RaftClient> clientMan,
              meta::SchemaManager* schemaMan)
        : Listener(spaceId, partId, std::move(localAddr), walPath,
                   ioPool, workers, handlers, snapshotMan, clientMan, schemaMan) {
        setCallback(std::bind(&ESListener::commitLog, this,
                              std::placeholders::_1, std::placeholders::_2),
                    std::bind(&ESListener::updateCommit, this,
                              std::placeholders::_1, std::placeholders::_2));
    }

protected:
    bool commitLog(LogID, folly::StringPiece) {
        return true;
    }

    bool updateCommit(LogID, TermID) {
        return true;
    }

    std::pair<LogID, TermID> lastCommittedLogId() override {
        return {0, 0};
    }

    std::pair<int64_t, int64_t> commitSnapshot(const std::vector<std::string>&,
                                               LogID,
                                               TermID,
                                               bool) override {
        LOG(FATAL) << "Not implemented";
    }

    void cleanup() override {
    }
};

}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_PLUGINS_ES_LISTENER_H_
