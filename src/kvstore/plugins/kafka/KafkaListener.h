/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef KVSTORE_PLUGINS_KAFKA_LISTENER_H_
#define KVSTORE_PLUGINS_KAFKA_LISTENER_H_

#include "common/base/Base.h"
#include "kvstore/Listener.h"
#include "codec/RowReaderWrapper.h"
#include "common/kafka/KafkaClient.h"

namespace nebula {
namespace kvstore {

class KafkaListener : public Listener {
public:
    KafkaListener(GraphSpaceID spaceId,
                  PartitionID partId,
                  HostAddr localAddr,
                  const std::string& walPath,
                  std::shared_ptr<folly::IOThreadPoolExecutor> ioPool,
                  std::shared_ptr<thread::GenericThreadPool> workers,
                  std::shared_ptr<folly::Executor> handlers,
                  std::shared_ptr<raftex::SnapshotManager> snapshotMan,
                  std::shared_ptr<RaftClient> clientMan,
                  std::shared_ptr<DiskManager> diskMan,
                  meta::SchemaManager* schemaMan)
        : Listener(spaceId, partId, std::move(localAddr), walPath,
                   ioPool, workers, handlers, snapshotMan, clientMan, diskMan, schemaMan) {
            LOG(INFO) << "Kakfa Listener init";
            CHECK(!!schemaMan);
            lastApplyLogFile_ = std::make_unique<std::string>(
                folly::stringPrintf("%s/kafka_last_apply_log_%d", walPath.c_str(), partId));
    }

protected:
    bool init() override;

    bool apply(const std::vector<KV>& data) override;

private:
    bool appendMessage(PartitionID part, RowReader* reader) const;

    bool appendVertex(const KV& kv) const;

    bool appendEdge(const KV& kv) const;

private:
    std::string                                 topic_;
    std::unique_ptr<nebula::kafka::KafkaClient> client_;
};

}  // namespace kvstore
}  // namespace nebula

#endif  // KVSTORE_PLUGINS_KAFKA_LISTENER_H_
