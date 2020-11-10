/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef KVSTORE_PLUGINS_ES_LISTENER_H_
#define KVSTORE_PLUGINS_ES_LISTENER_H_

#include "kvstore/Listener.h"
#include "common/plugin/fulltext/FTStorageAdapter.h"
#include "codec/RowReaderWrapper.h"

namespace nebula {
namespace kvstore {

using nebula::plugin::DocItem;

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
        CHECK(!!schemaMan);
        lastApplyLogFile_ = std::make_unique<std::string>(
            folly::stringPrintf("%s/last_apply_log_%d", walPath.c_str(), partId));
    }

protected:
    void init() override {
    }


    bool apply(const std::vector<KV>& data) override;

    bool persist(LogID lastId, TermID lastTerm, LogID lastApplyLogId) override;

    std::pair<LogID, TermID> lastCommittedLogId() override;

    LogID lastApplyLogId() override;

    void cleanup() override {
    }

private:
    bool writeAppliedId(LogID lastId, TermID lastTerm, LogID lastApplyLogId);

    bool readAppliedId(std::string& raw) const;

    std::string encodeAppliedId(LogID lastId, TermID lastTerm, LogID lastApplyLogId) const noexcept;

    bool appendDocItem(std::vector<DocItem>& items, const KV& kv, int32_t vIdLen) const;

    bool appendEdgeDocItem(std::vector<DocItem>& items, const KV& kv, int32_t vIdLen) const;

    bool appendTagDocItem(std::vector<DocItem>& items, const KV& kv, int32_t vIdLen) const;

    bool appendDocs(std::vector<DocItem>& items,
                    const meta::SchemaProviderIf* schema,
                    RowReader* reader,
                    int32_t schemaId,
                    bool isEdge) const;

    bool writeData(const std::vector<nebula::plugin::DocItem>& items,
                   const std::vector<nebula::plugin::HttpClient>& clients) const;

    bool writeDatum(const std::vector<nebula::plugin::DocItem>& items,
                    const std::vector<nebula::plugin::HttpClient>& clients) const;

    StatusOr<int32_t> getVidLen() const;

    StatusOr<std::string> getSpaceName() const;

private:
    std::unique_ptr<std::string> lastApplyLogFile_{nullptr};
};

}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_PLUGINS_ES_LISTENER_H_
