/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "utils/NebulaKeyUtils.h"
#include "kvstore/plugins/elasticsearch/ESListener.h"
#include "common/plugin/fulltext/elasticsearch/ESStorageAdapter.h"

DECLARE_int32(ft_request_retry_times);
DECLARE_int32(ft_bulk_batch_size);

namespace nebula {
namespace kvstore {
bool ESListener::apply(const std::vector<KV>& data) {
    auto vIdLen = getVidLen();
    if (!vIdLen.ok()) {
        return false;
    }
    auto clients = schemaMan_->getFTClients();
    if (!clients.ok()) {
        return false;
    }
    std::vector<nebula::plugin::HttpClient> hClients;
    for (const auto& c : clients.value()) {
        nebula::plugin::HttpClient hc;
        hc.host = c.host;
        if (c.__isset.user) {
            hc.user = c.user;
            hc.password = c.pwd;
        }
        hClients.emplace_back(std::move(hc));
    }
    std::vector<nebula::plugin::DocItem> docItems;
    for (const auto& kv : data) {
        if (!nebula::NebulaKeyUtils::isDataKey(kv.first)) {
            continue;
        }
        if (!appendDocItem(docItems, kv, vIdLen.value())) {
            return false;
        }
        if (docItems.size() >= static_cast<size_t>(FLAGS_ft_bulk_batch_size)) {
            auto suc = writeData(docItems, hClients);
            if (!suc) {
                return suc;
            }
            docItems.clear();
        }
    }
    if (docItems.size() > 0) {
        return writeData(docItems, hClients);
    }
    return true;
}

bool ESListener::persist(LogID lastId, TermID lastTerm, LogID lastApplyLogId) {
    if (!writeAppliedId(lastId, lastTerm, lastApplyLogId)) {
        LOG(FATAL) << "last apply ids write failed";
    }
    return true;
}

std::pair<LogID, TermID> ESListener::lastCommittedLogId() {
    if (access(lastApplyLogFile_->c_str(), 0) != 0) {
        VLOG(3) << "Invalid or non-existent file : " << *lastApplyLogFile_;
        return {0, 0};
    }
    int32_t fd = open(lastApplyLogFile_->c_str(), O_RDONLY);
    if (fd < 0) {
        LOG(FATAL) << "Failed to open the file \"" << lastApplyLogFile_->c_str() << "\" ("
                   << errno << "): " << strerror(errno);
    }
    // read last logId from listener wal file.
    LogID logId;
    CHECK_EQ(pread(fd, reinterpret_cast<char*>(&logId), sizeof(LogID), 0),
             static_cast<ssize_t>(sizeof(LogID)));

    // read last termId from listener wal file.
    TermID termId;
    CHECK_EQ(pread(fd, reinterpret_cast<char*>(&termId), sizeof(TermID), sizeof(LogID)),
             static_cast<ssize_t>(sizeof(TermID)));
    close(fd);
    return {logId, termId};
}

LogID ESListener::lastApplyLogId() {
    if (access(lastApplyLogFile_->c_str(), 0) != 0) {
        VLOG(3) << "Invalid or non-existent file : " << *lastApplyLogFile_;
        return 0;
    }
    int32_t fd = open(lastApplyLogFile_->c_str(), O_RDONLY);
    if (fd < 0) {
        LOG(FATAL) << "Failed to open the file \"" << lastApplyLogFile_->c_str() << "\" ("
                   << errno << "): " << strerror(errno);
    }
    // read last applied logId from listener wal file.
    LogID logId;
    auto offset = sizeof(LogID) + sizeof(TermID);
    CHECK_EQ(pread(fd, reinterpret_cast<char*>(&logId), sizeof(LogID), offset),
             static_cast<ssize_t>(sizeof(LogID)));
    close(fd);
    return logId;
}

bool ESListener::writeAppliedId(LogID lastId, TermID lastTerm, LogID lastApplyLogId) {
    int32_t fd = open(
        lastApplyLogFile_->c_str(),
        O_CREAT | O_WRONLY | O_TRUNC | O_CLOEXEC,
        0644);
    if (fd < 0) {
        VLOG(3) << "Failed to open file \"" << lastApplyLogFile_->c_str()
                << "\" (errno: " << errno << "): "
                << strerror(errno);
        return false;
    }
    auto raw = encodeAppliedId(lastId, lastTerm, lastApplyLogId);
    ssize_t written = write(fd, raw.c_str(), raw.size());
    if (written != (ssize_t)raw.size()) {
        VLOG(3) << idStr_ << "bytesWritten:" << written << ", expected:" << raw.size()
                << ", error:" << strerror(errno);
        close(fd);
        return false;
    }
    close(fd);
    return true;
}

std::string
ESListener::encodeAppliedId(LogID lastId, TermID lastTerm, LogID lastApplyLogId) const noexcept {
    std::string val;
    val.reserve(sizeof(LogID) * 2 + sizeof(TermID));
    val.append(reinterpret_cast<const char*>(&lastId), sizeof(LogID))
       .append(reinterpret_cast<const char*>(&lastTerm), sizeof(TermID))
       .append(reinterpret_cast<const char*>(&lastApplyLogId), sizeof(LogID));
    return val;
}

bool ESListener::appendDocItem(std::vector<DocItem>& items, const KV& kv, int32_t vIdLen) const {
    auto isEdge = NebulaKeyUtils::isEdge(vIdLen, kv.first);
    return isEdge ? appendEdgeDocItem(items, kv, vIdLen) : appendTagDocItem(items, kv, vIdLen);
}

bool ESListener::appendEdgeDocItem(std::vector<DocItem>& items,
                                   const KV& kv,
                                   int32_t vIdLen) const {
    auto edgeType = NebulaKeyUtils::getEdgeType(vIdLen, kv.first);
    auto schema = schemaMan_->getEdgeSchema(spaceId_, edgeType);
    if (schema == nullptr) {
        VLOG(3) << "get edge schema failed, edgeType " << edgeType;
        return false;
    }
    auto reader = RowReaderWrapper::getEdgePropReader(schemaMan_,
                                                      spaceId_,
                                                      edgeType,
                                                      kv.second);
    if (reader == nullptr) {
        VLOG(3) << "get edge reader failed, schema ID " << edgeType;
        return false;
    }
    return appendDocs(items, schema.get(), reader.get(), edgeType, true);
}

bool ESListener::appendTagDocItem(std::vector<DocItem>& items,
                                  const KV& kv,
                                  int32_t vIdLen) const {
    auto tagId = NebulaKeyUtils::getTagId(vIdLen, kv.first);
    auto schema = schemaMan_->getTagSchema(spaceId_, tagId);
    if (schema == nullptr) {
        VLOG(3) << "get tag schema failed, tagId " << tagId;
        return false;
    }
    auto reader = RowReaderWrapper::getTagPropReader(schemaMan_,
                                                     spaceId_,
                                                     tagId,
                                                     kv.second);
    if (reader == nullptr) {
        VLOG(3) << "get tag reader failed, tagID " << tagId;
        return false;
    }
    return appendDocs(items, schema.get(), reader.get(), tagId, false);
}

bool ESListener::appendDocs(std::vector<DocItem>& items,
                            const meta::SchemaProviderIf* schema,
                            RowReader* reader,
                            int32_t schemaId,
                            bool isEdge) const {
    auto spaceName = getSpaceName();
    if (!spaceName.ok()) {
        return false;
    }
    auto count = schema->getNumFields();
    for (size_t i = 0; i < count; i++) {
        auto name = schema->getFieldName(i);
        auto v = reader->getValueByName(name);
        if (v.type() != Value::Type::STRING) {
            continue;
        }
        auto ftIndex = nebula::plugin::IndexTraits::indexName(spaceName.value(), isEdge);
        items.emplace_back(DocItem(std::move(ftIndex),
                                   std::move(name),
                                   partId_,
                                   schemaId,
                                   std::move(v).getStr()));
    }
    return true;
}

bool ESListener::writeData(const std::vector<nebula::plugin::DocItem>& items,
                           const std::vector<nebula::plugin::HttpClient>& clients) const {
    bool isNeedWriteOneByOne = false;
    auto retryCnt = FLAGS_ft_request_retry_times;
    while (--retryCnt > 0) {
        auto index = folly::Random::rand32(clients.size() - 1);
        auto suc = nebula::plugin::ESStorageAdapter::kAdapter->bulk(clients[index], items);
        if (!suc.ok()) {
            VLOG(3) << "bulk failed. retry : " << retryCnt;
            continue;
        }
        if (!suc.value()) {
            isNeedWriteOneByOne = true;
            break;
        }
        return true;
    }
    if (isNeedWriteOneByOne) {
        return writeDatum(items, clients);
    }
    LOG(ERROR) << "A fatal error . Full-text engine is not working.";
    return false;
}

bool ESListener::writeDatum(const std::vector<nebula::plugin::DocItem>& items,
                            const std::vector<nebula::plugin::HttpClient>& clients) const {
    bool done = false;
    for (const auto& item : items) {
        done = false;
        auto retryCnt = FLAGS_ft_request_retry_times;
        while (--retryCnt > 0) {
            auto index = folly::Random::rand32(clients.size() - 1);
            auto suc = nebula::plugin::ESStorageAdapter::kAdapter->put(clients[index], item);
            if (!suc.ok()) {
                VLOG(3) << "put failed. retry : " << retryCnt;
                continue;
            }
            if (!suc.value()) {
                // TODO (sky) : Record failed data
                break;
            }
            done = true;
            break;
        }
        if (!done) {
            // means CURL fails, and no need to take the next step
            LOG(ERROR) << "A fatal error . Full-text engine is not working.";
            return false;
        }
    }
    return true;
}

StatusOr<int32_t> ESListener::getVidLen() const {
    auto ret = schemaMan_->getSpaceVidLen(spaceId_);
    if (!ret.ok()) {
        VLOG(3) << "Failed to get space vid length";
        return ret.status();
    }
    return ret.value();
}

StatusOr<std::string> ESListener::getSpaceName() const {
    auto ret = schemaMan_->toGraphSpaceName(spaceId_);
    if (!ret.ok()) {
        VLOG(3) << "Failed to get space name";
        return ret.status();
    }
    return ret.value();
}

}  // namespace kvstore
}  // namespace nebula
