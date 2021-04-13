/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "utils/NebulaKeyUtils.h"
#include "codec/RowReaderWrapper.h"
#include "kvstore/plugins/elasticsearch/ESListener.h"
#include "common/plugin/fulltext/elasticsearch/ESStorageAdapter.h"

DEFINE_int32(ft_request_retry_times, 3, "Retry times if fulltext request failed");
DEFINE_int32(ft_bulk_batch_size, 100, "Max batch size when bulk insert");

namespace nebula {
namespace kvstore {

bool ESListener::init() {
    auto vRet = schemaMan_->getSpaceVidLen(spaceId_);
    if (!vRet.ok()) {
        LOG(ERROR) << "vid length error";
        return false;
    }

    vIdLen_ = vRet.value();
    auto cRet = schemaMan_->getServiceClients(nebula::meta::cpp2::ServiceType::ELASTICSEARCH);
    if (!cRet.ok() || cRet.value().empty()) {
        LOG(ERROR) << "Get elasticsearch clients error";
        return false;
    }
    for (const auto& c : cRet.value()) {
        nebula::plugin::HttpClient hc;
        hc.host = c.host;
        if (c.user_ref().has_value()) {
            hc.user = *c.user_ref();
            hc.password = *c.pwd_ref();
        }
        esClients_.emplace_back(std::move(hc));
    }
    return true;
}

bool ESListener::apply(const std::vector<KV>& data) {
    std::vector<nebula::plugin::DocItem> docItems;
    for (const auto& kv : data) {
        if (!nebula::NebulaKeyUtils::isVertex(vIdLen_, kv.first) &&
            !nebula::NebulaKeyUtils::isEdge(vIdLen_, kv.first)) {
            continue;
        }
        if (!appendDocItem(docItems, kv)) {
            return false;
        }
        if (docItems.size() >= static_cast<size_t>(FLAGS_ft_bulk_batch_size)) {
            auto suc = writeData(docItems);
            if (!suc) {
                return suc;
            }
            docItems.clear();
        }
    }
    if (!docItems.empty()) {
        return writeData(docItems);
    }
    return true;
}

bool ESListener::appendDocItem(std::vector<DocItem>& items, const KV& kv) const {
    auto isEdge = NebulaKeyUtils::isEdge(vIdLen_, kv.first);
    return isEdge ? appendEdgeDocItem(items, kv) : appendTagDocItem(items, kv);
}

bool ESListener::appendEdgeDocItem(std::vector<DocItem>& items, const KV& kv) const {
    auto edgeType = NebulaKeyUtils::getEdgeType(vIdLen_, kv.first);
    auto ftIndex = schemaMan_->getFTIndex(spaceId_, edgeType);
    if (!ftIndex.ok()) {
        VLOG(3) << "get text search index failed";
        return (ftIndex.status() == nebula::Status::IndexNotFound()) ? true : false;
    }
    auto reader = RowReaderWrapper::getEdgePropReader(schemaMan_,
                                                      spaceId_,
                                                      edgeType,
                                                      kv.second);
    if (reader == nullptr) {
        VLOG(3) << "get edge reader failed, schema ID " << edgeType;
        return false;
    }
    return appendDocs(items, reader.get(), std::move(ftIndex).value());
}

bool ESListener::appendTagDocItem(std::vector<DocItem>& items, const KV& kv) const {
    auto tagId = NebulaKeyUtils::getTagId(vIdLen_, kv.first);
    auto ftIndex = schemaMan_->getFTIndex(spaceId_, tagId);
    if (!ftIndex.ok()) {
        VLOG(3) << "get text search index failed";
        return ftIndex.status() == nebula::Status::IndexNotFound();
    }
    auto reader = RowReaderWrapper::getTagPropReader(schemaMan_,
                                                     spaceId_,
                                                     tagId,
                                                     kv.second);
    if (reader == nullptr) {
        VLOG(3) << "get tag reader failed, tagID " << tagId;
        return false;
    }
    return appendDocs(items, reader.get(), std::move(ftIndex).value());
}

bool ESListener::appendDocs(std::vector<DocItem>& items,
                            RowReader* reader,
                            const std::pair<std::string, nebula::meta::cpp2::FTIndex>& fti) const {
    for (const auto& field : fti.second.get_fields()) {
        auto v = reader->getValueByName(field);
        if (v.isStr()) {
            items.emplace_back(fti.first, field, partId_, std::move(v).getStr());
        }
    }
    return true;
}

bool ESListener::writeData(const std::vector<nebula::plugin::DocItem>& items) const {
    bool isNeedWriteOneByOne = false;
    auto retryCnt = FLAGS_ft_request_retry_times;
    while (--retryCnt > 0) {
        auto index = folly::Random::rand32(esClients_.size() - 1);
        auto suc = nebula::plugin::ESStorageAdapter::kAdapter->bulk(esClients_[index], items);
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
        return writeDatum(items);
    }
    LOG(ERROR) << "A fatal error . Full-text engine is not working.";
    return false;
}

bool ESListener::writeDatum(const std::vector<nebula::plugin::DocItem>& items) const {
    bool done = false;
    for (const auto& item : items) {
        done = false;
        auto retryCnt = FLAGS_ft_request_retry_times;
        while (--retryCnt > 0) {
            auto index = folly::Random::rand32(esClients_.size() - 1);
            auto suc = nebula::plugin::ESStorageAdapter::kAdapter->put(esClients_[index], item);
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

}  // namespace kvstore
}  // namespace nebula
