/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "utils/NebulaKeyUtils.h"
#include "kvstore/plugins/kafka/KafkaListener.h"

namespace nebula {
namespace kvstore {

bool KafkaListener::init() {
    auto vRet = schemaMan_->getSpaceVidLen(spaceId_);
    if (!vRet.ok()) {
        LOG(ERROR) << "vid length error";
        return false;
    }

    vIdLen_ = vRet.value();
    auto spaceRet = schemaMan_->toGraphSpaceName(spaceId_);
    if (!spaceRet.ok()) {
        LOG(ERROR) << "space not found";
        return false;
    }

    topic_ = spaceRet.value();
    auto clientRet = schemaMan_->getServiceClients(nebula::meta::cpp2::ServiceType::KAFKA);
    if (!clientRet.ok() || clientRet.value().empty()) {
        LOG(ERROR) << "Get kafka clients error";
        return false;
    }

    std::vector<std::string> clients;
    for (const auto& client : clientRet.value()) {
        clients.emplace_back(folly::stringPrintf("%s:%d",
                                                 client.get_host().host.c_str(),
                                                 client.get_host().port));
    }

    auto brokers = folly::join(",", std::move(clients));
    LOG(INFO) << "Brokers: " << brokers;
    client_ = std::make_unique<nebula::kafka::KafkaClient>(std::move(brokers));
    return true;
}

bool KafkaListener::apply(const std::vector<KV>& data) {
    for (const auto& kv : data) {
        if (nebula::NebulaKeyUtils::isVertex(vIdLen_, kv.first)) {
            if (!appendVertex(kv)) {
                LOG(ERROR) << "Append vertex failed";
                return false;
            }
        } else if (nebula::NebulaKeyUtils::isEdge(vIdLen_, kv.first)) {
            if (!appendEdge(kv)) {
                LOG(ERROR) << "Append edge failed";
                return false;
            }
        } else {
            VLOG(3) << "Not vertex or edge data. Skip";
        }
    }
    return true;
}

bool KafkaListener::appendVertex(const KV& kv) const {
    auto tagId = NebulaKeyUtils::getTagId(vIdLen_, kv.first);
    auto part = NebulaKeyUtils::getPart(kv.first);
    auto reader = RowReaderWrapper::getTagPropReader(schemaMan_,
                                                     spaceId_,
                                                     tagId,
                                                     kv.second);
    if (reader == nullptr) {
        VLOG(3) << "get tag reader failed, tagID " << tagId;
        return false;
    }
    return appendMessage(part, reader.get());
}

bool KafkaListener::appendEdge(const KV& kv) const {
    auto edgeType = NebulaKeyUtils::getEdgeType(vIdLen_, kv.first);
    auto part = NebulaKeyUtils::getPart(kv.first);
    auto reader = RowReaderWrapper::getEdgePropReader(schemaMan_,
                                                      spaceId_,
                                                      edgeType,
                                                      kv.second);
    if (reader == nullptr) {
        VLOG(3) << "get edge reader failed, schema ID " << edgeType;
        return false;
    }
    return appendMessage(part, reader.get());
}

bool KafkaListener::appendMessage(PartitionID part, RowReader* reader) const {
    int32_t size = reader->numFields();
    folly::dynamic map = folly::dynamic::object;
    auto schema = reader->getSchema();
    for (int32_t index = 0; index < size; index++) {
        auto key = schema->getFieldName(index);
        auto value = reader->getValueByIndex(index);
        folly::dynamic dynamicValue;
        if (value.isStr()) {
            dynamicValue = value.getStr();
        } else if (value.isInt()) {
            dynamicValue = value.getInt();
        } else if (value.isFloat()) {
            dynamicValue = value.getFloat();
        } else if (value.isBool()) {
            dynamicValue = value.getBool();
        }
        map[key] = dynamicValue;
    }

    auto value = folly::toJson(map);
    auto kafkaPort = part - 1;
    auto code = client_->produce(topic_, kafkaPort, "", std::move(value));
    return code == nebula::cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace kvstore
}  // namespace nebula
