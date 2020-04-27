/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "storage/mutate/AddEdgesProcessor.h"
#include "common/NebulaKeyUtils.h"
#include <algorithm>
#include <limits>
#include "time/WallClock.h"
#include "codec/RowWriterV2.h"

namespace nebula {
namespace storage {

void AddEdgesProcessor::process(const cpp2::AddEdgesRequest& req) {
    auto version =
        std::numeric_limits<int64_t>::max() - time::WallClock::fastNowInMicroSec();
    // Switch version to big-endian, make sure the key is in ordered.
    version = folly::Endian::big(version);

    spaceId_ = req.get_space_id();
    const auto& partEdges = req.get_parts();
    const auto& propNames = req.get_prop_names();

    CHECK_NOTNULL(env_->schemaMan_);
    auto ret = env_->schemaMan_->getSpaceVidLen(spaceId_);
    if (!ret.ok()) {
        LOG(ERROR) << "Space " << spaceId_ << " VertexId length invalid."
                   << ret.status().toString();
        cpp2::PartitionResult thriftRet;
        thriftRet.set_code(cpp2::ErrorCode::E_INVALID_SPACEVIDLEN);
        codes_.emplace_back(std::move(thriftRet));
        onFinished();
        return;
    }
    auto spaceVidLen = ret.value();
    callingNum_ = req.parts.size();

    CHECK_NOTNULL(env_->indexMan_);
    auto iRet = env_->indexMan_->getEdgeIndexes(spaceId_);
    if (iRet.ok()) {
        indexes_ = std::move(iRet).value();
    }

    CHECK_NOTNULL(env_->kvstore_);
    if (indexes_.empty()) {
        for (auto& part : partEdges) {
            auto partId = part.first;
            const auto& newEdges = part.second;

            std::vector<kvstore::KV> data;
            data.reserve(32);
            for (auto& newEdge : newEdges) {
                auto edgeKey = newEdge.key;
                VLOG(3) << "PartitionID: " << partId << ", VertexID: " << edgeKey.src
                        << ", EdgeType: " << edgeKey.edge_type << ", EdgeRanking: "
                        << edgeKey.ranking << ", VertexID: "
                        << edgeKey.dst << ", EdgeVersion: " << version;

                auto key = NebulaKeyUtils::edgeKey(spaceVidLen, partId, edgeKey.src,
                                                   edgeKey.edge_type, edgeKey.ranking,
                                                   edgeKey.dst, version);
                auto schema = env_->schemaMan_->getEdgeSchema(spaceId_, edgeKey.edge_type);
                if (!schema) {
                    LOG(ERROR) << "Space " << spaceId_ << ", Edge "
                               << edgeKey.edge_type << " invalid";
                    pushResultCode(cpp2::ErrorCode::E_EDGE_NOT_FOUND, partId);
                    onFinished();
                    return;
                }

                auto props = newEdge.get_props();
                RowWriterV2 rowWrite(schema.get());
                // If req.prop_names is not empty, use the property name in req.prop_names
                // Otherwise, use property name in schema
                if (!propNames.empty()) {
                    for (size_t i = 0; i < propNames.size(); i++) {
                        auto wRet = rowWrite.setValue(propNames[i], props[i]);
                        if (wRet != WriteResult::SUCCEEDED) {
                            LOG(ERROR) << "Add edge faild";
                            pushResultCode(cpp2::ErrorCode::E_DATA_TYPE_MISMATCH, partId);
                            onFinished();
                            return;
                        }
                    }
                } else {
                    for (size_t i = 0; i < props.size(); i++) {
                        auto wRet = rowWrite.setValue(i, props[i]);
                        if (wRet != WriteResult::SUCCEEDED) {
                            LOG(ERROR) << "Add edge faild";
                            pushResultCode(cpp2::ErrorCode::E_DATA_TYPE_MISMATCH, partId);
                            onFinished();
                            return;
                        }
                    }
                }

                auto wRet = rowWrite.finish();
                if (wRet != WriteResult::SUCCEEDED) {
                    LOG(ERROR) << "Add edge faild";
                    pushResultCode(cpp2::ErrorCode::E_DATA_TYPE_MISMATCH, partId);
                    onFinished();
                    return;
                }

                std::string encode = std::move(rowWrite).moveEncodedStr();
                data.emplace_back(std::move(key), std::move(encode));
            }
            doPut(spaceId_, partId, std::move(data));
        }
    } else {
        for (auto& part : partEdges) {
            auto partId = part.first;
            auto atomic = [version, partId, edges = std::move(part.second), this]()
                          -> folly::Optional<std::string> {
                return this->addEdges(version, partId, edges);
            };
            auto callback = [partId, this](kvstore::ResultCode code) {
                this->handleAsync(this->spaceId_, partId, code);
            };
            env_->kvstore_->asyncAtomicOp(spaceId_, partId, atomic, callback);
        }
    }
}

std::string AddEdgesProcessor::addEdges(int64_t version, PartitionID partId,
                                        const std::vector<cpp2::NewEdge>& edges) {
    UNUSED(version);
    UNUSED(partId);
    UNUSED(edges);
    return std::string("");
}

}  // namespace storage
}  // namespace nebula
