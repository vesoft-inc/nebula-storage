/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/mutate/AddVerticesProcessor.h"
#include "common/NebulaKeyUtils.h"
#include <algorithm>
#include <limits>
#include "time/WallClock.h"
#include "codec/RowWriterV2.h"
#include "storage/StorageFlags.h"

DECLARE_bool(enable_vertex_cache);

namespace nebula {
namespace storage {

void AddVerticesProcessor::process(const cpp2::AddVerticesRequest& req) {
    auto version =
        std::numeric_limits<int64_t>::max() - time::WallClock::fastNowInMicroSec();
    // Switch version to big-endian, make sure the key is in ordered.
    version = folly::Endian::big(version);

    spaceId_ = req.get_space_id();
    const auto& partVertices = req.get_parts();
    const auto& propNamesMap = req.get_prop_names();

    CHECK_NOTNULL(env_->schemaMan_);
    auto ret = env_->schemaMan_->getSpaceVidLen(spaceId_);
    if (!ret.ok()) {
        LOG(ERROR) << ret.status();
        cpp2::PartitionResult thriftRet;
        thriftRet.set_code(cpp2::ErrorCode::E_INVALID_SPACEVIDLEN);
        codes_.emplace_back(std::move(thriftRet));
        onFinished();
        return;
    }
    spaceVidLen_ = ret.value();
    callingNum_ = partVertices.size();

    CHECK_NOTNULL(env_->indexMan_);
    auto iRet = env_->indexMan_->getTagIndexes(spaceId_);
    if (iRet.ok()) {
        indexes_ = std::move(iRet).value();
    }

    CHECK_NOTNULL(env_->kvstore_);
    if (indexes_.empty()) {
        for (auto& part : partVertices) {
            auto partId = part.first;
            const auto& newVertices = part.second;

            std::vector<kvstore::KV> data;
            data.reserve(32);
            for (auto& newVertex : newVertices) {
                auto vid = newVertex.get_id();
                const auto& newTags = newVertex.get_tags();

                if (!NebulaKeyUtils::isValidVidLen(spaceVidLen_, vid)) {
                    LOG(ERROR) << "Space " << spaceId_ << ", vertex length invalid, "
                               << " space vid len: " << spaceVidLen_ << ",  vid is " << vid;
                    pushResultCode(cpp2::ErrorCode::E_Invalid_VID, partId);
                    onFinished();
                    return;
                }

                for (auto& newTag : newTags) {
                    auto tagId = newTag.get_tag_id();
                    VLOG(3) << "PartitionID: " << partId << ", VertexID: " << vid
                            << ", TagID: " << tagId << ", TagVersion: " << version;

                    auto key = NebulaKeyUtils::vertexKey(spaceVidLen_, partId, vid,
                                                         tagId, version);
                    auto schema = env_->schemaMan_->getTagSchema(spaceId_, tagId);
                    if (!schema) {
                        LOG(ERROR) << "Space " << spaceId_ << ", Tag " << tagId << " invalid";
                        pushResultCode(cpp2::ErrorCode::E_TAG_NOT_FOUND, partId);
                        onFinished();
                        return;
                    }

                    auto props = newTag.get_props();
                    auto iter = propNamesMap.find(tagId);
                    RowWriterV2 rowWrite(schema.get());
                    // If tagId is specified in req.prop_names, use the property name
                    // in req.prop_names, otherwise, use property name in schema
                    if (iter != propNamesMap.end()) {
                        auto colNames = iter->second;
                        for (size_t i = 0; i < colNames.size(); i++) {
                            auto wRet = rowWrite.setValue(colNames[i], props[i]);
                            if (wRet != WriteResult::SUCCEEDED) {
                                LOG(ERROR) << "Add vertex faild";
                                pushResultCode(cpp2::ErrorCode::E_DATA_TYPE_MISMATCH, partId);
                                onFinished();
                                return;
                            }
                        }
                    } else {
                        for (size_t i = 0; i < props.size(); i++) {
                            auto wRet = rowWrite.setValue(i, props[i]);
                            if (wRet != WriteResult::SUCCEEDED) {
                                LOG(ERROR) << "Add vertex faild";
                                pushResultCode(cpp2::ErrorCode::E_DATA_TYPE_MISMATCH, partId);
                                onFinished();
                                return;
                            }
                        }
                    }
                    auto wRet = rowWrite.finish();
                    if (wRet != WriteResult::SUCCEEDED) {
                        LOG(ERROR) << "Add vertex faild";
                        pushResultCode(cpp2::ErrorCode::E_DATA_TYPE_MISMATCH, partId);
                        onFinished();
                        return;
                    }

                    std::string encode = std::move(rowWrite).moveEncodedStr();
                    data.emplace_back(std::move(key), std::move(encode));

                    if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
                        vertexCache_->evict(std::make_pair(vid, tagId), partId);
                        VLOG(3) << "Evict cache for vId " << vid
                                << ", tagId " << tagId;
                    }
                }
            }
            doPut(spaceId_, partId, std::move(data));
        }
    } else {
        for (auto& part : partVertices) {
            auto partId = part.first;
            auto atomic = [version, partId, vertices = std::move(part.second), this]()
                          -> folly::Optional<std::string> {
                return this->addVertices(version, partId, vertices);
            };
            auto callback = [partId, this](kvstore::ResultCode code) {
                this->handleAsync(this->spaceId_, partId, code);
            };
            env_->kvstore_->asyncAtomicOp(spaceId_, partId, atomic, callback);
        }
    }
}

std::string AddVerticesProcessor::addVertices(int64_t version, PartitionID partId,
                                              const std::vector<cpp2::NewVertex>& vertices) {
    UNUSED(version);
    UNUSED(partId);
    UNUSED(vertices);
    return std::string("");
}

}  // namespace storage
}  // namespace nebula
