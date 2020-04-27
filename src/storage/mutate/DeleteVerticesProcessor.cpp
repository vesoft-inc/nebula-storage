/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/mutate/DeleteVerticesProcessor.h"
#include "common/NebulaKeyUtils.h"
#include "storage/StorageFlags.h"

namespace nebula {
namespace storage {

void DeleteVerticesProcessor::process(const cpp2::DeleteVerticesRequest& req) {
    spaceId_ = req.get_space_id();
    const auto& partVertices = req.get_parts();

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
    callingNum_ = partVertices.size();

    CHECK_NOTNULL(env_->indexMan_);
    auto iRet = env_->indexMan_->getTagIndexes(spaceId_);
    if (iRet.ok()) {
        indexes_ = std::move(iRet).value();
    }

    CHECK_NOTNULL(env_->kvstore_);
    if (indexes_.empty()) {
        // Operate every part, the graph layer guarantees the unique of the vid
        std::vector<std::string> keys;
        keys.reserve(32);
        for (auto& part : partVertices) {
            auto partId = part.first;
            const auto& vertexIds = part.second;
            keys.clear();

            for (auto& vid : vertexIds) {
                auto prefix = NebulaKeyUtils::vertexPrefix(spaceVidLen, partId, vid);
                std::unique_ptr<kvstore::KVIterator> iter;
                auto retRes = env_->kvstore_->prefix(spaceId_, partId, prefix, &iter);
                if (retRes != kvstore::ResultCode::SUCCEEDED) {
                    VLOG(3) << "Error! ret = " << static_cast<int32_t>(retRes)
                            << ", spaceID " << spaceId_;
                    this->handleErrorCode(retRes, spaceId_, partId);
                    this->onFinished();
                    return;
                }
                while (iter->valid()) {
                    auto key = iter->key();
                    if (NebulaKeyUtils::isVertex(spaceVidLen, key)) {
                        auto tagId = NebulaKeyUtils::getTagId(spaceVidLen, key);
                        // Evict vertices from cache
                        if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
                            VLOG(3) << "Evict vertex cache for VID " << vid
                                    << ", TagID " << tagId;
                            vertexCache_->evict(std::make_pair(vid, tagId), partId);
                        }
                        keys.emplace_back(key.str());
                    }
                    iter->next();
                }
            }
            doRemove(spaceId_, partId, keys);
        }
    } else {
        for (auto& part : partVertices) {
            auto partId = part.first;
            auto atomic = [partId, vids = std::move(part.second), this]()
                          -> folly::Optional<std::string> {
                return this->deleteVertices(partId, vids);
            };

            auto callback = [partId, this](kvstore::ResultCode code) {
                VLOG(3) << "partId:" << partId << ", code:" << static_cast<int32_t>(code);
                this->handleAsync(this->spaceId_, partId, code);
            };
            env_->kvstore_->asyncAtomicOp(spaceId_, partId, atomic, callback);
        }
    }
}

folly::Optional<std::string>
DeleteVerticesProcessor::deleteVertices(PartitionID partId,
                                        const std::vector<VertexID>& vertices) {
    UNUSED(partId);
    UNUSED(vertices);
    return std::string("");
}

}  // namespace storage
}  // namespace nebula
