/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/query/GetTagVerticesProcessor.h"
#include "utils/IndexKeyUtils.h"

namespace nebula {
namespace storage {

void GetTagVerticesProcessor::process(const cpp2::GetTagVerticesRequest& req) {
    spaceId_ = req.get_space_id();
    auto parts = req.get_parts();
    auto indexId = req.get_index();

    CHECK_NOTNULL(env_->schemaMan_);
    auto retSpaceVidLen = env_->schemaMan_->getSpaceVidLen(spaceId_);
    if (!retSpaceVidLen.ok()) {
        LOG(ERROR) << retSpaceVidLen.status();
        for (auto& part : parts) {
            pushResultCode(cpp2::ErrorCode::E_INVALID_SPACEVIDLEN, part);
        }
        onFinished();
        return;
    }
    spaceVidLen_ = retSpaceVidLen.value();

    CHECK_NOTNULL(env_->indexMan_);
    auto retCode = checkAndBuildContexts(req);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        for (auto& part : parts) {
            pushResultCode(retCode, part);
        }
        onFinished();
        return;
    }

    CHECK_NOTNULL(env_->kvstore_);
    // get all vertexId in one tag
    for (auto& partId : parts) {
        std::string val;
        auto prefix = StatisticsIndexKeyUtils::vertexIndexPrefix(partId, indexId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = env_->kvstore_->prefix(spaceId_, partId, prefix, &iter);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            LOG(ERROR) << "Get vertex index Id:" << indexId << " error";
            handleErrorCode(ret, spaceId_,  partId);
            onFinished();
            return;
        } else {
            while (iter && iter->valid()) {
                auto key = iter->key();
                auto vId = StatisticsIndexKeyUtils::getIndexVertexID(spaceVidLen_, key);
                vertices_.emplace_back(std::move(vId));
                iter->next();
            }
        }
    }

    onProcessFinished();
    onFinished();
    return;
}

cpp2::ErrorCode
GetTagVerticesProcessor::checkAndBuildContexts(const cpp2::GetTagVerticesRequest& req) {
    auto indexId = req.get_index();
    auto iRet = env_->indexMan_->getTagIndex(spaceId_, indexId);
    if (!iRet.ok()) {
        LOG(ERROR) << "Get statistic vertex index Id: " << indexId <<" not exist.";
        return cpp2::ErrorCode::E_INDEX_NOT_FOUND;
    }

    auto indexItem = iRet.value();

    // IndexType::VERTEX       get all vertex in one tag of one space
    auto indexType = indexItem->index_type;
    if (indexType !=  nebula::meta::cpp2::IndexType::VERTEX) {
        LOG(ERROR) << "Index type illegal " << indexItem->index_name << " type "
                   << static_cast<int32_t>(indexType);
        return cpp2::ErrorCode::E_INDEX_NOT_FOUND;
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

void GetTagVerticesProcessor::onProcessFinished() {
    resp_.set_vertices(std::move(vertices_));
}

}  // namespace storage
}  // namespace nebula
