/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/query/GetEdgetypeEdgesProcessor.h"
#include "utils/IndexKeyUtils.h"

namespace nebula {
namespace storage {

void GetEdgetypeEdgesProcessor::process(const cpp2::GetEdgetypeEdgesRequest& req) {
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
    edges_.colNames.emplace_back(kSrc);
    edges_.colNames.emplace_back(kRank);
    edges_.colNames.emplace_back(kDst);

    // Get all edges in one edgetype
    for (auto& partId : parts) {
        std::string val;
        auto prefix = StatisticsIndexKeyUtils::edgeIndexPrefix(partId, indexId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = env_->kvstore_->prefix(spaceId_, partId, prefix, &iter);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            LOG(ERROR) << "Get edge index Id:" << indexId << " error";
            handleErrorCode(ret, spaceId_, partId);
            onFinished();
            return;
        } else {
            while (iter && iter->valid()) {
                Row row;
                auto key = iter->key();
                auto srcId = StatisticsIndexKeyUtils::getIndexSrcId(spaceVidLen_, key);
                auto rank = StatisticsIndexKeyUtils::getIndexRank(spaceVidLen_, key);
                auto dst = StatisticsIndexKeyUtils::getIndexDstId(spaceVidLen_, key);
                row.emplace_back(std::move(srcId));
                row.emplace_back(std::move(rank));
                row.emplace_back(std::move(dst));
                edges_.rows.emplace_back(std::move(row));
                iter->next();
            }
        }
    }

    onProcessFinished();
    onFinished();
    return;
}

cpp2::ErrorCode
GetEdgetypeEdgesProcessor::checkAndBuildContexts(const cpp2::GetEdgetypeEdgesRequest& req) {
    auto indexId = req.get_index();
    auto iRet = env_->indexMan_->getEdgeIndex(spaceId_, indexId);
    if (!iRet.ok()) {
        LOG(ERROR) << "Get statistic edge index Id: " << indexId <<" not exist.";
        return cpp2::ErrorCode::E_INDEX_NOT_FOUND;
    }

    auto indexItem = iRet.value();

    // IndexType::EDGE       get all edges in one edgetype of one space
    auto indexType = indexItem->index_type;
    if (indexType !=  nebula::meta::cpp2::IndexType::EDGE) {
        LOG(ERROR) << "Index type illegal " << indexItem->index_name << " type "
                   << static_cast<int32_t>(indexType);
        return cpp2::ErrorCode::E_INDEX_NOT_FOUND;
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

void GetEdgetypeEdgesProcessor::onProcessFinished() {
    resp_.set_edges(std::move(edges_));
}

}  // namespace storage
}  // namespace nebula
