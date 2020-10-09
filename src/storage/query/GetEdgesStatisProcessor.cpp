/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/query/GetEdgesStatisProcessor.h"
#include "utils/IndexKeyUtils.h"

namespace nebula {
namespace storage {

void GetEdgesStatisProcessor::process(const cpp2::GetEdgesStatisRequest& req) {
    spaceId_ = req.get_space_id();
    auto parts = req.get_parts();
    auto indexId = req.get_index();

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
    // get all edge count in space spaceId
    if (indexType_ == nebula::meta::cpp2::IndexType::EDGE_COUNT) {
        for (auto& partId : parts) {
            std::string val;
            auto eCountIndexKey = StatisticsIndexKeyUtils::countIndexKey(partId, indexId);
            auto ret = env_->kvstore_->get(spaceId_, partId, eCountIndexKey, &val);
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                if (ret != kvstore::ResultCode::ERR_KEY_NOT_FOUND) {
                    LOG(ERROR) << "Get edge count index Id:" << indexId << " error";
                    pushResultCode(to(ret), partId);
                    onFinished();
                    return;
                }
                // When the key does not exist, return 0
            } else {
                retCount_ += *reinterpret_cast<const int64_t*>(val.c_str());
            }
        }
    } else {
        // get all edge count in one edgetype
        for (auto& partId : parts) {
            std::string val;
            auto prefix = StatisticsIndexKeyUtils::edgeIndexPrefix(partId, indexId);
            std::unique_ptr<kvstore::KVIterator> iter;
            auto ret = env_->kvstore_->prefix(spaceId_, partId, prefix, &iter);
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                if (ret != kvstore::ResultCode::ERR_KEY_NOT_FOUND) {
                    LOG(ERROR) << "Get edge index Id:" << indexId << " error";
                    pushResultCode(to(ret), partId);
                    onFinished();
                    return;
                }
            } else {
                while (iter && iter->valid()) {
                    retCount_++;
                    iter->next();
                }
            }
        }
    }

    onProcessFinished();
    onFinished();
    return;
}

cpp2::ErrorCode
GetEdgesStatisProcessor::checkAndBuildContexts(const cpp2::GetEdgesStatisRequest& req) {
    auto indexId = req.get_index();
    auto iRet = env_->indexMan_->getEdgeIndex(spaceId_, indexId);
    if (!iRet.ok()) {
        LOG(ERROR) << "Get statistic edge count index Id: " << indexId <<" not exist.";
        return cpp2::ErrorCode::E_INDEX_NOT_FOUND;
    }

    auto indexItem = iRet.value();

    // IndexType::EDGE_COUNT get all edge count in space spaceId
    // IndexType::EDGE       get all edge count in one tag of one space
    indexType_ = indexItem->index_type;
    if (indexType_ != nebula::meta::cpp2::IndexType::EDGE_COUNT &&
        indexType_ !=  nebula::meta::cpp2::IndexType::EDGE) {
        LOG(ERROR) << "Index type illegal " << indexItem->index_name << " type "
                   << static_cast<int32_t>(indexType_);
        return cpp2::ErrorCode::E_INDEX_NOT_FOUND;
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

void GetEdgesStatisProcessor::onProcessFinished() {
    resp_.set_count(retCount_);
}

}  // namespace storage
}  // namespace nebula
