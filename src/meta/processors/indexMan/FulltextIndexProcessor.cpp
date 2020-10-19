/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/indexMan/FulltextIndexProcessor.h"

namespace nebula {
namespace meta {

void CreateFTIndexProcessor::process(const cpp2::CreateFTIndexReq& req) {
    auto space = req.get_space_id();
    folly::SharedMutex::WriteHolder wHolder(LockUtils::fulltextIndexLock());
    const auto& index = req.get_index();
    auto indexKey = MetaServiceUtils::fulltextIndexKey(space, index.get_index_type());
    auto ret = doGet(indexKey);
    if (ret.ok()) {
        handleErrorCode(cpp2::ErrorCode::E_EXISTED);
        onFinished();
        return;
    }

    std::vector<kvstore::KV> data;
    data.emplace_back(std::move(indexKey),
                      MetaServiceUtils::fulltextIndexVal(index));
    // TODO (sky) : create index into full text cluster.
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    doSyncPutAndUpdate(std::move(data));
}

void DropFTIndexProcessor::process(const cpp2::DropFTIndexReq& req) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::fulltextIndexLock());
    auto indexKey = MetaServiceUtils::fulltextIndexKey(req.get_space_id(),
                                                       req.get_type());
    auto ret = doGet(indexKey);
    if (!ret.ok()) {
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    // TODO (sky) : drop index from full text cluster.
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    doSyncMultiRemoveAndUpdate({std::move(indexKey)});
}

void ListFTIndicesProcessor::process(const cpp2::ListFTIndicesReq& req) {
    auto space = req.get_space_id();
    folly::SharedMutex::WriteHolder rHolder(LockUtils::fulltextIndexLock());
    auto prefix = MetaServiceUtils::fulltextIndexPrefix(space);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Can't find any full text indices.";
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    decltype(resp_.indices) indices;
    while (iter->valid()) {
        indices.emplace_back(MetaServiceUtils::parseFTindex(iter->val()));
        iter->next();
    }
    resp_.set_indices(std::move(indices));
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    onFinished();
}

}  // namespace meta
}  // namespace nebula
