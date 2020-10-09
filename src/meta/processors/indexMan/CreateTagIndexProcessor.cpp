/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/indexMan/CreateTagIndexProcessor.h"

namespace nebula {
namespace meta {

cpp2::ErrorCode CreateTagIndexProcessor::checkFields() {
    std::unordered_set<std::string> columnSet(fieldNames_.begin(), fieldNames_.end());
    if (fieldNames_.size() != columnSet.size()) {
        LOG(ERROR) << "Conflict field in the tag index.";
        return cpp2::ErrorCode::E_CONFLICT;
    }

    switch (indexType_) {
        case cpp2::IndexType::NORMAL: {
            if (fieldNames_.empty()) {
                LOG(ERROR) << "The normal index field of a tag should not be empty.";
                return cpp2::ErrorCode::E_INVALID_PARM;
            }
            // A maximum of 16 columns are allowed in the normal index.
            if (fieldNames_.size() > 16) {
                LOG(ERROR) << "The number of normal index columns exceeds maximum limit 16";
                return cpp2::ErrorCode::E_CONFLICT;
            }
            break;
        }
        case cpp2::IndexType::VERTEX: {
            // No columns are allowed.
            if (fieldNames_.size() > 0) {
                LOG(ERROR) << "No columns are allowed in the vertex index";
                return cpp2::ErrorCode::E_CONFLICT;
            }
            break;
        }
        case cpp2::IndexType::VERTEX_COUNT: {
            // No columns are allowed.
            if (fieldNames_.size() > 0) {
                LOG(ERROR) << "No columns are allowed in the count statistics index";
                return cpp2::ErrorCode::E_CONFLICT;
            }
            break;
        }
        default:
            return cpp2::ErrorCode::E_UNSUPPORTED;
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

cpp2::ErrorCode CreateTagIndexProcessor::checkAndBuildIndex() {
    if (indexType_ == cpp2::IndexType::NORMAL || indexType_ == cpp2::IndexType::VERTEX) {
        auto tagIDRet = getTagId(spaceId_, tagName_);
        if (!tagIDRet.ok()) {
            LOG(ERROR) << "Create Tag Index Failed: Tag " << tagName_ << " not exist";
            return cpp2::ErrorCode::E_NOT_FOUND;
        }
        tagId_ = tagIDRet.value();
    } else if (indexType_ == cpp2::IndexType::VERTEX_COUNT) {
        // Statistics the number of all vertices, tagName is *, tagId is 0
        if (tagName_ != "*") {
            LOG(ERROR) << "VertexCount index type only use to statistics all vertices.";
            return cpp2::ErrorCode::E_UNSUPPORTED;
        }
        tagId_ = 0;
    }

    // Check if there is a corresponding index
    // In order to traverse quickly, put indextype in front of indexId
    // Only search in the same indextype
    auto prefix = MetaServiceUtils::indexPrefix(spaceId_, indexType_);
    std::unique_ptr<kvstore::KVIterator> checkIter;
    auto checkRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &checkIter);
    if (checkRet != kvstore::ResultCode::SUCCEEDED) {
        return MetaCommon::to(checkRet);
    }

    while (checkIter->valid()) {
        auto val = checkIter->val();
        auto item = MetaServiceUtils::parseIndex(val);
        if (item.get_schema_id().getType() != cpp2::SchemaID::Type::tag_id ||
            tagId_ != item.get_schema_id().get_tag_id()) {
            checkIter->next();
            continue;
        }
        switch (indexType_) {
            case cpp2::IndexType::NORMAL: {
                if (fieldNames_.size() > item.get_fields().size()) {
                    break;
                }
                if (checkIndexExist(fieldNames_, item)) {
                    return cpp2::ErrorCode::E_EXISTED;
                }
                break;
            }
            case cpp2::IndexType::VERTEX:
            case cpp2::IndexType::VERTEX_COUNT: {
                if (item.get_fields().size() == 0) {
                    return cpp2::ErrorCode::E_EXISTED;
                }
                break;
            }
            default:
                break;
        }
        checkIter->next();
    }

    if (indexType_ == cpp2::IndexType::NORMAL || indexType_ == cpp2::IndexType::VERTEX) {
        auto schemaRet = getLatestTagSchema(spaceId_, tagId_);
        if (!schemaRet.ok()) {
            return cpp2::ErrorCode::E_NOT_FOUND;
        }

        auto latestTagSchema = schemaRet.value();

        // TODO(pandasheep) Now normal index and ttl cannot coexist,
        // other index types can coexist with ttl, but they need to be handled separately
        if (indexType_ == cpp2::IndexType::NORMAL) {
            if (tagOrEdgeHasTTL(latestTagSchema)) {
                LOG(ERROR) << "Tag: " << tagName_ << " has ttl, not create index";
                return cpp2::ErrorCode::E_INDEX_WITH_TTL;
            }
        }

        const auto& schemaCols = latestTagSchema.get_columns();
        for (auto &field : fieldNames_) {
            auto iter = std::find_if(schemaCols.begin(), schemaCols.end(),
                                     [field](const auto& col) { return field == col.get_name(); });
            if (iter == schemaCols.end()) {
                LOG(ERROR) << "Field " << field << " not found in Tag " << tagName_;
                return cpp2::ErrorCode::E_NOT_FOUND;
            } else {
                columns_.emplace_back(*iter);
            }
        }
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

void CreateTagIndexProcessor::process(const cpp2::CreateTagIndexReq& req) {
    spaceId_ = req.get_space_id();
    indexType_ = req.get_index_type();
    indexName_ = req.get_index_name();
    tagName_ = req.get_tag_name();
    fieldNames_ = req.get_fields();
    auto ifNotExists = req.get_if_not_exists();

    CHECK_SPACE_ID_AND_RETURN(spaceId_);
    auto retCode = checkFields();
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        handleErrorCode(retCode);
        onFinished();
        return;
    }

    folly::SharedMutex::WriteHolder wHolder(LockUtils::tagIndexLock());
    auto ret = getIndexID(spaceId_, indexName_);
    if (ret.ok()) {
        LOG(ERROR) << "Create Tag Index Failed: " << indexName_ << " have existed";
        if (ifNotExists) {
            handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
        } else {
            handleErrorCode(cpp2::ErrorCode::E_EXISTED);
        }
        onFinished();
        return;
    }

    retCode = checkAndBuildIndex();
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        handleErrorCode(retCode);
        onFinished();
        return;
    }

    std::vector<kvstore::KV> data;
    auto tagIndexRet = autoIncrementId();
    if (!nebula::ok(tagIndexRet)) {
        LOG(ERROR) << "Create tag index failed : Get tag index ID failed";
        handleErrorCode(nebula::error(tagIndexRet));
        onFinished();
        return;
    }

    auto tagIndex = nebula::value(tagIndexRet);
    cpp2::IndexItem item;
    item.set_index_id(tagIndex);
    item.set_index_type(indexType_);
    item.set_index_name(indexName_);
    cpp2::SchemaID schemaID;
    schemaID.set_tag_id(tagId_);
    item.set_schema_id(schemaID);
    item.set_schema_name(tagName_);
    item.set_fields(std::move(columns_));

    data.emplace_back(MetaServiceUtils::indexIndexKey(spaceId_, indexName_),
                      std::string(reinterpret_cast<const char*>(&tagIndex), sizeof(IndexID)));
    data.emplace_back(MetaServiceUtils::indexKey(spaceId_, indexType_, tagIndex),
                      MetaServiceUtils::indexVal(item));
    LOG(INFO) << "Create Tag Index " << indexName_
              << " index type " << MetaServiceUtils::IndexTypeToString(indexType_)
              << " indexId " <<  tagIndex;
    resp_.set_id(to(tagIndex, EntryType::INDEX));
    doSyncPutAndUpdate(std::move(data));
}

}  // namespace meta
}  // namespace nebula

