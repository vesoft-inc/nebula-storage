/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/indexMan/CreateEdgeIndexProcessor.h"

namespace nebula {
namespace meta {

cpp2::ErrorCode CreateEdgeIndexProcessor::checkFields() {
    std::unordered_set<std::string> columnSet(fieldNames_.begin(), fieldNames_.end());
    if (fieldNames_.size() != columnSet.size()) {
        LOG(ERROR) << "Conflict field in the edge index.";
        return cpp2::ErrorCode::E_CONFLICT;
    }

    switch (indexType_) {
        case cpp2::IndexType::NORMAL: {
            if (fieldNames_.empty()) {
                LOG(ERROR) << "The index field of an edge should not be empty.";
                return cpp2::ErrorCode::E_INVALID_PARM;
            }
            // A maximum of 16 columns are allowed in the index.
            if (columnSet.size() > 16) {
                LOG(ERROR) << "The number of normal index columns exceeds maximum limit 16";
                return cpp2::ErrorCode::E_CONFLICT;
            }
            break;
        }
        case cpp2::IndexType::EDGE: {
            // No columns are allowed.
            if (columnSet.size() > 0) {
                LOG(ERROR) << "No columns are allowed in the edge index";
                return cpp2::ErrorCode::E_CONFLICT;
            }
            break;
        }
        case cpp2::IndexType::EDGE_COUNT : {
            // No columns are allowed.
            if (columnSet.size() > 0) {
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

cpp2::ErrorCode CreateEdgeIndexProcessor::checkAndBuildIndex() {
    if (indexType_ == cpp2::IndexType::NORMAL || indexType_ == cpp2::IndexType::EDGE) {
        auto edgeTypeRet = getEdgeType(spaceId_, edgeName_);
        if (!edgeTypeRet.ok()) {
            LOG(ERROR) << "Create Edge Index Failed: " << edgeName_ << " not exist";
            return cpp2::ErrorCode::E_NOT_FOUND;
        }

        edgeType_ = edgeTypeRet.value();
    } else if (indexType_ == cpp2::IndexType::EDGE_COUNT) {
        // Statistics the number of all edge, edgeName is *, edgeType is 0
        if (edgeName_ != "*") {
            LOG(ERROR) << "EdgeCount index type only use to statistics all edge.";
            return cpp2::ErrorCode::E_UNSUPPORTED;
        }
        edgeType_ = 0;
    }

    // Check if there is a corresponding index
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
        if (item.get_schema_id().getType() != cpp2::SchemaID::Type::edge_type ||
            edgeType_ != item.get_schema_id().get_edge_type()) {
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
            case cpp2::IndexType::EDGE :
            case cpp2::IndexType::EDGE_COUNT : {
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

    if (indexType_ == cpp2::IndexType::NORMAL || indexType_ == cpp2::IndexType::EDGE) {
        auto schemaRet = getLatestEdgeSchema(spaceId_, edgeType_);
        if (!schemaRet.ok()) {
            return cpp2::ErrorCode::E_NOT_FOUND;
        }

        auto latestEdgeSchema = schemaRet.value();
        // TODO(pandasheep) Now normal index and ttl cannot coexist,
        // other index types can coexist with ttl, but they need to be handled separately
        if (indexType_ == cpp2::IndexType::NORMAL) {
            if (tagOrEdgeHasTTL(latestEdgeSchema)) {
                LOG(ERROR) << "Edge: " << edgeName_  << " has ttl, not create index";
                 return cpp2::ErrorCode::E_INDEX_WITH_TTL;
            }
        }

        const auto& schemaCols = latestEdgeSchema.get_columns();
        for (auto &field : fieldNames_) {
            auto iter = std::find_if(schemaCols.begin(), schemaCols.end(),
                                     [field](const auto& col) { return field == col.get_name(); });

            if (iter == schemaCols.end()) {
                LOG(ERROR) << "Field " << field << " not found in Edge " << edgeName_;
                return cpp2::ErrorCode::E_NOT_FOUND;
            } else {
                columns_.emplace_back(*iter);
            }
        }
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

void CreateEdgeIndexProcessor::process(const cpp2::CreateEdgeIndexReq& req) {
    spaceId_ = req.get_space_id();
    indexType_ = req.get_index_type();
    indexName_ = req.get_index_name();
    edgeName_ = req.get_edge_name();
    fieldNames_ = req.get_fields();
    auto ifNotExists = req.get_if_not_exists();

    CHECK_SPACE_ID_AND_RETURN(spaceId_);
    auto retCode = checkFields();
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        handleErrorCode(retCode);
        onFinished();
        return;
    }

    folly::SharedMutex::WriteHolder wHolder(LockUtils::edgeIndexLock());
    auto ret = getIndexID(spaceId_, indexName_);
    if (ret.ok()) {
        LOG(ERROR) << "Create Edge Index Failed: " << indexName_ << " have existed";
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
    auto edgeIndexRet = autoIncrementId();
    if (!nebula::ok(edgeIndexRet)) {
        LOG(ERROR) << "Create edge index failed: Get edge index ID failed";
        handleErrorCode(nebula::error(edgeIndexRet));
        onFinished();
        return;
    }

    auto edgeIndex = nebula::value(edgeIndexRet);
    cpp2::IndexItem item;
    item.set_index_id(edgeIndex);
    item.set_index_type(indexType_);
    item.set_index_name(indexName_);
    cpp2::SchemaID schemaID;
    schemaID.set_edge_type(edgeType_);
    item.set_schema_id(schemaID);
    item.set_schema_name(edgeName_);
    item.set_fields(std::move(columns_));

    data.emplace_back(MetaServiceUtils::indexIndexKey(spaceId_, indexName_),
                      std::string(reinterpret_cast<const char*>(&edgeIndex), sizeof(IndexID)));
    data.emplace_back(MetaServiceUtils::indexKey(spaceId_, indexType_, edgeIndex),
                      MetaServiceUtils::indexVal(item));
    LOG(INFO) << "Create Edge Index " << indexName_
              << " index type " << MetaServiceUtils::IndexTypeToString(indexType_)
              << " indexId " << edgeIndex;
    resp_.set_id(to(edgeIndex, EntryType::INDEX));
    doSyncPutAndUpdate(std::move(data));
}

}  // namespace meta
}  // namespace nebula

