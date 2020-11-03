/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/indexMan/CreateEdgeIndexProcessor.h"

namespace nebula {
namespace meta {

void CreateEdgeIndexProcessor::process(const cpp2::CreateEdgeIndexReq& req) {
    auto space = req.get_space_id();
    CHECK_SPACE_ID_AND_RETURN(space);
    const auto &indexName = req.get_index_name();
    auto &edgeName = req.get_edge_name();
    const auto &fields = req.get_fields();

    std::set<std::string> columnSet;
    for (const auto& field : fields) {
        columnSet.emplace(field.get_name());
    }
    if (fields.size() != columnSet.size()) {
        LOG(ERROR) << "Conflict field in the edge index.";
        handleErrorCode(cpp2::ErrorCode::E_CONFLICT);
        onFinished();
        return;
    }

    // A maximum of 16 columns are allowed in the index
    if (columnSet.size() > 16) {
        LOG(ERROR) << "The number of index columns exceeds maximum limit 16";
        handleErrorCode(cpp2::ErrorCode::E_CONFLICT);
        onFinished();
        return;
    }

    folly::SharedMutex::WriteHolder wHolder(LockUtils::edgeIndexLock());
    auto ret = getIndexID(space, indexName);
    if (ret.ok()) {
        LOG(ERROR) << "Create Edge Index Failed: " << indexName << " has existed";
        if (req.get_if_not_exists()) {
            resp_.set_id(to(ret.value(), EntryType::INDEX));
            handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
        } else {
            handleErrorCode(cpp2::ErrorCode::E_EXISTED);
        }
        onFinished();
        return;
    }

    auto edgeTypeRet = getEdgeType(space, edgeName);
    if (!edgeTypeRet.ok()) {
        LOG(ERROR) << "Create Edge Index Failed: " << edgeName << " not exist";
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    auto edgeType = edgeTypeRet.value();
    auto prefix = MetaServiceUtils::indexPrefix(space);
    std::unique_ptr<kvstore::KVIterator> checkIter;
    auto checkRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &checkIter);
    if (checkRet != kvstore::ResultCode::SUCCEEDED) {
        resp_.set_code(MetaCommon::to(checkRet));
        onFinished();
        return;
    }

    while (checkIter->valid()) {
        auto val = checkIter->val();
        auto item = MetaServiceUtils::parseIndex(val);
        if (item.get_schema_id().getType() != cpp2::SchemaID::Type::edge_type ||
            fields.size() > item.get_fields().size() ||
            edgeType != item.get_schema_id().get_edge_type()) {
            checkIter->next();
            continue;
        }

        if (checkIndexExist(fields, item)) {
            resp_.set_code(cpp2::ErrorCode::E_EXISTED);
            onFinished();
            return;
        }
        checkIter->next();
    }

    auto schemaRet = getLatestEdgeSchema(space, edgeType);
    if (!schemaRet.ok()) {
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    auto latestEdgeSchema = schemaRet.value();
    if (tagOrEdgeHasTTL(latestEdgeSchema)) {
       LOG(ERROR) << "Edge: " << edgeName  << " has ttl, not create index";
       handleErrorCode(cpp2::ErrorCode::E_INDEX_WITH_TTL);
       onFinished();
       return;
    }

    const auto& schemaCols = latestEdgeSchema.get_columns();
    std::vector<cpp2::ColumnDef> columns;
    for (auto &field : fields) {
        auto iter = std::find_if(schemaCols.begin(), schemaCols.end(),
                                 [field](const auto& col) {
                                     return field.get_name() == col.get_name();
                                 });

        if (iter == schemaCols.end()) {
            LOG(ERROR) << "Field " << field.get_name() << " not found in Edge " << edgeName;
            handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
            onFinished();
            return;
        }
        cpp2::ColumnDef col = *iter;
        if (col.type.get_type() == meta::cpp2::PropertyType::STRING) {
            if (!field.__isset.type_length) {
                LOG(ERROR) << "No type length set : " << field.get_name();
                handleErrorCode(cpp2::ErrorCode::E_INVALID_PARM);
                onFinished();
                return;
            }
            col.type.set_type(meta::cpp2::PropertyType::FIXED_STRING);
            col.type.set_type_length(*field.get_type_length());
        } else if (field.__isset.type_length) {
            LOG(ERROR) << "No need to set type length : " << field.get_name();
            handleErrorCode(cpp2::ErrorCode::E_INVALID_PARM);
            onFinished();
            return;
        }
        columns.emplace_back(col);
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
    item.set_index_name(indexName);
    cpp2::SchemaID schemaID;
    schemaID.set_edge_type(edgeType);
    item.set_schema_id(schemaID);
    item.set_schema_name(edgeName);
    item.set_fields(std::move(columns));

    data.emplace_back(MetaServiceUtils::indexIndexKey(space, indexName),
                      std::string(reinterpret_cast<const char*>(&edgeIndex), sizeof(IndexID)));
    data.emplace_back(MetaServiceUtils::indexKey(space, edgeIndex),
                      MetaServiceUtils::indexVal(item));
    LOG(INFO) << "Create Edge Index " << indexName << ", edgeIndex " << edgeIndex;
    resp_.set_id(to(edgeIndex, EntryType::INDEX));
    doSyncPutAndUpdate(std::move(data));
}

}  // namespace meta
}  // namespace nebula

