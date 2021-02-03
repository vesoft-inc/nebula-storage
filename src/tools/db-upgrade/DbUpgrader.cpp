/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/fs/FileUtils.h"
#include "common/datatypes/Value.h"
#include "tools/db-upgrade/DbUpgrader.h"
#include "tools/db-upgrade/NebulaKeyUtilsV1.h"
#include "tools/db-upgrade/NebulaKeyUtilsV2.h"
#include "utils/NebulaKeyUtils.h"
#include "utils/IndexKeyUtils.h"
#include "codec/RowWriterV2.h"


DEFINE_string(src_db_path, "", "Source data path(data_path in storage 1.0 conf), "
                               "multi paths should be split by comma");
DEFINE_string(dst_db_path, "", "Destination data path(data_path in storage 2.0 conf), "
                               "multi paths should be split by comma");
DEFINE_string(upgrade_meta_server, "127.0.0.1:45500", "Meta servers' address.");
DEFINE_uint32(write_batch_num, 100, "The size of the batch written to rocksdb");
DEFINE_uint32(upgrade_version, 0, "When the value is 1, upgrade the data from 1.0 to 2.0 GA. "
                                  "When the value is 2, upgrade the data from 2.0 RC to 2.0 GA.");
DEFINE_bool(compactions, true, "When the upgrade of all spaces under a path is completed, "
                               "whether to compact");


namespace nebula {
namespace storage {

Status UpgraderSpace::init(meta::MetaClient* mclient,
                           meta::ServerBasedSchemaManager*sMan,
                           meta::IndexManager* iMan,
                           const std::string& srcPath,
                           const std::string& dstPath,
                           const std::string& entry) {
    metaClient_ = mclient;
    schemaMan_ = sMan;
    indexMan_ = iMan;
    srcPath_ = srcPath;
    dstPath_ = dstPath;
    entry_ = entry;

    auto ret = initSpace(entry_);
    if (!ret.ok()) {
        return ret;
    }

    return Status::OK();
}

Status UpgraderSpace::initSpace(const std::string& sId) {
    auto spaceId = folly::to<GraphSpaceID>(sId);
    auto sRet = schemaMan_->toGraphSpaceName(spaceId);
    if (!sRet.ok()) {
        LOG(ERROR) << "space Id " << spaceId << " no found";
        return sRet.status();
    }
    spaceName_ = sRet.value();
    spaceId_   = spaceId;

    auto spaceVidLen = metaClient_->getSpaceVidLen(spaceId_);
    if (!spaceVidLen.ok()) {
        return spaceVidLen.status();
    }
    spaceVidLen_ = spaceVidLen.value();

    // use read only rocksdb
    readEngine_.reset(new nebula::kvstore::RocksEngine(spaceId_, spaceVidLen_, srcPath_,
                                                       nullptr, nullptr, true));
    writeEngine_.reset(new nebula::kvstore::RocksEngine(spaceId_, spaceVidLen_, dstPath_));

    parts_.clear();
    parts_ = readEngine_->allParts();
    LOG(INFO) << "Src data path: " << srcPath_ << " space id " << spaceId_  << " has "
              << parts_.size() << " parts";

    tagSchemas_.clear();
    tagFieldName_.clear();
    tagIndexes_.clear();
    edgeSchemas_.clear();
    edgeFieldName_.clear();
    edgeIndexes_.clear();

    auto ret = buildSchemaAndIndex();
    if (!ret.ok()) {
         LOG(ERROR) << "Build schema and index failed in space id " << spaceId_;
         return ret;
    }
    return Status::OK();
}

Status UpgraderSpace::buildSchemaAndIndex() {
    // Get all tag in space
    auto tags = schemaMan_->getAllVerTagSchema(spaceId_);
    if (!tags.ok()) {
        LOG(ERROR) << "space Id " << spaceId_ << " no found";
        return tags.status();
    }
    tagSchemas_ = std::move(tags).value();

    for (auto&tag : tagSchemas_) {
        auto tagId = tag.first;
        auto newestTagschema = tag.second.back();
        auto fields = newestTagschema->getNumFields();
        for (size_t i = 0; i < fields; i++) {
            tagFieldName_[tagId].emplace_back(newestTagschema->getFieldName(i));
        }
        LOG(INFO) << "Tag Id " << tagId << " has " << tagFieldName_[tagId].size()
                  << " fields!";
    }

    // Get all tag index in space
    std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>> tagIndexes;
    auto iRet = indexMan_->getTagIndexes(spaceId_);
    if (!iRet.ok()) {
        LOG(ERROR) << "space Id " << spaceId_ << " no found";
        return iRet.status();
    }
    tagIndexes = std::move(iRet).value();

    // Handle tag index
    for (auto&tagIndex : tagIndexes) {
        auto tagId = tagIndex->get_schema_id().get_tag_id();
        LOG(INFO) << "Tag Id " << tagId;
        tagIndexes_[tagId].emplace(tagIndex);
    }

    for (auto&tagindexes : tagIndexes_) {
        LOG(INFO) << "Tag Id " << tagindexes.first  << " has "
                  << tagindexes.second.size() << " indexes";
    }

    // Get all edge in space
    auto edges = schemaMan_->getAllVerEdgeSchema(spaceId_);
    if (!edges.ok()) {
        LOG(ERROR) << "space Id " << spaceId_ << " no found";
        return edges.status();
    }
    edgeSchemas_ = std::move(edges).value();

    for (auto&edge : edgeSchemas_) {
        auto edgetype = edge.first;
        auto newestEdgeSchema = edge.second.back();
        auto fields = newestEdgeSchema->getNumFields();
        for (size_t i = 0; i < fields; i++) {
            edgeFieldName_[std::abs(edgetype)].emplace_back(newestEdgeSchema->getFieldName(i));
        }
        LOG(INFO) << "Edgetype " << edgetype << " has " << edgeFieldName_[std::abs(edgetype)].size()
                  << " fields!";
    }

    // Get all edge index in space
    std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>> edgeIndexes;
    iRet = indexMan_->getEdgeIndexes(spaceId_);
    if (!iRet.ok()) {
        LOG(ERROR) << "space Id " << spaceId_ << " no found";
        return iRet.status();
    }
    edgeIndexes = std::move(iRet).value();

    // Handle edge index
    for (auto& edgeIndex : edgeIndexes) {
        auto edgetype = edgeIndex->get_schema_id().get_edge_type();
        LOG(INFO) << "Edgetype " << edgetype;
        edgeIndexes_[edgetype].emplace(edgeIndex);
    }

    for (auto&edgeindexes : edgeIndexes_) {
        LOG(INFO) << "EdgeType " << edgeindexes.first  << " has "
                  << edgeindexes.second.size() << " indexes";
    }
    return  Status::OK();
}

bool UpgraderSpace::isValidVidLen(VertexID srcVId, VertexID dstVId) {
    if (!NebulaKeyUtils::isValidVidLen(spaceVidLen_, srcVId, dstVId)) {
        LOG(ERROR) << "vertex id length is illegal, expect: " << spaceVidLen_
                   << " result: " << srcVId << " " << dstVId;
        return false;
    }
    return true;
}

void UpgraderSpace::doProcessV1() {
    LOG(INFO) << "Start to handle data in space Id " << spaceId_;
    // Handle vertex and edge, if there is an index, generate index data
    for (auto& partId : parts_) {
        LOG(INFO) << "Start to handle vertex/edge/index data in space Id " << spaceId_
                  << " partId " << partId;
        auto prefix = NebulaKeyUtilsV1::prefix(partId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retCode = readEngine_->prefix(prefix, &iter);
        if (retCode != kvstore::ResultCode::SUCCEEDED) {
            LOG(ERROR) << "Space id " << spaceId_ << " part " << partId
                       << " no found!";
            LOG(ERROR) << "Handle vertex/edge/index data in space Id " << spaceId_
                       << " partId " << partId << " failed";
            continue;
        }

        std::vector<kvstore::KV> data;
        TagID                                lastTagId = 0;
        int64_t                              lastVertexId = 0;

        int64_t                              lastSrcVertexId = 0;
        EdgeType                             lastEdgeType = 0;
        int64_t                              lastDstVertexId = 0;
        EdgeRanking                          lastRank = 0;

        while (iter && iter->valid()) {
            auto key = iter->key();
            if (NebulaKeyUtilsV1::isVertex(key)) {
                auto vId = NebulaKeyUtilsV1::getVertexId(key);
                auto tagId = NebulaKeyUtilsV1::getTagId(key);

                auto it = tagSchemas_.find(tagId);
                if (it == tagSchemas_.end()) {
                    // Invalid data
                    iter->next();
                    continue;
                }
                auto iterField = tagFieldName_.find(tagId);
                if (iterField == tagFieldName_.end()) {
                    // Invalid data
                    iter->next();
                    continue;
                }
                if (vId == lastVertexId && tagId == lastTagId) {
                    // Multi version
                    iter->next();
                    continue;
                }

                auto strVid = std::string(reinterpret_cast<const char*>(&vId), sizeof(vId));
                auto newTagSchema = it->second.back().get();
                // Generate 2.0 key
                auto newKey = NebulaKeyUtils::vertexKey(spaceVidLen_,
                                                        partId,
                                                        strVid,
                                                        tagId);
                auto val = iter->val();
                auto reader = RowReaderWrapper::getTagPropReader(schemaMan_, spaceId_, tagId, val);
                if (!reader) {
                    LOG(ERROR) << "Can't get tag reader of " << tagId;
                    iter->next();
                    continue;
                }
                // Generate 2.0 value and index records
                encodeVertexValue(partId, reader.get(), newTagSchema,
                                  newKey, strVid, tagId, data);

                lastTagId = tagId;
                lastVertexId = vId;
            } else if (NebulaKeyUtilsV1::isEdge(key)) {
                auto svId = NebulaKeyUtilsV1::getSrcId(key);
                auto edgetype = NebulaKeyUtilsV1::getEdgeType(key);
                auto ranking = NebulaKeyUtilsV1::getRank(key);
                auto dvId = NebulaKeyUtilsV1::getDstId(key);

                auto it = edgeSchemas_.find(std::abs(edgetype));
                if (it == edgeSchemas_.end()) {
                    // Invalid data
                    iter->next();
                    continue;
                }
                auto iterField = edgeFieldName_.find(std::abs(edgetype));
                if (iterField == edgeFieldName_.end()) {
                    // Invalid data
                    iter->next();
                    continue;
                }
                if (svId == lastSrcVertexId &&
                    edgetype == lastEdgeType &&
                    ranking == lastRank &&
                    dvId == lastDstVertexId) {
                    // Multi version
                    iter->next();
                    continue;
                }

                auto strsvId = std::string(reinterpret_cast<const char*>(&svId), sizeof(svId));
                auto strdvId = std::string(reinterpret_cast<const char*>(&dvId), sizeof(dvId));

                auto newEdgeSchema = it->second.back().get();

                // Generate 2.0 key
                auto newKey = NebulaKeyUtils::edgeKey(spaceVidLen_,
                                                      partId,
                                                      strsvId,
                                                      edgetype,
                                                      ranking,
                                                      strdvId);
                auto val = iter->val();
                auto reader = RowReaderWrapper::getEdgePropReader(schemaMan_,
                                                                  spaceId_,
                                                                  std::abs(edgetype),
                                                                  val);
                if (!reader) {
                    LOG(ERROR) << "Can't get edge reader of " << edgetype;
                    iter->next();
                    continue;
                }
                // Generate 2.0 value and index records
                encodeEdgeValue(partId, reader.get(), newEdgeSchema, newKey, strsvId,
                                edgetype, ranking, strdvId, data);
                lastSrcVertexId = svId;
                lastEdgeType  = edgetype;
                lastRank = ranking;
                lastDstVertexId = dvId;
            }

            if (data.size() >= FLAGS_write_batch_num) {
                VLOG(2) << "Send record total rows " << data.size();
                auto code = writeEngine_->multiPut(data);
                if (code != kvstore::ResultCode::SUCCEEDED) {
                    LOG(FATAL) << "Write multi put failed in space id " << spaceId_
                               << " partid " << partId;
                }
                data.clear();
            }

            iter->next();
        }

        auto code = writeEngine_->multiPut(data);
        if (code != kvstore::ResultCode::SUCCEEDED) {
            LOG(FATAL) << "Write multi put failed in space id " << spaceId_
                       << " partid " << partId;
        }
        data.clear();
        LOG(INFO) << "Handle vertex/edge/index data in space Id " << spaceId_
                  << " partId " << partId << " succeed";
    }

    // handle system data
    {
        LOG(INFO) << "Start to handle system data in space Id " << spaceId_;
        auto prefix = NebulaKeyUtilsV1::systemPrefix();
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retCode = readEngine_->prefix(prefix, &iter);
        if (retCode != kvstore::ResultCode::SUCCEEDED) {
            LOG(ERROR) << "Space id " << spaceId_ << " get system data failed";
            LOG(ERROR) << "Handle system data in space Id " << spaceId_ << " failed";
            return;
        }
        std::vector<kvstore::KV> data;
        while (iter && iter->valid()) {
            auto key = iter->key();
            auto val = iter->val();
            data.emplace_back(std::move(key), std::move(val));
            if (data.size() >= FLAGS_write_batch_num) {
                VLOG(2) << "Send system data total rows " << data.size();
                auto code = writeEngine_->multiPut(data);
                if (code != kvstore::ResultCode::SUCCEEDED) {
                    LOG(FATAL) << "Write multi put failed in space id " << spaceId_;
                }
                data.clear();
            }
            iter->next();
        }

        auto code = writeEngine_->multiPut(data);
        if (code != kvstore::ResultCode::SUCCEEDED) {
            LOG(FATAL) << "Write multi put failed in space id " << spaceId_;
        }
        LOG(INFO) << "Handle system data in space Id " << spaceId_ << " success";
        LOG(INFO) << "Handle data in space Id " << spaceId_ << " success";
    }
}

void UpgraderSpace::doProcessV2() {
    LOG(INFO) << "Start to handle data in space Id " << spaceId_;
    // Handle vertex and edge, if there is an index, generate index data
    for (auto& partId : parts_) {
        LOG(INFO) << "Start to handle vertex/edge/index data in space Id " << spaceId_
                  << " partId " << partId;
        auto prefix = NebulaKeyUtilsV2::partPrefix(partId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retCode = readEngine_->prefix(prefix, &iter);
        if (retCode != kvstore::ResultCode::SUCCEEDED) {
            LOG(ERROR) << "Space id " << spaceId_ << " part " << partId
                       << " no found!";
            LOG(ERROR) << "Handle vertex/edge/index data in space Id " << spaceId_
                       << " partId " << partId << " failed";
            continue;
        }

        std::vector<kvstore::KV> data;
        TagID                                lastTagId = 0;
        VertexID                             lastVertexId = "";

        VertexID                             lastSrcVertexId = "";
        EdgeType                             lastEdgeType = 0;
        VertexID                             lastDstVertexId = "";
        EdgeRanking                          lastRank = 0;

        while (iter && iter->valid()) {
            auto key = iter->key();
            if (NebulaKeyUtilsV2::isVertex(spaceVidLen_, key)) {
                auto vId = NebulaKeyUtilsV2::getVertexId(spaceVidLen_, key).str();
                auto tagId = NebulaKeyUtilsV2::getTagId(spaceVidLen_, key);

                auto it = tagSchemas_.find(tagId);
                if (it == tagSchemas_.end()) {
                    // Invalid data
                    iter->next();
                    continue;
                }
                auto iterField = tagFieldName_.find(tagId);
                if (iterField == tagFieldName_.end()) {
                    // Invalid data
                    iter->next();
                    continue;
                }
                if (vId == lastVertexId && tagId == lastTagId) {
                    // Multi version
                    iter->next();
                    continue;
                }

                auto newTagSchema = it->second.back().get();
                // Generate 2.0 key
                auto newKey = NebulaKeyUtils::vertexKey(spaceVidLen_,
                                                        partId,
                                                        vId,
                                                        tagId);
                auto val = iter->val();
                auto reader = RowReaderWrapper::getTagPropReader(schemaMan_, spaceId_, tagId, val);
                if (!reader) {
                    LOG(ERROR) << "Can't get tag reader of " << tagId;
                    iter->next();
                    continue;
                }
                // Generate 2.0 value and index records
                encodeVertexValue(partId, reader.get(), newTagSchema,
                                  newKey, vId, tagId, data);

                lastTagId = tagId;
                lastVertexId = vId;
            } else if (NebulaKeyUtilsV2::isEdge(spaceVidLen_, key)) {
                auto svId = NebulaKeyUtilsV2::getSrcId(spaceVidLen_, key).str();
                auto edgetype = NebulaKeyUtilsV2::getEdgeType(spaceVidLen_, key);
                auto ranking = NebulaKeyUtilsV2::getRank(spaceVidLen_, key);
                auto dvId = NebulaKeyUtilsV2::getDstId(spaceVidLen_, key).str();

                auto it = edgeSchemas_.find(std::abs(edgetype));
                if (it == edgeSchemas_.end()) {
                    // Invalid data
                    iter->next();
                    continue;
                }
                auto iterField = edgeFieldName_.find(std::abs(edgetype));
                if (iterField == edgeFieldName_.end()) {
                    // Invalid data
                    iter->next();
                    continue;
                }
                if (svId == lastSrcVertexId &&
                    edgetype == lastEdgeType &&
                    ranking == lastRank &&
                    dvId == lastDstVertexId) {
                    // Multi version
                    iter->next();
                    continue;
                }

                auto newEdgeSchema = it->second.back().get();

                // Generate 2.0 key
                auto newKey = NebulaKeyUtils::edgeKey(spaceVidLen_,
                                                      partId,
                                                      svId,
                                                      edgetype,
                                                      ranking,
                                                      dvId);
                auto val = iter->val();
                auto reader = RowReaderWrapper::getEdgePropReader(schemaMan_,
                                                                  spaceId_,
                                                                  std::abs(edgetype),
                                                                  val);
                if (!reader) {
                    LOG(ERROR) << "Can't get edge reader of " << edgetype;
                    iter->next();
                    continue;
                }
                // Generate 2.0 value and index records
                encodeEdgeValue(partId, reader.get(), newEdgeSchema, newKey, svId,
                                edgetype, ranking, dvId, data);
                lastSrcVertexId = svId;
                lastEdgeType  = edgetype;
                lastRank = ranking;
                lastDstVertexId = dvId;
            }

            if (data.size() >= FLAGS_write_batch_num) {
                VLOG(2) << "Send record total rows " << data.size();
                auto code = writeEngine_->multiPut(data);
                if (code != kvstore::ResultCode::SUCCEEDED) {
                    LOG(FATAL) << "Write multi put failed in space id " << spaceId_
                               << " partid " << partId;
                }
                data.clear();
            }

            iter->next();
        }

        auto code = writeEngine_->multiPut(data);
        if (code != kvstore::ResultCode::SUCCEEDED) {
            LOG(FATAL) << "Write multi put failed in space id " << spaceId_
                       << " partid " << partId;
        }
        data.clear();
        LOG(INFO) << "Handle vertex/edge/index data in space Id " << spaceId_
                  << " partId " << partId << " succeed";
    }

    // handle system data
    {
        LOG(INFO) << "Start to handle system data in space Id " << spaceId_;
        auto prefix = NebulaKeyUtilsV2::systemPrefix();
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retCode = readEngine_->prefix(prefix, &iter);
        if (retCode != kvstore::ResultCode::SUCCEEDED) {
            LOG(ERROR) << "Space id " << spaceId_ << " get system data failed";
            LOG(ERROR) << "Handle system data in space Id " << spaceId_ << " failed";
            return;
        }
        std::vector<kvstore::KV> data;
        while (iter && iter->valid()) {
            auto key = iter->key();
            auto val = iter->val();
            data.emplace_back(std::move(key), std::move(val));
            if (data.size() >= FLAGS_write_batch_num) {
                VLOG(2) << "Send system data total rows " << data.size();
                auto code = writeEngine_->multiPut(data);
                if (code != kvstore::ResultCode::SUCCEEDED) {
                    LOG(FATAL) << "Write multi put failed in space id " << spaceId_;
                }
                data.clear();
            }
            iter->next();
        }

        auto code = writeEngine_->multiPut(data);
        if (code != kvstore::ResultCode::SUCCEEDED) {
            LOG(FATAL) << "Write multi put failed in space id " << spaceId_;
        }
        LOG(INFO) << "Handle system data in space Id " << spaceId_ << " success";
        LOG(INFO) << "Handle data in space Id " << spaceId_ << " success";
    }
}

void UpgraderSpace::encodeVertexValue(PartitionID partId,
                                      RowReader* reader,
                                      const meta::NebulaSchemaProvider* schema,
                                      std::string& newkey,
                                      VertexID& strVid,
                                      TagID tagId,
                                      std::vector<kvstore::KV>& data) {
    // Get all returned field name
    auto& fieldNames = tagFieldName_[tagId];

    auto ret = encodeRowVal(reader, schema, fieldNames);
    if (ret.empty()) {
        LOG(ERROR)  << "Vertex or edge value is empty";
        return;
    }
    data.emplace_back(std::move(newkey), ret);

    // encode v2 index value
    auto it = tagIndexes_.find(tagId);
    if (it != tagIndexes_.end()) {
        // Use new RowReader
        auto nReader = RowReaderWrapper::getTagPropReader(schemaMan_, spaceId_, tagId, ret);
        if (nReader == nullptr) {
             LOG(ERROR) << "Bad format row";
             return;
        }
        for (auto& index : it->second) {
            auto newIndexKey = indexVertexKey(partId, strVid, nReader.get(), index);
            if (!newIndexKey.empty()) {
                data.emplace_back(std::move(newIndexKey), "");
            }
        }
    }
}

// Used for vertex and edge
std::string UpgraderSpace::encodeRowVal(const RowReader* reader,
                                        const meta::NebulaSchemaProvider* schema,
                                        std::vector<std::string>& fieldName) {
    auto oldSchema = reader->getSchema();
    if (oldSchema == nullptr) {
        LOG(ERROR)  << "schema not found from RowReader.";
        return "";
    }

    // encode v2 value, use new schema
    WriteResult wRet;
    RowWriterV2 rowWrite(schema);

    // fieldName contains all the fields of the latest schema.
    // The data reader may not use the latest schema,
    // If it does not contain the field, the default value or null value will be used.
    for (auto& name : fieldName) {
        auto val = reader->getValueByName(name);
        if (val.type() != Value::Type::NULLVALUE) {
            wRet = rowWrite.setValue(name, val);
            if (wRet != WriteResult::SUCCEEDED) {
                LOG(ERROR)  << "Write rowWriterV2 failed";
                return "";
            }
        } else {
            // read null value
            auto nullType = val.getNull();
            if (nullType == NullType::__NULL__) {
                wRet = rowWrite.setValue(name, val);
                if (wRet != WriteResult::SUCCEEDED) {
                    LOG(ERROR)  << "Write rowWriterV2 failed";
                    return "";
                }
            } else if (nullType != NullType::UNKNOWN_PROP) {
                // nullType == NullType::kNullUnknownProp, indicates that the field is
                // only in the latest schema, maybe use default value or null value.
                LOG(ERROR)  << "Data is illegal in " << name << " field";
                return "";
            }
        }
    }

    wRet = rowWrite.finish();
    if (wRet != WriteResult::SUCCEEDED) {
        LOG(ERROR)  << "Write rowWriterV2 failed";
        return "";
    }

    return std::move(rowWrite).moveEncodedStr();
}

std::string UpgraderSpace::indexVertexKey(PartitionID partId,
                VertexID& vId,
                RowReader* reader,
                std::shared_ptr<nebula::meta::cpp2::IndexItem> index) {
    auto values = IndexKeyUtils::collectIndexValues(reader, index->get_fields());
    if (!values.ok()) {
        return "";
    }
    return IndexKeyUtils::vertexIndexKey(spaceVidLen_,
                                         partId,
                                         index->get_index_id(),
                                         vId,
                                         std::move(values).value());
}

void UpgraderSpace::encodeEdgeValue(PartitionID partId,
                                    RowReader* reader,
                                    const meta::NebulaSchemaProvider* schema,
                                    std::string& newkey,
                                    VertexID& svId,
                                    EdgeType type,
                                    EdgeRanking rank,
                                    VertexID& dstId,
                                    std::vector<kvstore::KV>& data) {
    // Get all returned field name
    auto& fieldNames = edgeFieldName_[std::abs(type)];

    auto ret = encodeRowVal(reader, schema, fieldNames);
    if (ret.empty()) {
        return;
    }
    data.emplace_back(std::move(newkey), ret);

    if (type <= 0) {
        return;
    }

    // encode v2 index value
    auto it = edgeIndexes_.find(type);
    if (it != edgeIndexes_.end()) {
        auto nReader = RowReaderWrapper::getEdgePropReader(schemaMan_, spaceId_, type, ret);
        if (nReader == nullptr) {
            LOG(ERROR) << "Bad format row";
            return;
        }
        for (auto& index : it->second) {
            auto newIndexKey = indexEdgeKey(partId, nReader.get(), svId, rank, dstId, index);
            if (!newIndexKey.empty()) {
                data.emplace_back(std::move(newIndexKey), "");
            }
        }
    }
}

std::string UpgraderSpace::indexEdgeKey(PartitionID partId,
                        RowReader* reader,
                        VertexID& svId,
                        EdgeRanking rank,
                        VertexID& dstId,
                        std::shared_ptr<nebula::meta::cpp2::IndexItem> index) {
    auto values = IndexKeyUtils::collectIndexValues(reader, index->get_fields());
    if (!values.ok()) {
        return "";
    }
    return IndexKeyUtils::edgeIndexKey(spaceVidLen_,
                                       partId,
                                       index->get_index_id(),
                                       svId,
                                       rank,
                                       dstId,
                                       std::move(values).value());
}


void UpgraderSpace::doCompaction() {
    auto ret = writeEngine_->compact();
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Path " << dstPath_ << " space id " << spaceId_
                   << " compaction failed!";
    }
    LOG(INFO) << "Path " << dstPath_ << " space id " << spaceId_
              << " compaction success!";
}

Status DbUpgrader::init(meta::MetaClient* mclient,
                        meta::ServerBasedSchemaManager*sMan,
                        meta::IndexManager* iMan,
                        const std::string& srcPath,
                        const std::string& dstPath) {
    metaClient_ = mclient;
    schemaMan_ = sMan;
    indexMan_ = iMan;
    srcPath_ = srcPath;
    dstPath_ = dstPath;

    auto ret = listSpace();
    if (!ret.ok()) {
        return ret;
    }

    return Status::OK();
}

Status DbUpgrader::listSpace() {
    // from srcPath_ to srcPath_/nebula
    auto path = fs::FileUtils::joinPath(srcPath_, "nebula");
    if (!fs::FileUtils::exist(path)) {
        LOG(ERROR) << "Source data path " << srcPath_ << " not exists!";
        return Status::Error("Db path '%s' not exists.", srcPath_.c_str());
    }

    if (!fs::FileUtils::exist(dstPath_)) {
         LOG(ERROR)  << "Destination data path " << dstPath_ << " not exists!";
         return Status::Error("Db path '%s' not exists.", dstPath_.c_str());
    }
    subDirs_ = fs::FileUtils::listAllDirsInDir(path.c_str());
    return Status::OK();
}

void DbUpgrader::run() {
    LOG(INFO) << "Upgrade from path " << srcPath_ << " to path "
              << dstPath_ << " in DbUpgrader run begin";
    // 1. Get all the directories in the data directory,
    // each directory name is spaceId, traverse each directory
    std::vector<std::unique_ptr<nebula::storage::UpgraderSpace>> upgraderSpaces;
    for (auto& entry : subDirs_) {
        auto it = std::make_unique<nebula::storage::UpgraderSpace>();
        auto ret = it->init(metaClient_, schemaMan_, indexMan_, srcPath_, dstPath_, entry);
        if (!ret.ok()) {
            LOG(INFO) << "Upgrade from path " << srcPath_  << " space id" << entry
                       << " to path " << dstPath_ << " init failed";
            return;
        }
        upgraderSpaces.emplace_back(std::move(it));
    }

    std::vector<std::thread> threads;
    for (size_t i = 0; i < upgraderSpaces.size(); i++) {
        threads.emplace_back(std::thread([upgraderSpaceIter = upgraderSpaces[i].get()] {
            LOG(INFO) << "Upgrade from path " << upgraderSpaceIter->srcPath_
                      << " space id" << upgraderSpaceIter->entry_ << " to path "
                      << upgraderSpaceIter->dstPath_ << " begin";
            if (FLAGS_upgrade_version == 1) {
                upgraderSpaceIter->doProcessV1();
            } else {
                upgraderSpaceIter->doProcessV2();
            }
            LOG(INFO) << "Upgrade from path " << upgraderSpaceIter->srcPath_
                      << " space id" << upgraderSpaceIter->entry_ << " to path "
                      << upgraderSpaceIter->dstPath_ << " end";
        }));
    }

    // Wait for all threads to finish
    for (auto& t : threads) {
        t.join();
    }

    if (FLAGS_compactions) {
        std::vector<std::thread> compactThreads;
        for (size_t i = 0; i < upgraderSpaces.size(); i++) {
            compactThreads.emplace_back(std::thread([upgraderSpaceIter = upgraderSpaces[i].get()] {
                LOG(INFO) << "Upgrade from path " << upgraderSpaceIter->srcPath_
                          << " space id" << upgraderSpaceIter->entry_ << " to path "
                          << upgraderSpaceIter->dstPath_ << " compaction begin";
                upgraderSpaceIter->doCompaction();

                LOG(INFO) << "Upgrade from path " << upgraderSpaceIter->srcPath_
                          << " space id" << upgraderSpaceIter->entry_ << " to path "
                          << upgraderSpaceIter->dstPath_ << " compaction end";
            }));
        }

        // Wait for all threads to finish
        for (auto& t : compactThreads) {
            t.join();
        }
    }

    LOG(INFO) << "Upgrade from path " << srcPath_ << " to path "
              << dstPath_ << " in DbUpgrader run end";
}

}  // namespace storage
}  // namespace nebula
