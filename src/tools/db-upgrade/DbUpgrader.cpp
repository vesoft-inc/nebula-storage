/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/fs/FileUtils.h"
#include "common/datatypes/Value.h"
#include "tools/db-upgrade/DbUpgrader.h"
#include "tools/db-upgrade/NebulaKeyUtilsV1.h"
#include "utils/NebulaKeyUtils.h"
#include "utils/IndexKeyUtils.h"
#include "codec/RowWriterV2.h"


DEFINE_string(upgrade_db_path, "./", "Path to rocksdb.");
DEFINE_string(upgrade_meta_server, "127.0.0.1:45500", "Meta servers' address.");

namespace nebula {
namespace storage {

Status DbUpgrader::init() {
    auto status = initMeta();
    if (!status.ok()) {
        return status;
    }

    status = listSpace();
    if (!status.ok()) {
        return status;
    }

    return Status::OK();
}


Status DbUpgrader::initMeta() {
    auto addrs = network::NetworkUtils::toHosts(FLAGS_upgrade_meta_server);
    if (!addrs.ok()) {
        return addrs.status();
    }

    auto ioExecutor = std::make_shared<folly::IOThreadPoolExecutor>(1);
    meta::MetaClientOptions options;
    options.skipConfig_ = true;
    metaClient_ = std::make_unique<meta::MetaClient>(ioExecutor,
                                                     std::move(addrs.value()),
                                                     options);
    if (!metaClient_->waitForMetadReady(1)) {
        return Status::Error("Meta is not ready: '%s'.", FLAGS_upgrade_meta_server.c_str());
    }
    schemaMan_ = meta::ServerBasedSchemaManager::create(metaClient_.get());
    indexMan_ = meta::ServerBasedIndexManager::create(metaClient_.get());
    return Status::OK();
}

// Get all string spaceId
Status DbUpgrader::listSpace() {
    // from FLAGS_upgrade_db_path to FLAGS_upgrade_db_path/nebula
    auto path = fs::FileUtils::joinPath(FLAGS_upgrade_db_path, "nebula");
    if (!fs::FileUtils::exist(path)) {
        return Status::Error("Db path '%s' not exists.", FLAGS_upgrade_db_path.c_str());
    }
    subDirs_ = fs::FileUtils::listAllDirsInDir(path.c_str());
    return Status::OK();
}

Status DbUpgrader::initSpace(std::string& sId) {
    auto spaceId = folly::to<GraphSpaceID>(sId);
    auto sRet = schemaMan_->toGraphSpaceName(spaceId);
    if (!sRet.ok()) {
        LOG(INFO) << "space Id " << spaceId << " no found";
        return sRet.status();
    }
    spaceName_ = sRet.value();
    spaceId_   = spaceId;

    auto spaceVidLen = metaClient_->getSpaceVidLen(spaceId_);
    if (!spaceVidLen.ok()) {
        return spaceVidLen.status();
    }
    spaceVidLen_ = spaceVidLen.value();

    engine_.reset(new nebula::kvstore::RocksEngine(spaceId_, spaceVidLen_, FLAGS_upgrade_db_path));
    parts_.clear();
    parts_ = engine_->allParts();
    LOG(INFO) << "Path: " << FLAGS_upgrade_db_path << "\n"
              << "Space Id " << spaceId_ << " need handle " << parts_.size() << " parts!";
    tagSchemas_.clear();
    tagIndexes_.clear();
    edgeSchemas_.clear();
    edgeIndexes_.clear();
    return Status::OK();
}


Status DbUpgrader::buildSchemaAndIndex() {
    auto tags = schemaMan_->getAllVerTagSchema(spaceId_);
    if (!tags.ok()) {
        LOG(INFO) << "space Id " << spaceId_ << " no found";
        return tags.status();
    }
    tagSchemas_ = std::move(tags).value();

    // Get all index in space
    std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>> tagIndexes;
    auto iRet = indexMan_->getTagIndexes(spaceId_);
    if (iRet.ok()) {
        tagIndexes = std::move(iRet).value();
    }

    // handle index
    for (auto&tagIndex : tagIndexes) {
        if (tagIndex->schema_id.tag_id > 0) {
            tagIndexes_[tagIndex->schema_id.tag_id].emplace(tagIndex);
        }
    }

    for (auto&tagindexes : tagIndexes_) {
        LOG(INFO) << "Tag Id " << tagindexes.first  << " has "
                  << tagindexes.second.size() << " indexes";
    }

    // Get all edge in space
    auto edges = schemaMan_->getAllVerEdgeSchema(spaceId_);
    if (!edges.ok()) {
        LOG(INFO) << "space Id " << spaceId_ << " no found";
        return edges.status();
    }
    edgeSchemas_ = std::move(edges).value();

    // Get all edge index in space
    std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>> edgeIndexes;
    iRet = indexMan_->getEdgeIndexes(spaceId_);
    if (iRet.ok()) {
       edgeIndexes = std::move(iRet).value();
    }
    // handle index
    for (auto& edgeIndex : edgeIndexes) {
        if (edgeIndex->schema_id.edge_type > 0) {
            edgeIndexes_[edgeIndex->schema_id.edge_type].emplace(edgeIndex);
        }
    }
    for (auto&edgeindexes : edgeIndexes_) {
        LOG(INFO) << "EdgeType " << edgeindexes.first  << " has "
                  << edgeindexes.second.size() << " indexes";
    }
    return  Status::OK();
}


bool DbUpgrader::isValidVidLen(VertexID srcVId, VertexID dstVId) {
    if (!NebulaKeyUtils::isValidVidLen(spaceVidLen_, srcVId, dstVId)) {
        LOG(INFO) << "vertex id length is illegal, expect: " << spaceVidLen_
                  << " result: " << srcVId << " " << dstVId;
        return false;
    }
    return true;
}

// Used for vertex and edge
std::string DbUpgrader::encodeRowVal(const RowReader* reader,
                                     const meta::NebulaSchemaProvider* schema) {
    auto oldSchema = reader->getSchema();
    if (oldSchema == nullptr) {
        LOG(INFO)  << "schema not found.";
        return "";
    }
    std::vector<Value> props;
    auto iter = oldSchema->begin();
    size_t idx = 0;
    while (iter) {
        auto value = reader->getValueByIndex(idx);
        if (value != Value::kNullUnknownProp && value != Value::kNullBadType) {
            props.emplace_back(value);
        } else {
            LOG(INFO)  << "Data is illegal.";
            return "";
        }
        ++iter;
        ++idx;
    }

    // encode v2 value, use new schema
    WriteResult wRet;
    RowWriterV2 rowWrite(schema);
    for (size_t i = 0; i < props.size(); i++) {
        wRet = rowWrite.setValue(i, props[i]);
        if (wRet != WriteResult::SUCCEEDED) {
            LOG(INFO)  << "Write rowWriterV2 failed";
            return "";
        }
    }
    wRet = rowWrite.finish();
    if (wRet != WriteResult::SUCCEEDED) {
        LOG(INFO)  << "Write rowWriterV2 failed";
        return "";
    }

    return std::move(rowWrite).moveEncodedStr();
}

// Think that the old and new values are exactly the same, so use one reader
void DbUpgrader::encodeVertexValue(PartitionID partId,
                                 RowReader* reader,
                                 const meta::NebulaSchemaProvider* schema,
                                 std::string& newkey,
                                 VertexID& strVid,
                                 TagID   tagId,
                                 std::vector<kvstore::KV>& data) {
    if (reader == nullptr) {
        return;
    }
    auto ret = encodeRowVal(reader, schema);
    if (ret.empty()) {
        return;
    }
    data.emplace_back(std::move(newkey), std::move(ret));

    // encode v2 index value
    auto it = tagIndexes_.find(tagId);
    if (it != tagIndexes_.end()) {
        for (auto& index : it->second) {
            auto newIndexKey = indexVertexKey(partId, strVid, reader, index);
            if (!newIndexKey.empty()) {
                data.emplace_back(std::move(newIndexKey), "");
            }
        }
    }
}

std::string DbUpgrader::indexVertexKey(PartitionID partId,
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

// Think that the old and new values are exactly the same, so use one reader
void DbUpgrader::encodeEdgeValue(PartitionID partId,
                               RowReader* reader,
                               const meta::NebulaSchemaProvider* schema,
                               std::string& newkey,
                               VertexID& svId,
                               EdgeType type,
                               EdgeRanking rank,
                               VertexID& dstId,
                               std::vector<kvstore::KV>& data) {
    if (reader == nullptr) {
        return;
    }
    auto ret = encodeRowVal(reader, schema);
    if (ret.empty()) {
        return;
    }
    data.emplace_back(std::move(newkey), std::move(ret));

    if (type <= 0) {
        return;
    }

    // encode v2 index value
    auto it = edgeIndexes_.find(type);
    if (it != edgeIndexes_.end()) {
        for (auto& index : it->second) {
            auto newIndexKey = indexEdgeKey(partId, reader, svId, rank, dstId, index);
            if (!newIndexKey.empty()) {
                data.emplace_back(std::move(newIndexKey), "");
            }
        }
    }
}

std::string DbUpgrader::indexEdgeKey(PartitionID partId,
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

void DbUpgrader::doProcessAllTagsAndEdges() {
    auto ret = buildSchemaAndIndex();
    if (!ret.ok()) {
         LOG(INFO) << "Build schema and index failed";
         return;
    }

    for (auto& partId : parts_) {
        LOG(INFO) << "Start space id " << spaceId_ << " partId " <<  partId;
        auto prefix = NebulaKeyUtilsV1::prefix(partId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retCode = engine_->prefix(prefix, &iter);
        if (retCode != kvstore::ResultCode::SUCCEEDED) {
            LOG(INFO) << "Space id " << spaceId_ << " part " << partId
                      << " no found!";
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
                auto version = NebulaKeyUtilsV1::getVersion(key);

                auto it = tagSchemas_.find(tagId);
                if (it == tagSchemas_.end()) {
                    // Invalid data
                    iter->next();
                    continue;
                }
                if (vId == lastVertexId && tagId == lastTagId) {
                    // Multi version
                    iter->next();
                    continue;
                }

                // auto strVid = folly::to<std::string>(vId);
                auto strVid = std::string(reinterpret_cast<const char*>(&vId), sizeof(vId));
                if (!isValidVidLen(strVid)) {
                   iter->next();
                   continue;
                }

                auto newSchema = it->second.back();
                // Generate 2.0 key
                auto newKey = NebulaKeyUtils::vertexKey(spaceVidLen_,
                                                        partId,
                                                        strVid,
                                                        tagId,
                                                        version);
                auto val = iter->val();
                auto reader = RowReaderWrapper::getTagPropReader(schemaMan_.get(),
                                                                 spaceId_, tagId, val);
                if (!reader) {
                    LOG(INFO) << "Can't get tag reader of " << tagId;
                    iter->next();
                    continue;
                }
                // Generate 2.0 value and index records
                encodeVertexValue(partId, reader.get(), newSchema.get(),
                                  newKey, strVid, tagId, data);

                lastTagId = tagId;
                lastVertexId = vId;
            } else if (NebulaKeyUtilsV1::isEdge(key)) {
                auto svId = NebulaKeyUtilsV1::getSrcId(key);
                auto edgetype = NebulaKeyUtilsV1::getEdgeType(key);
                auto ranking = NebulaKeyUtilsV1::getRank(key);
                auto dvId = NebulaKeyUtilsV1::getDstId(key);
                auto version = NebulaKeyUtilsV1::getVersion(key);

                auto it = edgeSchemas_.find(std::abs(edgetype));
                if (it == edgeSchemas_.end()) {
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

                // auto strsvId = folly::to<std::string>(svId);
                // auto strdvId = folly::to<std::string>(dvId);
                auto strsvId = std::string(reinterpret_cast<const char*>(&svId), sizeof(svId));
                auto strdvId = std::string(reinterpret_cast<const char*>(&dvId), sizeof(dvId));
                if (!isValidVidLen(strsvId, strdvId)) {
                    iter->next();
                    continue;
                }

                auto newEdgeSchema = it->second.back();

                // Generate 2.0 key
                auto newKey = NebulaKeyUtils::edgeKey(spaceVidLen_,
                                                      partId,
                                                      strsvId,
                                                      edgetype,
                                                      ranking,
                                                      strdvId,
                                                      version);
                auto val = iter->val();
                auto reader = RowReaderWrapper::getEdgePropReader(schemaMan_.get(),
                                                                  spaceId_,
                                                                  std::abs(edgetype),
                                                                  val);
                if (!reader) {
                    LOG(INFO) << "Can't get edge reader of " << edgetype;
                    iter->next();
                    continue;
                }
                // Generate 2.0 value and index records
                encodeEdgeValue(partId, reader.get(), newEdgeSchema.get(), newKey, strsvId,
                                edgetype, ranking, strdvId, data);
                lastSrcVertexId = svId;
                lastEdgeType  = edgetype;
                lastRank = ranking;
                lastDstVertexId = dvId;
            }

            if (data.size() > 100) {
                LOG(INFO) << "Send record total rows " << data.size();
                auto code = engine_->multiPut(data);
                if (code != kvstore::ResultCode::SUCCEEDED) {
                    LOG(ERROR)  << "Write multi put failed！";
                }
                data.clear();
            }

            iter->next();
        }

        auto code = engine_->multiPut(data);
        if (code != kvstore::ResultCode::SUCCEEDED) {
            LOG(ERROR)  << "Write multi put failed！";
        }
        data.clear();
        LOG(INFO) << "Finish space id " << spaceId_ << " partId " <<  partId;
    }
}

void DbUpgrader::run() {
    // 1. Get all the directories in the data directory,
    // each directory name is spaceId, traverse each directory
    for (auto& entry : subDirs_) {
        // 2. determine whether the space exists,
        auto ret = initSpace(entry);
        if (!ret.ok()) {
            LOG(INFO) << "Init space  " << entry << " failed";
            continue;
        }

        // 3. Process all tags edges and the indexes on the space
        doProcessAllTagsAndEdges();
   }
}

}  // namespace storage
}  // namespace nebula
