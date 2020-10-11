/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/StorageFlags.h"
#include "storage/admin/RebuildTagIndexTask.h"
#include "utils/IndexKeyUtils.h"
#include "codec/RowReaderWrapper.h"

namespace nebula {
namespace storage {

StatusOr<IndexItems>
RebuildTagIndexTask::getIndexes(GraphSpaceID space) {
    return env_->indexMan_->getTagIndexes(space, nebula::meta::cpp2::IndexType::ALL);
}


kvstore::ResultCode
RebuildTagIndexTask::buildIndexGlobal(GraphSpaceID space,
                                      PartitionID part,
                                      std::shared_ptr<meta::cpp2::IndexItem> item) {
    if (canceled_) {
        LOG(ERROR) << "Rebuild Tag Index is Canceled";
        return kvstore::ResultCode::SUCCEEDED;
    }

    auto vidSizeRet = env_->schemaMan_->getSpaceVidLen(space);
    if (!vidSizeRet.ok()) {
        LOG(ERROR) << "Get VID Size Failed";
        return kvstore::ResultCode::ERR_IO_ERROR;
    }

    auto vidSize = vidSizeRet.value();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto prefix = NebulaKeyUtils::partPrefix(part);
    auto ret = env_->kvstore_->prefix(space, part, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Processing Part " << part << " Failed";
        return ret;
    }

    VertexID currentVertex = "";
    std::vector<kvstore::KV> data;
    data.reserve(FLAGS_rebuild_index_batch_num);
    int64_t vertexCountOfPart = 0;
    RowReaderWrapper reader;
    auto indexType = item->get_index_type();
    auto indexId = item->get_index_id();

    while (iter && iter->valid()) {
        if (canceled_) {
            LOG(ERROR) << "Rebuild Tag Index is Canceled";
            return kvstore::ResultCode::SUCCEEDED;
        }

        if (static_cast<int32_t>(data.size()) == FLAGS_rebuild_index_batch_num) {
            auto result = writeData(space, part, data);
            if (result != kvstore::ResultCode::SUCCEEDED) {
                LOG(ERROR) << "Write Part " << part << " Index Failed";
                return result;
            }

            data.clear();
        }

        auto key = iter->key();
        auto val = iter->val();
        if (!NebulaKeyUtils::isVertex(vidSize, key)) {
            iter->next();
            continue;
        }

        auto tagID = NebulaKeyUtils::getTagId(vidSize, key);
        if (indexType == nebula::meta::cpp2::IndexType::NORMAL ||
            indexType == nebula::meta::cpp2::IndexType::VERTEX) {
            // Check whether this record contains the index of indexId
            if (item->get_schema_id().get_tag_id() != tagID) {
                VLOG(3) << "This record is not built index.";
                iter->next();
                continue;
            }
        }

        auto vertex = NebulaKeyUtils::getVertexId(vidSize, key).str();
        VLOG(3) << "Tag ID " << tagID << " Vertex ID " << vertex;
        if (currentVertex == vertex) {
            iter->next();
            continue;
        } else {
            currentVertex = vertex;
        }
        reader = RowReaderWrapper::getTagPropReader(env_->schemaMan_, space, tagID, val);
        if (reader == nullptr) {
            iter->next();
            continue;
        }

        // Distinguish different index
        if (indexType == nebula::meta::cpp2::IndexType::NORMAL) {
            std::vector<Value::Type> colsType;
            auto valuesRet = IndexKeyUtils::collectIndexValues(reader.get(),
                                                               item->get_fields(),
                                                               colsType);
            auto indexKey = IndexKeyUtils::vertexIndexKey(vidSize,
                                                         part,
                                                         indexId,
                                                         vertex,
                                                         valuesRet.value(),
                                                         std::move(colsType));
            data.emplace_back(std::move(indexKey), "");
        } else if (indexType == nebula::meta::cpp2::IndexType::VERTEX) {
            auto vertexIndexKey = StatisticsIndexKeyUtils::vertexIndexKey(vidSize,
                                                                          part,
                                                                          indexId,
                                                                          vertex);
            data.emplace_back(std::move(vertexIndexKey), "");
        } else if (indexType == nebula::meta::cpp2::IndexType::VERTEX_COUNT) {
            vertexCountOfPart++;
        } else {
            LOG(ERROR) << "Wrong index type " << static_cast<int32_t>(indexType)
                       << " indexId " << indexId;
            return kvstore::ResultCode::ERR_BUILD_INDEX_FAILED;
        }

        iter->next();
    }

    // Statistic all vertex index data,
    // Index data cannot be deleted during the rebuild index process,
    // when the number of vertex is 0, overwrite vertex count index data
    if (indexType == nebula::meta::cpp2::IndexType::VERTEX_COUNT) {
        auto vCountIndexKey = StatisticsIndexKeyUtils::countIndexKey(part, indexId);
        auto newCount = std::string(reinterpret_cast<const char*>(&vertexCountOfPart),
                                    sizeof(int64_t));
        data.emplace_back(std::move(vCountIndexKey), std::move(newCount));
    }

    auto result = writeData(space, part, std::move(data));
    if (result != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Write Part " << part << " Index Failed";
        return kvstore::ResultCode::ERR_IO_ERROR;
    }
    return kvstore::ResultCode::SUCCEEDED;
}

}  // namespace storage
}  // namespace nebula
