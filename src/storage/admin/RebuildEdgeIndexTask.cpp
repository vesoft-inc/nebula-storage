/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/StorageFlags.h"
#include "storage/admin/RebuildEdgeIndexTask.h"
#include "utils/IndexKeyUtils.h"
#include "codec/RowReaderWrapper.h"

namespace nebula {
namespace storage {

StatusOr<IndexItems>
RebuildEdgeIndexTask::getIndexes(GraphSpaceID space) {
    return env_->indexMan_->getEdgeIndexes(space, nebula::meta::cpp2::IndexType::ALL);
}

kvstore::ResultCode
RebuildEdgeIndexTask::buildIndexGlobal(GraphSpaceID space,
                                       PartitionID part,
                                       std::shared_ptr<meta::cpp2::IndexItem> item) {
    if (canceled_) {
        LOG(ERROR) << "Rebuild Edge Index is Canceled";
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

    VertexID currentSrcVertex = "";
    VertexID currentDstVertex = "";
    EdgeRanking currentRanking = 0;
    std::vector<kvstore::KV> data;
    data.reserve(FLAGS_rebuild_index_batch_num);
    int64_t edgeCountOfPart = 0;
    RowReaderWrapper reader;
    auto indexType = item->get_index_type();
    auto indexId = item->get_index_id();

    while (iter && iter->valid()) {
        if (canceled_) {
            LOG(ERROR) << "Rebuild Edge Index is Canceled";
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
        if (!NebulaKeyUtils::isEdge(vidSize, key)) {
            iter->next();
            continue;
        }

        auto edgeType = NebulaKeyUtils::getEdgeType(vidSize, key);
        if (edgeType < 0) {
            iter->next();
            continue;
        }

        if (indexType == nebula::meta::cpp2::IndexType::NORMAL ||
            indexType == nebula::meta::cpp2::IndexType::EDGE) {
            // Check whether this record contains the index of indexId
            if (item->get_schema_id().get_edge_type() != edgeType) {
                VLOG(3) << "This record is not built index.";
                iter->next();
                continue;
            }
        }

        auto source = NebulaKeyUtils::getSrcId(vidSize, key).str();
        auto destination = NebulaKeyUtils::getDstId(vidSize, key).str();
        auto ranking = NebulaKeyUtils::getRank(vidSize, key);
        VLOG(3) << "Source " << source << " Destination " << destination
                << " Ranking " << ranking << " Edge Type " << edgeType;
        if (currentSrcVertex == source &&
            currentDstVertex == destination &&
            currentRanking == ranking) {
            iter->next();
            continue;
        } else {
            currentSrcVertex = source.data();
            currentDstVertex = destination.data();
            currentRanking = ranking;
        }

        reader = RowReaderWrapper::getEdgePropReader(env_->schemaMan_, space, edgeType, val);
        if (reader == nullptr) {
            LOG(WARNING) << "Create edge property reader failed";
            iter->next();
            continue;
        }


        // Distinguish different index
        if (indexType == nebula::meta::cpp2::IndexType::NORMAL) {
            std::vector<Value::Type> colsType;
            auto valuesRet = IndexKeyUtils::collectIndexValues(reader.get(),
                                                               item->get_fields(),
                                                               colsType);
            auto indexKey = IndexKeyUtils::edgeIndexKey(vidSize,
                                                        part,
                                                        indexId,
                                                        source,
                                                        ranking,
                                                        destination,
                                                        valuesRet.value(),
                                                        std::move(colsType));
            data.emplace_back(std::move(indexKey), "");
        } else if (indexType == nebula::meta::cpp2::IndexType::EDGE) {
            auto eIndexKey = StatisticsIndexKeyUtils::edgeIndexKey(vidSize,
                                                                   part,
                                                                   indexId,
                                                                   source,
                                                                   ranking,
                                                                   destination);
            data.emplace_back(std::move(eIndexKey), "");
        } else if (indexType == nebula::meta::cpp2::IndexType::EDGE_COUNT) {
            edgeCountOfPart++;
        } else {
            LOG(ERROR) << "Wrong index type " << static_cast<int32_t>(indexType)
                       << " indexId " << indexId;
            return kvstore::ResultCode::ERR_BUILD_INDEX_FAILED;
        }

        iter->next();
    }

    // Statistic all edge index data,
    // Index data cannot be deleted during the rebuild index process,
    // when the number of edge is 0, overwrite edge count index data
    if (indexType == nebula::meta::cpp2::IndexType::EDGE_COUNT) {
        auto eCountIndexKey = StatisticsIndexKeyUtils::countIndexKey(part, indexId);
        auto newCount = std::string(reinterpret_cast<const char*>(&edgeCountOfPart),
                                    sizeof(int64_t));
        data.emplace_back(std::move(eCountIndexKey), std::move(newCount));
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
