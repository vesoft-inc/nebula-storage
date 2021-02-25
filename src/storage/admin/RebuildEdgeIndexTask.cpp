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
    return env_->indexMan_->getEdgeIndexes(space);
}

StatusOr<std::shared_ptr<meta::cpp2::IndexItem>>
RebuildEdgeIndexTask::getIndex(GraphSpaceID space, IndexID index) {
    return env_->indexMan_->getEdgeIndex(space, index);
}

kvstore::ResultCode RebuildEdgeIndexTask::buildIndexGlobal(GraphSpaceID space,
                                                           PartitionID part,
                                                           const IndexItems& items) {
    if (canceled_) {
        LOG(ERROR) << "Rebuild Edge Index is Canceled";
        return kvstore::ResultCode::SUCCEEDED;
    }

    auto vidSizeRet = env_->schemaMan_->getSpaceVidLen(space);
    if (!vidSizeRet.ok()) {
        LOG(ERROR) << "Get VID Size Failed";
        return kvstore::ResultCode::ERR_IO_ERROR;
    }

    std::unordered_set<EdgeType> edgeTypes;
    for (const auto& item : items) {
        edgeTypes.emplace(item->get_schema_id().get_edge_type());
    }

    auto vidSize = vidSizeRet.value();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto prefix = NebulaKeyUtils::edgePrefix(part);
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
    RowReaderWrapper reader;
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

        auto edgeType = NebulaKeyUtils::getEdgeType(vidSize, key);
        if (edgeType < 0) {
            iter->next();
            continue;
        }

        // Check whether this record contains the index of indexId
        if (edgeTypes.find(edgeType) == edgeTypes.end()) {
            VLOG(3) << "This record is not built index.";
            iter->next();
            continue;
        }

        auto source = NebulaKeyUtils::getSrcId(vidSize, key);
        auto destination = NebulaKeyUtils::getDstId(vidSize, key);
        auto ranking = NebulaKeyUtils::getRank(vidSize, key);
        VLOG(3) << "Source " << source << " Destination " << destination
                << " Ranking " << ranking << " Edge Type " << edgeType;
        if (currentSrcVertex == source &&
            currentDstVertex == destination &&
            currentRanking == ranking) {
            iter->next();
            continue;
        } else {
            currentSrcVertex = source.toString();
            currentDstVertex = destination.toString();
            currentRanking = ranking;
        }

        reader = RowReaderWrapper::getEdgePropReader(env_->schemaMan_, space, edgeType, val);
        if (reader == nullptr) {
            LOG(WARNING) << "Create edge property reader failed";
            iter->next();
            continue;
        }

        for (const auto& item : items) {
            if (item->get_schema_id().get_edge_type() == edgeType) {
                auto valuesRet =
                    IndexKeyUtils::collectIndexValues(reader.get(), item->get_fields());
                if (!valuesRet.ok()) {
                    LOG(WARNING) << "Collect index value failed";
                    continue;
                }
                auto indexKey = IndexKeyUtils::edgeIndexKey(vidSize,
                                                            part,
                                                            item->get_index_id(),
                                                            source.toString(),
                                                            ranking,
                                                            destination.toString(),
                                                            std::move(valuesRet).value());
                data.emplace_back(std::move(indexKey), "");
            }
        }
        iter->next();
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
