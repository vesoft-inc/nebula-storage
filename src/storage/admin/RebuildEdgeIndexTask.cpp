/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/StorageFlags.h"
#include "storage/index/IndexUtils.h"
#include "storage/admin/RebuildEdgeIndexTask.h"

namespace nebula {
namespace storage {

StatusOr<std::shared_ptr<nebula::meta::cpp2::IndexItem>>
RebuildEdgeIndexTask::getIndex(GraphSpaceID space, IndexID indexID) {
    return env_->indexMan_->getEdgeIndex(space, indexID);
}

kvstore::ResultCode
RebuildEdgeIndexTask::buildIndexGlobal(GraphSpaceID space,
                                       PartitionID part,
                                       meta::cpp2::SchemaID schemaID,
                                       IndexID indexID,
                                       const std::vector<meta::cpp2::ColumnDef>& cols) {
    std::unique_ptr<kvstore::KVIterator> iter;
    auto prefix = NebulaKeyUtils::prefix(part);
    auto ret = env_->kvstore_->prefix(space, part, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Processing Part " << part << " Failed";
        return ret;
    }

    auto vidSizeRet = env_->schemaMan_->getSpaceVidLen(space);
    if (!vidSizeRet.ok()) {
        LOG(ERROR) << "Get VID Size Failed";
        return kvstore::ResultCode::ERR_IO_ERROR;
    }

    auto vidSize = vidSizeRet.value();
    auto edgeType = schemaID.get_edge_type();
    std::vector<kvstore::KV> data;
    data.reserve(FLAGS_rebuild_index_batch_num);
    int32_t batchNum = 0;
    VertexID currentSrcVertex;
    VertexID currentDstVertex;

    while (iter && iter->valid()) {
        if (canceled_) {
            LOG(ERROR) << "Rebuild Edge Index is Canceled";
            return kvstore::ResultCode::ERR_IO_ERROR;
        }

        if (batchNum >= FLAGS_rebuild_index_batch_num) {
            auto result = saveJobStatus(space, part, std::move(data));
            if (result != kvstore::ResultCode::SUCCEEDED) {
                LOG(ERROR) << "Write Part " << part << " Index Failed";
                return kvstore::ResultCode::ERR_IO_ERROR;
            }
            data.reserve(FLAGS_rebuild_index_batch_num);
            batchNum = 0;
        }

        auto key = iter->key().str();
        if (!NebulaKeyUtils::isEdge(vidSize, key) ||
            NebulaKeyUtils::getEdgeType(vidSize, key) != edgeType) {
            iter->next();
            continue;
        }

        auto val = iter->val();
        auto source = NebulaKeyUtils::getSrcId(vidSize, key);
        auto destination = NebulaKeyUtils::getDstId(vidSize, key);
        if (currentSrcVertex == source && currentDstVertex == destination) {
            iter->next();
            continue;
        } else {
            currentSrcVertex = source.data();
            currentDstVertex = destination.data();
        }
        auto ranking = NebulaKeyUtils::getRank(vidSize, key);
        auto reader = RowReader::getEdgePropReader(env_->schemaMan_,
                                                   space,
                                                   edgeType,
                                                   std::move(val));
        if (reader == nullptr) {
            iter->next();
            continue;
        }

        std::vector<Value::Type> colsType;
        auto valuesRet = IndexUtils::collectIndexValues(reader.get(), cols, colsType);

        LOG(INFO) << "Part" << part << "Source " << source
                << " Rank " << ranking << " Des " << destination;
        auto indexKey = IndexKeyUtils::edgeIndexKey(vidSize,
                                                    part,
                                                    indexID,
                                                    source.data(),
                                                    ranking,
                                                    destination.data(),
                                                    valuesRet.value(),
                                                    colsType);
        data.emplace_back(std::move(indexKey), "");
        LOG(INFO) << "Append Edge Index";
        batchNum += 1;
        iter->next();
    }
    auto result = saveJobStatus(space, part, std::move(data));
    if (result != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Write Part " << part << " Index Failed";
        return kvstore::ResultCode::ERR_IO_ERROR;
    }

    return kvstore::ResultCode::SUCCEEDED;
}

}  // namespace storage
}  // namespace nebula
