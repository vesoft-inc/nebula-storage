/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/StorageFlags.h"
#include "storage/index/IndexUtils.h"
#include "storage/admin/RebuildTagIndexTask.h"

namespace nebula {
namespace storage {

StatusOr<std::shared_ptr<nebula::meta::cpp2::IndexItem>>
RebuildTagIndexTask::getIndex(GraphSpaceID space, IndexID indexID) {
    return env_->indexMan_->getTagIndex(space, indexID);
}


kvstore::ResultCode
RebuildTagIndexTask::buildIndexGlobal(GraphSpaceID space,
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
    auto tagID = schemaID.get_tag_id();
    std::vector<kvstore::KV> data;
    data.reserve(FLAGS_rebuild_index_batch_num);
    int32_t batchNum = 0;
    VertexID currentVertex;

    while (iter && iter->valid()) {
        if (canceled_) {
            LOG(ERROR) << "Rebuild Tag Index is Canceled";
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
        auto val = iter->val();
        if (!NebulaKeyUtils::isVertex(vidSize, key) ||
            NebulaKeyUtils::getTagId(vidSize, key) != tagID) {
            iter->next();
            continue;
        }

        auto vertex = NebulaKeyUtils::getVertexId(vidSize, key);
        if (currentVertex == vertex) {
            iter->next();
            continue;
        } else {
            currentVertex = vertex.data();
        }

        auto reader = RowReader::getTagPropReader(env_->schemaMan_,
                                                  space,
                                                  tagID,
                                                  std::move(val));
        if (reader == nullptr) {
            iter->next();
            continue;
        }

        std::vector<Value::Type> colsType;
        auto valuesRet = IndexUtils::collectIndexValues(reader.get(), cols, colsType);
        LOG(INFO) << "Part " << part << " vertex " << vertex.data();
        auto indexKey = IndexKeyUtils::vertexIndexKey(vidSize,
                                                      part,
                                                      indexID,
                                                      vertex.data(),
                                                      valuesRet.value());
        data.emplace_back(std::move(indexKey), "");
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
