/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "kvstore/Common.h"
#include "storage/StorageFlags.h"
#include "storage/admin/RebuildIndexTask.h"
#include "utils/OperationKeyUtils.h"

namespace nebula {
namespace storage {

ErrorOr<cpp2::ErrorCode, std::vector<AdminSubTask>>
RebuildIndexTask::genSubTasks() {
    CHECK_NOTNULL(env_->kvstore_);
    space_ = ctx_.spaceId_;
    auto parts = ctx_.parts_;
    auto parameters = ctx_.parameters_;
    auto indexID = std::stoi(parameters.task_specfic_paras[0]);
    bool isOffline = !strcasecmp("offline", parameters.task_specfic_paras[1].c_str());

    auto itemRet = getIndex(space_, indexID);
    if (!itemRet.ok()) {
        LOG(ERROR) << "Index Not Found";
        return cpp2::ErrorCode::E_INDEX_NOT_FOUND;
    }

    if (env_->rebuildIndexGuard_->find(space_) != env_->rebuildIndexGuard_->cend() &&
        env_->rebuildPartsGuard_->find(space_) != env_->rebuildPartsGuard_->cend()) {
        LOG(ERROR) << "Some index is rebuilding";
        return cpp2::ErrorCode::E_REBUILD_INDEX_FAILED;
    }

    auto result = env_->rebuildIndexGuard_->insert(space_, indexID);
    if (!result.second) {
        LOG(ERROR) << "RebuildIndexTask set failed";
        return cpp2::ErrorCode::E_REBUILD_INDEX_FAILED;
    }
    auto item = itemRet.value();
    auto schemaID = item->get_schema_id();

    if (isOffline) {
        LOG(INFO) << "Offline Rebuild Index Space: " << space_ << " Index: " << indexID;
    } else {
        LOG(INFO) << "Online Rebuild Index Space: " << space_ << " Index: " << indexID;
    }

    std::vector<AdminSubTask> tasks;
    for (const auto& part : parts) {
        std::function<kvstore::ResultCode()> task = std::bind(&RebuildIndexTask::genSubTask,
                                                              this, space_, part, schemaID,
                                                              indexID, item, isOffline);
        tasks.emplace_back(task);
    }
    return tasks;
}

kvstore::ResultCode RebuildIndexTask::genSubTask(GraphSpaceID space,
                                                 PartitionID part,
                                                 meta::cpp2::SchemaID schemaID,
                                                 int32_t indexID,
                                                 std::shared_ptr<meta::cpp2::IndexItem> item,
                                                 bool isOffline) {
    auto partIter = env_->rebuildPartsGuard_->find(space);
    if (partIter == env_->rebuildPartsGuard_->cend()) {
        std::unordered_set<PartitionID> parts;
        parts.emplace(part);
        env_->rebuildPartsGuard_->insert(space, std::move(parts));
    } else {
        auto parts = env_->rebuildPartsGuard_->at(space);
        parts.emplace(part);
    }

    LOG(INFO) << "Start building index";
    auto result = buildIndexGlobal(space, part, schemaID, indexID, item->get_fields());
    if (result != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Building index failed";
        return kvstore::ResultCode::E_BUILD_INDEX_FAILED;
    } else {
        LOG(INFO) << "Building index successful";
    }

    if (!isOffline) {
        LOG(INFO) << "Processing operation logs";
        result = buildIndexOnOperations(space, part);
        if (result != kvstore::ResultCode::SUCCEEDED) {
            LOG(ERROR) << "Building index with operation logs failed";
            return kvstore::ResultCode::E_INVALID_OPERATION;
        }
    }

    if (env_->rebuildIndexGuard_->find(space) != env_->rebuildIndexGuard_->cend() &&
        env_->rebuildPartsGuard_->find(space) != env_->rebuildPartsGuard_->cend()) {
        auto parts = env_->rebuildPartsGuard_->find(space)->second;
        parts.erase(part);
    }
    LOG(INFO) << "RebuildIndexTask Finished";
    return result;
}

kvstore::ResultCode RebuildIndexTask::buildIndexOnOperations(GraphSpaceID space,
                                                             PartitionID part) {
    if (canceled_) {
        LOG(ERROR) << "Rebuild index canceled";
        return kvstore::ResultCode::SUCCEEDED;
    }

    while (true) {
        std::vector<std::string> operations;
        operations.reserve(FLAGS_rebuild_index_batch_num);
        int32_t lastProcessedOperationsNum = 0;

        std::unique_ptr<kvstore::KVIterator> operationIter;
        auto operationPrefix = OperationKeyUtils::operationPrefix(part);
        auto operationRet = env_->kvstore_->prefix(space,
                                                   part,
                                                   operationPrefix,
                                                   &operationIter);
        if (operationRet != kvstore::ResultCode::SUCCEEDED) {
            LOG(ERROR) << "Processing Part " << part << " Failed";
            return operationRet;
        }

        while (operationIter->valid()) {
            lastProcessedOperationsNum += 1;
            auto opKey = operationIter->key();
            auto opVal = operationIter->val();
            // replay operation record
            if (OperationKeyUtils::isModifyOperation(opKey)) {
                auto key = OperationKeyUtils::getOperationKey(opKey);
                std::vector<kvstore::KV> pairs;
                pairs.emplace_back(std::move(key), "");
                VLOG(3) << "Processing delete operation " << opKey;
                auto ret = processModifyOperation(space, part,
                                                  std::move(pairs));
                if (kvstore::ResultCode::SUCCEEDED != ret) {
                    LOG(ERROR) << "Modify Playback Failed";
                    return ret;
                }
            } else if (OperationKeyUtils::isDeleteOperation(opKey)) {
                VLOG(3) << "Processing delete operation " << opVal;
                auto ret = processRemoveOperation(space, part,
                                                  opVal.str());
                if (kvstore::ResultCode::SUCCEEDED != ret) {
                    LOG(ERROR) << "Delete Playback Failed";
                    return ret;
                }
            } else {
                LOG(ERROR) << "Unknow Operation Type";
                return kvstore::ResultCode::E_INVALID_OPERATION;
            }

            operations.emplace_back(opKey);
            if (lastProcessedOperationsNum % FLAGS_rebuild_index_batch_num == 0) {
                auto ret = cleanupOperationLogs(space, part,
                                                std::move(operations));
                if (kvstore::ResultCode::SUCCEEDED != ret) {
                    LOG(ERROR) << "Delete Operation Failed";
                    return ret;
                }

                operations.reserve(FLAGS_rebuild_index_batch_num);
            }
            operationIter->next();
        }

        auto ret = cleanupOperationLogs(space, part,
                                        std::move(operations));
        if (kvstore::ResultCode::SUCCEEDED != ret) {
            LOG(ERROR) << "Delete Operation Failed";
            return ret;
        }
        if (lastProcessedOperationsNum == 0) {
            break;
        } else {
            lastProcessedOperationsNum = 0;
        }
    }
    return kvstore::ResultCode::SUCCEEDED;
}

kvstore::ResultCode
RebuildIndexTask::processModifyOperation(GraphSpaceID space,
                                         PartitionID part,
                                         std::vector<kvstore::KV>&& data) {
    folly::Baton<true, std::atomic> baton;
    kvstore::ResultCode result = kvstore::ResultCode::SUCCEEDED;
    env_->kvstore_->asyncMultiPut(space, part, std::move(data),
                                 [&result, &baton](kvstore::ResultCode code) {
        if (code != kvstore::ResultCode::SUCCEEDED) {
            LOG(ERROR) << "Modify the index data failed";
            result = code;
        }
        baton.post();
    });
    baton.wait();
    return result;
}

kvstore::ResultCode
RebuildIndexTask::processRemoveOperation(GraphSpaceID space,
                                         PartitionID part,
                                         std::string&& key) {
    folly::Baton<true, std::atomic> baton;
    kvstore::ResultCode result = kvstore::ResultCode::SUCCEEDED;
    env_->kvstore_->asyncRemove(space, part, std::move(key),
                               [&result, &baton](kvstore::ResultCode code) {
        if (code != kvstore::ResultCode::SUCCEEDED) {
            LOG(ERROR) << "Remove the operation log failed";
            result = code;
        }
        baton.post();
    });
    baton.wait();
    return result;
}

kvstore::ResultCode
RebuildIndexTask::cleanupOperationLogs(GraphSpaceID space,
                                       PartitionID part,
                                       std::vector<std::string>&& keys) {
    folly::Baton<true, std::atomic> baton;
    kvstore::ResultCode result = kvstore::ResultCode::SUCCEEDED;
    env_->kvstore_->asyncSingleRemove(space, part, std::move(keys),
                                    [&result, &baton](kvstore::ResultCode code) {
        if (code != kvstore::ResultCode::SUCCEEDED) {
            LOG(ERROR) << "Cleanup the operation log failed";
            result = code;
        }
        baton.post();
    });
    baton.wait();
    return result;
}

}  // namespace storage
}  // namespace nebula
