/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_UPDATENODE_H_
#define STORAGE_EXEC_UPDATENODE_H_

#include "common/base/Base.h"
#include "common/expression/Expression.h"
#include "storage/context/StorageExpressionContext.h"
#include "storage/exec/TagNode.h"
#include "storage/exec/FilterNode.h"
#include "storage/StorageFlags.h"
#include "kvstore/LogEncoder.h"
#include "utils/OperationKeyUtils.h"

namespace nebula {
namespace storage {

template<typename T>
class UpdateNode  : public RelNode<T> {
public:
    UpdateNode(PlanContext* planCtx,
               std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>> indexes,
               std::vector<storage::cpp2::UpdatedProp>& updatedProps,
               FilterNode<T>* filterNode,
               bool insertable,
               std::vector<std::pair<std::string, std::unordered_set<std::string>>> depPropMap,
               StorageExpressionContext* expCtx,
               bool isEdge)
        : planContext_(planCtx)
        , indexes_(indexes)
        , updatedProps_(updatedProps)
        , filterNode_(filterNode)
        , insertable_(insertable)
        , depPropMap_(depPropMap)
        , expCtx_(expCtx)
        , isEdge_(isEdge) {}

    kvstore::ResultCode checkField(const meta::SchemaProviderIf::Field* field) {
        if (!field) {
            VLOG(1) << "Fail to read prop";
            if (isEdge_) {
                return kvstore::ResultCode::ERR_EDGE_PROP_NOT_FOUND;
            }
            return kvstore::ResultCode::ERR_TAG_PROP_NOT_FOUND;
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

    kvstore::ResultCode getDefaultOrNullValue(const meta::SchemaProviderIf::Field* field,
                                              const std::string& name) {
        if (field->hasDefault()) {
            props_[name] = field->defaultValue();
        } else if (field->nullable()) {
            props_[name] = Value::kNullValue;
        } else {
            return kvstore::ResultCode::ERR_INVALID_FIELD_VALUE;
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

    // Used for upsert tag/edge
    kvstore::ResultCode checkPropsAndGetDefaultValue() {
        // Store checked props
        // For example:
        // set a = 1, b = a + 1, c = 2,             `a` does not require default value and nullable
        // set a = 1, b = c + 1, c = 2,             `c` requires default value and nullable
        // set a = 1, b = (a + 1) + 1, c = 2,       support recursion multiple times
        // set a = 1, c = 2, b = (a + 1) + (c + 1)  support multiple properties
        std::unordered_set<std::string> checkedProp;
        // check depPropMap_ in set clause
        // this props must have default value or nullable, or set int UpdatedProp_
        for (auto& prop : depPropMap_) {
            for (auto& p :  prop.second) {
                auto it = checkedProp.find(p);
                if (it == checkedProp.end()) {
                    auto field = schema_->field(p);
                    auto ret = checkField(field);
                    if (ret != kvstore::ResultCode::SUCCEEDED) {
                        return ret;
                    }
                    ret = getDefaultOrNullValue(field, p);
                    if (ret != kvstore::ResultCode::SUCCEEDED) {
                        return ret;
                    }
                    checkedProp.emplace(p);
                }
            }

            // set field not need default value or nullable
            auto field = schema_->field(prop.first);
            auto ret = checkField(field);
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                return ret;
            }
            checkedProp.emplace(prop.first);
        }

        // props not in set clause must have default value or nullable
        auto fieldIter = schema_->begin();
        while (fieldIter) {
            auto propName = fieldIter->name();
            auto propIter = checkedProp.find(propName);
            if (propIter == checkedProp.end()) {
                auto ret = getDefaultOrNullValue(&(*fieldIter), propName);
                if (ret != kvstore::ResultCode::SUCCEEDED) {
                    return ret;
                }
            }
            ++fieldIter;
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

    folly::Optional<std::string> rebuildingModifyOp(GraphSpaceID spaceId,
                                                    PartitionID partId,
                                                    IndexID indexId,
                                                    std::string key,
                                                    kvstore::BatchHolder* batchHolder,
                                                    std::string val = "") {
        // Check the index is building for the specified partition or not.
        if (planContext_->env_->checkRebuilding(spaceId, partId, indexId)) {
            auto modifyOpKey = OperationKeyUtils::modifyOperationKey(partId, key);
            batchHolder->put(std::move(modifyOpKey), std::move(val));
        } else if (planContext_->env_->checkIndexLocked(spaceId, partId, indexId)) {
            LOG(ERROR) << "The index has been locked, index id:" << indexId;
            return folly::none;
        } else {
            batchHolder->put(std::move(key), std::move(val));
        }
        return std::string("");
    }

protected:
    // ============================ input =====================================================
    PlanContext                                                            *planContext_;
    std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>>             indexes_;
    // update <prop name, new value expression>
    std::vector<storage::cpp2::UpdatedProp>                                 updatedProps_;
    FilterNode<T>                                                          *filterNode_;
    // Whether to allow insert
    bool                                                                    insertable_{false};

    std::string                                                             key_;
    RowReader                                                              *reader_{nullptr};

    const meta::NebulaSchemaProvider                                       *schema_{nullptr};

    // use to save old row value
    std::string                                                             val_;
    std::unique_ptr<RowWriterV2>                                            rowWriter_;
    // prop -> value
    std::unordered_map<std::string, Value>                                  props_;
    std::atomic<kvstore::ResultCode>                                        exeResult_;

    // updatedProps_ dependent props in value expression
    std::vector<std::pair<std::string, std::unordered_set<std::string>>>    depPropMap_;

    StorageExpressionContext                                               *expCtx_;
    bool                                                                    isEdge_{false};
};

// Only use for update vertex
// Update records, write to kvstore
class UpdateTagNode : public UpdateNode<VertexID> {
public:
    using RelNode<VertexID>::execute;

    UpdateTagNode(PlanContext* planCtx,
                  std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>> indexes,
                  std::shared_ptr<nebula::meta::cpp2::IndexItem> allVertexStatIndex,
                  std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>> vertexIndexes,
                  std::unordered_map<TagID, IndexID> tagIdToIndexId,
                  std::vector<storage::cpp2::UpdatedProp>& updatedProps,
                  FilterNode<VertexID>* filterNode,
                  bool insertable,
                  std::vector<std::pair<std::string, std::unordered_set<std::string>>> depPropMap,
                  StorageExpressionContext* expCtx,
                  TagContext* tagContext)
        : UpdateNode<VertexID>(planCtx, indexes, updatedProps,
                               filterNode, insertable, depPropMap, expCtx, false)
        , allVertexStatIndex_(allVertexStatIndex)
        , vertexIndexes_(vertexIndexes)
        , tagIdToIndexId_(tagIdToIndexId)
        , tagContext_(tagContext) {
            tagId_ = planContext_->tagId_;
        }

    kvstore::ResultCode execute(PartitionID partId, const VertexID& vId) override {
        CHECK_NOTNULL(planContext_->env_->kvstore_);

        folly::Baton<true, std::atomic> baton;
        auto ret = kvstore::ResultCode::SUCCEEDED;
        planContext_->env_->kvstore_->asyncAtomicOp(planContext_->spaceId_, partId,
            [&partId, &vId, this] ()
            -> folly::Optional<std::string> {
                exeResult_ = RelNode::execute(partId, vId);

                if (exeResult_ == kvstore::ResultCode::SUCCEEDED) {
                    if (planContext_->resultStat_ == ResultStatus::ILLEGAL_DATA) {
                        exeResult_ = kvstore::ResultCode::ERR_INVALID_DATA;
                        return folly::none;
                    } else if (planContext_->resultStat_ ==  ResultStatus::FILTER_OUT) {
                        exeResult_ = kvstore::ResultCode::ERR_RESULT_FILTERED;
                        return folly::none;
                    }

                    if (filterNode_->valid()) {
                        this->reader_ = filterNode_->reader();
                    }
                    // reset StorageExpressionContext reader_ to nullptr
                    expCtx_->reset();

                    if (!reader_ && insertable_) {
                        exeResult_ = insertTagProps(partId, vId);
                    } else if (reader_) {
                        key_ = filterNode_->key().str();
                        exeResult_ = collTagProp();
                    } else {
                        exeResult_ = kvstore::ResultCode::ERR_KEY_NOT_FOUND;
                    }

                    if (exeResult_ != kvstore::ResultCode::SUCCEEDED) {
                        return folly::none;
                    }
                    return updateAndWriteBack(partId, vId);
                } else {
                    // if tagnode/edgenode error
                    return folly::none;
                }
            },
            [&ret, &baton, this] (kvstore::ResultCode code) {
                planContext_->env_->onFlyingRequest_.fetch_sub(1);
                if (code == kvstore::ResultCode::ERR_ATOMIC_OP_FAILED &&
                    exeResult_ != kvstore::ResultCode::SUCCEEDED) {
                    ret = exeResult_;
                } else {
                    ret = code;
                }
                baton.post();
            });
        baton.wait();

        return ret;
    }

    kvstore::ResultCode getLatestTagSchemaAndName() {
        auto schemaIter = tagContext_->schemas_.find(tagId_);
        if (schemaIter == tagContext_->schemas_.end() ||
            schemaIter->second.empty()) {
            return kvstore::ResultCode::ERR_TAG_NOT_FOUND;
        }
        schema_ = schemaIter->second.back().get();
        if (!schema_) {
            LOG(ERROR) << "Get nullptr schema";
            return kvstore::ResultCode::ERR_TAG_NOT_FOUND;
        }

        auto iter = tagContext_->tagNames_.find(tagId_);
        if (iter == tagContext_->tagNames_.end()) {
            VLOG(1) << "Can't find spaceId " << planContext_->spaceId_
                    << " tagId " << tagId_;
            return kvstore::ResultCode::ERR_TAG_NOT_FOUND;
        }
        tagName_ = iter->second;
        return kvstore::ResultCode::SUCCEEDED;
    }

    // Insert props row,
    // For insert, condition is always true,
    // Props must have default value or nullable, or set in UpdatedProp_
    kvstore::ResultCode insertTagProps(PartitionID partId, const VertexID& vId) {
        planContext_->insert_ = true;
        auto ret = getLatestTagSchemaAndName();
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }
        ret = checkPropsAndGetDefaultValue();
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        for (auto &e : props_) {
            expCtx_->setTagProp(tagName_, e.first, e.second);
        }

        // build key, value is emtpy
        auto version = FLAGS_enable_multi_versions ?
            std::numeric_limits<int64_t>::max() - time::WallClock::fastNowInMicroSec() : 0L;
        // Switch version to big-endian, make sure the key is in ordered.
        version = folly::Endian::big(version);
        key_ = NebulaKeyUtils::vertexKey(planContext_->vIdLen_,
                                         partId, vId, tagId_, version);
        rowWriter_ = std::make_unique<RowWriterV2>(schema_);

        return kvstore::ResultCode::SUCCEEDED;
    }

    // collect tag prop
    kvstore::ResultCode collTagProp() {
        auto ret = getLatestTagSchemaAndName();
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        for (auto index = 0UL; index < schema_->getNumFields(); index++) {
            auto propName = std::string(schema_->getFieldName(index));
            VLOG(1) << "Collect prop " << propName << ", type " << tagId_;

            // read prop value, If the RowReader contains this field,
            // read from the rowreader, otherwise read the default value
            // or null value from the latest schema
            auto retVal = QueryUtils::readValue(reader_, propName, schema_);
            if (!retVal.ok()) {
                VLOG(1) << "Bad value for tag: " << tagId_
                        << ", prop " << propName;
                return kvstore::ResultCode::ERR_TAG_PROP_NOT_FOUND;
            }
            props_[propName] = std::move(retVal.value());
        }

        for (auto &e : props_) {
            expCtx_->setTagProp(tagName_, e.first, e.second);
        }

        // After alter tag, the schema get from meta and the schema in RowReader
        // may be inconsistent, so the following method cannot be used
        // this->rowWriter_ = std::make_unique<RowWriterV2>(schema.get(), reader->getData());
        rowWriter_ = std::make_unique<RowWriterV2>(schema_);
        val_ = reader_->getData();
        return kvstore::ResultCode::SUCCEEDED;
    }

    folly::Optional<std::string>
    updateAndWriteBack(const PartitionID partId, const VertexID vId) {
        planContext_->env_->onFlyingRequest_.fetch_add(1);
        for (auto& updateProp : updatedProps_) {
            auto propName = updateProp.get_name();
            auto updateExp = Expression::decode(updateProp.get_value());
            if (!updateExp) {
                LOG(ERROR) << "Update expression decode failed " << updateProp.get_value();
                return folly::none;
            }
            auto updateVal = updateExp->eval(*expCtx_);
            // update prop value to props_
            props_[propName] = updateVal;
            // update expression context
            expCtx_->setTagProp(tagName_, propName, std::move(updateVal));
        }

        for (auto& e : props_) {
            auto wRet = rowWriter_->setValue(e.first, e.second);
            if (wRet != WriteResult::SUCCEEDED) {
                LOG(ERROR) << "Add field faild ";
                return folly::none;
            }
        }

        std::unique_ptr<kvstore::BatchHolder> batchHolder
            = std::make_unique<kvstore::BatchHolder>();

        auto wRet = rowWriter_->finish();
        if (wRet != WriteResult::SUCCEEDED) {
            LOG(ERROR) << "Add field faild ";
            return folly::none;
        }

        auto nVal = rowWriter_->moveEncodedStr();

        // update index if exists
        // Note: when insert_ is true, either there is no origin data or TTL expired
        // when there is no origin data, there is no the old index.
        // when TTL exists, there is no index.
        // TODO(pandasheep) support index when ttl exists
        // when insert_ is true, not old index, val_ is empty.
        if (!indexes_.empty() || !vertexIndexes_.empty() || allVertexStatIndex_ != nullptr) {
            // Normal index
            RowReaderWrapper nReader;
            for (auto& index : indexes_) {
                auto indexId = index->get_index_id();
                if (tagId_ == index->get_schema_id().get_tag_id()) {
                    /*
                     * step 1, delete old version normal index if exists.
                     */
                    if (!val_.empty()) {
                        if (!reader_) {
                            LOG(ERROR) << "Bad format row";
                            return folly::none;
                        }
                        auto oi = normalIndexKey(partId, vId, reader_, index);
                        if (!oi.empty()) {
                            if (planContext_->env_->checkRebuilding(planContext_->spaceId_,
                                                                    partId, indexId)) {
                                auto deleteOpKey = OperationKeyUtils::deleteOperationKey(partId);
                                batchHolder->put(std::move(deleteOpKey), std::move(oi));
                            } else if (planContext_->env_->checkIndexLocked(planContext_->spaceId_,
                                                                            partId, indexId)) {
                                LOG(ERROR) << "The index has been locked: "
                                           << index->get_index_name();
                                return folly::none;
                            } else {
                                batchHolder->remove(std::move(oi));
                            }
                        }
                    }

                    /*
                     * step 2, insert new vertex normal index
                     */
                    if (!nReader) {
                        nReader = RowReaderWrapper::getTagPropReader(
                            planContext_->env_->schemaMan_, planContext_->spaceId_, tagId_, nVal);
                    }
                    if (!nReader) {
                        LOG(ERROR) << "Bad format row";
                        return folly::none;
                    }
                    auto ni = normalIndexKey(partId, vId, nReader.get(), index);
                    if (!ni.empty()) {
                        auto retRebuild = rebuildingModifyOp(planContext_->spaceId_,
                                                             partId,
                                                             indexId,
                                                             ni,
                                                             batchHolder.get());
                        if (retRebuild == folly::none) {
                            return folly::none;
                        }
                    }
                }
            }

            /*
             * step 3, upsert vertex index data
             * Only allow to update/upsert a tag of a vertex,
             * Check whether the vertex index data exists, insert vertex index data if not exists
             */
            auto vIndexIt = tagIdToIndexId_.find(tagId_);
            if (vIndexIt != tagIdToIndexId_.end()) {
                auto indexId = vIndexIt->second;
                auto vIndexKey = StatisticsIndexKeyUtils::vertexIndexKey(planContext_->vIdLen_,
                                                                         partId,
                                                                         indexId,
                                                                         vId);
                if (!vIndexKey.empty()) {
                    std::string val;
                    auto ret = planContext_->env_->kvstore_->get(planContext_->spaceId_,
                                                                 partId,
                                                                 vIndexKey,
                                                                 &val);
                    if (ret != kvstore::ResultCode::SUCCEEDED) {
                        if (ret != kvstore::ResultCode::ERR_KEY_NOT_FOUND) {
                            LOG(ERROR) << "Get vertex index faild, index id " << indexId;
                            return folly::none;
                        } else {
                            auto retRebuild = rebuildingModifyOp(planContext_->spaceId_,
                                                                 partId,
                                                                 indexId,
                                                                 vIndexKey,
                                                                 batchHolder.get());
                            if (retRebuild == folly::none) {
                                return folly::none;
                            }
                        }
                    }
                }
            }

            /*
             * step 4, upsert statistic all vertex index data
             * First check vertex data exists, if not exists, update vertex count index
             */
            if (planContext_->insert_ && allVertexStatIndex_ != nullptr) {
                auto prefix = NebulaKeyUtils::vertexPrefix(planContext_->vIdLen_, partId, vId);
                std::unique_ptr<kvstore::KVIterator> iter;
                auto ret = planContext_->env_->kvstore_->prefix(planContext_->spaceId_,
                                                                partId, prefix, &iter);
                if (ret != kvstore::ResultCode::SUCCEEDED) {
                    LOG(ERROR) << "Error! ret = " << static_cast<int32_t>(ret)
                               << ", spaceId " << planContext_->spaceId_;
                    return folly::none;
                }
                bool isExist = false;
                while (iter && iter->valid()) {
                    auto key = iter->key();
                    if (!NebulaKeyUtils::isVertex(planContext_->vIdLen_, key)) {
                        iter->next();
                        continue;
                    }
                    isExist = true;
                    break;
                }

                // Add one when vertex not exist
                if (!isExist) {
                    auto vCountIndexId = allVertexStatIndex_->get_index_id();
                    int64_t countVal = 1;
                    std::string val;
                    auto vCountIndexKey = StatisticsIndexKeyUtils::countIndexKey(partId,
                                                                                 vCountIndexId);
                    ret = planContext_->env_->kvstore_->get(planContext_->spaceId_,
                                                            partId, vCountIndexKey, &val);
                    if (ret != kvstore::ResultCode::SUCCEEDED) {
                        if (ret != kvstore::ResultCode::ERR_KEY_NOT_FOUND) {
                            LOG(ERROR) << "Get all vertex count index error";
                            return folly::none;
                        }
                        // key does not exist
                    } else {
                        countVal += *reinterpret_cast<const int64_t*>(val.c_str());
                    }

                    if (!vCountIndexKey.empty()) {
                        auto newCount = std::string(reinterpret_cast<const char*>(&countVal),
                                                    sizeof(int64_t));
                        auto retRebuild = rebuildingModifyOp(planContext_->spaceId_,
                                                             partId,
                                                             vCountIndexId,
                                                             vCountIndexKey,
                                                             batchHolder.get(),
                                                             newCount);
                        if (retRebuild == folly::none) {
                            return folly::none;
                        }
                    }
                }
            }
        }
        // step 5, insert new vertex data
        batchHolder->put(std::move(key_), std::move(nVal));

        return encodeBatchValue(batchHolder->getBatch());
    }

    std::string normalIndexKey(PartitionID partId,
                               VertexID vId,
                               RowReader* reader,
                               std::shared_ptr<nebula::meta::cpp2::IndexItem> index) {
        std::vector<Value::Type> colsType;
        auto values = IndexKeyUtils::collectIndexValues(reader, index->get_fields(), colsType);
        if (!values.ok()) {
            return "";
        }
        return IndexKeyUtils::vertexIndexKey(planContext_->vIdLen_, partId, index->get_index_id(),
                                             vId, values.value(), colsType);
    }

private:
    // VERTEX_COUNT index, statistic all vertice count in current space
    std::shared_ptr<nebula::meta::cpp2::IndexItem>                     allVertexStatIndex_{nullptr};

    // VERTEX index, statistic all vertice in every tag of current space
    std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>>        vertexIndexes_;

    std::unordered_map<TagID, IndexID>                                 tagIdToIndexId_;

    TagContext                                                        *tagContext_;

    TagID                                                              tagId_;

    std::string                                                        tagName_;
};

// Only use for update edge
// Update records, write to kvstore
class UpdateEdgeNode : public UpdateNode<cpp2::EdgeKey> {
public:
    using RelNode<cpp2::EdgeKey>::execute;

    UpdateEdgeNode(PlanContext* planCtx,
                   std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>> indexes,
                   std::shared_ptr<nebula::meta::cpp2::IndexItem> allEdgeStatIndex,
                   std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>> edgeIndexes,
                   std::unordered_map<EdgeType, IndexID> edgeTypeToIndexId,
                   std::vector<storage::cpp2::UpdatedProp>& updatedProps,
                   FilterNode<cpp2::EdgeKey>* filterNode,
                   bool insertable,
                   std::vector<std::pair<std::string, std::unordered_set<std::string>>> depPropMap,
                   StorageExpressionContext* expCtx,
                   EdgeContext* edgeContext)
        : UpdateNode<cpp2::EdgeKey>(planCtx, indexes, updatedProps, filterNode, insertable,
                                    depPropMap, expCtx, true)
        , allEdgeStatIndex_(allEdgeStatIndex)
        , edgeIndexes_(edgeIndexes)
        , edgeTypeToIndexId_(edgeTypeToIndexId)
        , edgeContext_(edgeContext) {
            edgeType_ = planContext_->edgeType_;
        }

    kvstore::ResultCode execute(PartitionID partId, const cpp2::EdgeKey& edgeKey) override {
        CHECK_NOTNULL(planContext_->env_->kvstore_);

        folly::Baton<true, std::atomic> baton;
        auto ret = kvstore::ResultCode::SUCCEEDED;
        planContext_->env_->kvstore_->asyncAtomicOp(planContext_->spaceId_, partId,
            [&partId, &edgeKey, this] ()
            -> folly::Optional<std::string> {
                exeResult_ = RelNode::execute(partId, edgeKey);
                if (exeResult_ == kvstore::ResultCode::SUCCEEDED) {
                    if (edgeKey.edge_type != edgeType_) {
                        exeResult_ = kvstore::ResultCode::ERR_KEY_NOT_FOUND;
                        return folly::none;
                    }
                    if (planContext_->resultStat_ == ResultStatus::ILLEGAL_DATA) {
                        exeResult_ = kvstore::ResultCode::ERR_INVALID_DATA;
                        return folly::none;
                    } else if (planContext_->resultStat_ ==  ResultStatus::FILTER_OUT) {
                        exeResult_ = kvstore::ResultCode::ERR_RESULT_FILTERED;
                        return folly::none;
                    }

                    if (filterNode_->valid()) {
                        reader_ = filterNode_->reader();
                    }
                    // reset StorageExpressionContext reader_ to nullptr
                    expCtx_->reset();

                    if (!reader_ && insertable_) {
                        exeResult_ = insertEdgeProps(partId, edgeKey);
                    } else if (reader_) {
                        key_ = filterNode_->key().str();
                        exeResult_ = collEdgeProp(edgeKey);
                    } else {
                        exeResult_ = kvstore::ResultCode::ERR_KEY_NOT_FOUND;
                    }

                    if (exeResult_ != kvstore::ResultCode::SUCCEEDED) {
                        return folly::none;
                    }
                    return updateAndWriteBack(partId, edgeKey);
                } else {
                    // If filter out, StorageExpressionContext is set in filterNode
                    return folly::none;
                }
            },
            [&ret, &baton, this] (kvstore::ResultCode code) {
                planContext_->env_->onFlyingRequest_.fetch_sub(1);
                if (code == kvstore::ResultCode::ERR_ATOMIC_OP_FAILED &&
                    exeResult_ != kvstore::ResultCode::SUCCEEDED) {
                    ret = exeResult_;
                } else {
                    ret = code;
                }
                baton.post();
            });
        baton.wait();

        return ret;
    }

    kvstore::ResultCode getLatestEdgeSchemaAndName() {
        auto schemaIter = edgeContext_->schemas_.find(std::abs(edgeType_));
        if (schemaIter == edgeContext_->schemas_.end() ||
            schemaIter->second.empty()) {
            return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
        }
        schema_ = schemaIter->second.back().get();
        if (!schema_) {
            LOG(ERROR) << "Get nullptr schema";
            return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
        }

        auto iter = edgeContext_->edgeNames_.find(edgeType_);
        if (iter == edgeContext_->edgeNames_.end()) {
            VLOG(1) << "Can't find spaceId " << planContext_->spaceId_
                    << " edgeType " << edgeType_;
            return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
        }
        edgeName_ = iter->second;
        return kvstore::ResultCode::SUCCEEDED;
    }

    // Insert props row,
    // Operator props must have default value or nullable, or set in UpdatedProp_
    kvstore::ResultCode insertEdgeProps(const PartitionID partId, const cpp2::EdgeKey& edgeKey) {
        planContext_->insert_ = true;
        auto ret = getLatestEdgeSchemaAndName();
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        ret = checkPropsAndGetDefaultValue();
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        // build expression context
        // add _src, _type, _rank, _dst
        expCtx_->setEdgeProp(edgeName_, kSrc, edgeKey.src);
        expCtx_->setEdgeProp(edgeName_, kDst, edgeKey.dst);
        expCtx_->setEdgeProp(edgeName_, kRank, edgeKey.ranking);
        expCtx_->setEdgeProp(edgeName_, kType, edgeKey.edge_type);

        for (auto &e : props_) {
            expCtx_->setEdgeProp(edgeName_, e.first, e.second);
        }

        // build key, value is emtpy
        auto version = FLAGS_enable_multi_versions ?
            std::numeric_limits<int64_t>::max() - time::WallClock::fastNowInMicroSec() : 0L;
        // Switch version to big-endian, make sure the key is in ordered.
        version = folly::Endian::big(version);
        key_ = NebulaKeyUtils::edgeKey(planContext_->vIdLen_,
                                       partId,
                                       edgeKey.src.getStr(),
                                       edgeKey.edge_type,
                                       edgeKey.ranking,
                                       edgeKey.dst.getStr(),
                                       version);
        rowWriter_ = std::make_unique<RowWriterV2>(schema_);

        return kvstore::ResultCode::SUCCEEDED;
    }

    // Collect edge prop
    kvstore::ResultCode collEdgeProp(const cpp2::EdgeKey& edgeKey) {
        auto ret = getLatestEdgeSchemaAndName();
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        for (auto index = 0UL; index < schema_->getNumFields(); index++) {
            auto propName = std::string(schema_->getFieldName(index));
            VLOG(1) << "Collect prop " << propName << ", edgeType " << edgeType_;

            // Read prop value, If the RowReader contains this field,
            // read from the rowreader, otherwise read the default value
            // or null value from the latest schema
            auto retVal = QueryUtils::readValue(reader_, propName, schema_);
            if (!retVal.ok()) {
                VLOG(1) << "Bad value for edge: " << edgeType_
                        << ", prop " << propName;
                return kvstore::ResultCode::ERR_EDGE_PROP_NOT_FOUND;
            }
            props_[propName] = std::move(retVal.value());
        }

        // build expression context
        // add _src, _type, _rank, _dst
        expCtx_->setEdgeProp(edgeName_, kSrc, edgeKey.src);
        expCtx_->setEdgeProp(edgeName_, kDst, edgeKey.dst);
        expCtx_->setEdgeProp(edgeName_, kRank, edgeKey.ranking);
        expCtx_->setEdgeProp(edgeName_, kType, edgeKey.edge_type);

        for (auto &e : props_) {
            expCtx_->setEdgeProp(edgeName_, e.first, e.second);
        }

        // After alter edge, the schema get from meta and the schema in RowReader
        // may be inconsistent, so the following method cannot be used
        // rowWriter_ = std::make_unique<RowWriterV2>(schema.get(), reader->getData());
        rowWriter_ = std::make_unique<RowWriterV2>(schema_);
        val_ = reader_->getData();
        return kvstore::ResultCode::SUCCEEDED;
    }

    folly::Optional<std::string>
    updateAndWriteBack(const PartitionID partId, const cpp2::EdgeKey& edgeKey) {
        planContext_->env_->onFlyingRequest_.fetch_add(1);
        for (auto& updateProp : updatedProps_) {
            auto propName = updateProp.get_name();
            auto updateExp = Expression::decode(updateProp.get_value());
            if (!updateExp) {
                return folly::none;
            }
            auto updateVal = updateExp->eval(*expCtx_);
            // update prop value to updateContext_
            props_[propName] = updateVal;
            // update expression context
            expCtx_->setEdgeProp(edgeName_, propName, std::move(updateVal));
        }

        for (auto& e : props_) {
            auto wRet = rowWriter_->setValue(e.first, e.second);
            if (wRet != WriteResult::SUCCEEDED) {
                VLOG(1) << "Add field faild ";
                return folly::none;
            }
        }

        std::unique_ptr<kvstore::BatchHolder> batchHolder
            = std::make_unique<kvstore::BatchHolder>();

        auto wRet = rowWriter_->finish();
        if (wRet != WriteResult::SUCCEEDED) {
            VLOG(1) << "Add field faild ";
            return folly::none;
        }

        auto nVal = rowWriter_->moveEncodedStr();

        // update index if exists
        // Note: when insert_ is true, either there is no origin data or TTL expired
        // when there is no origin data, there is no the old index.
        // when TTL exists, there is no index.
        // TODO(pandasheep) support index when ttl exists
        // when insert_ is true, not old index, val_ is empty.
        if (!indexes_.empty() || !edgeIndexes_.empty() || allEdgeStatIndex_ != nullptr) {
            // Normal index
            RowReaderWrapper nReader;
            for (auto& index : indexes_) {
                auto indexId = index->get_index_id();
                if (edgeType_ == index->get_schema_id().get_edge_type()) {
                    /*
                     * step 1, delete old version normal index if exists.
                     */
                    if (!val_.empty()) {
                        if (!reader_) {
                            LOG(ERROR) << "Bad format row";
                            return folly::none;
                        }
                        auto oi = normalIndexKey(partId, reader_, edgeKey, index);
                        if (!oi.empty()) {
                            if (planContext_->env_->checkRebuilding(planContext_->spaceId_,
                                                                    partId, indexId)) {
                                auto deleteOpKey = OperationKeyUtils::deleteOperationKey(partId);
                                batchHolder->put(std::move(deleteOpKey), std::move(oi));
                            } else if (planContext_->env_->checkIndexLocked(planContext_->spaceId_,
                                                                            partId, indexId)) {
                                LOG(ERROR) << "The index has been locked: "
                                           << index->get_index_name();
                                return folly::none;
                            } else {
                                batchHolder->remove(std::move(oi));
                            }
                        }
                    }

                    /*
                     * step 2, insert new edge normal index
                     */
                    if (!nReader) {
                        nReader = RowReaderWrapper::getEdgePropReader(
                            planContext_->env_->schemaMan_, planContext_->spaceId_,
                            std::abs(edgeType_), nVal);
                    }
                    if (!nReader) {
                        LOG(ERROR) << "Bad format row";
                        return folly::none;
                    }
                    auto ni = normalIndexKey(partId, nReader.get(), edgeKey, index);
                    if (!ni.empty()) {
                        auto retRebuild = rebuildingModifyOp(planContext_->spaceId_,
                                                             partId,
                                                             indexId,
                                                             ni,
                                                             batchHolder.get());
                        if (retRebuild == folly::none) {
                            return folly::none;
                        }
                    }
                }
            }

            /*
             * step 3, upsert edge index data
             * Check whether the edge index data exists, insert edge index data if not exists
             */
            auto eIndexIt = edgeTypeToIndexId_.find(edgeType_);
            if (eIndexIt != edgeTypeToIndexId_.end()) {
                auto eIndexId = eIndexIt->second;
                auto eIndexKey = StatisticsIndexKeyUtils::edgeIndexKey(planContext_->vIdLen_,
                                                                       partId,
                                                                       eIndexId,
                                                                       edgeKey.get_src().getStr(),
                                                                       edgeType_,
                                                                       edgeKey.get_ranking(),
                                                                       edgeKey.get_dst().getStr());
                if (!eIndexKey.empty()) {
                    std::string val;
                    auto ret = planContext_->env_->kvstore_->get(planContext_->spaceId_,
                                                                 partId,
                                                                 eIndexKey,
                                                                 &val);
                    if (ret != kvstore::ResultCode::SUCCEEDED) {
                        if (ret != kvstore::ResultCode::ERR_KEY_NOT_FOUND) {
                            LOG(ERROR) << "Get edge index faild, index id " << eIndexId;
                            return folly::none;
                        } else {
                            auto retRebuild = rebuildingModifyOp(planContext_->spaceId_,
                                                                 partId,
                                                                 eIndexId,
                                                                 eIndexKey,
                                                                 batchHolder.get());
                            if (retRebuild == folly::none) {
                                return folly::none;
                            }
                        }
                    }
                }
            }

            /*
             * Step 4, upsert statistic all edge index data
             * First check edge data exists, if not exists, update edge count index
             * For edge, if planContext_->insert_ is true, edge count add one
             */
            if (planContext_->insert_ && allEdgeStatIndex_ != nullptr) {
                auto eCountIndexId = allEdgeStatIndex_->get_index_id();
                int64_t countVal = 1;
                std::string val;
                auto eCountIndexKey = StatisticsIndexKeyUtils::countIndexKey(partId,
                                                                             eCountIndexId);
                auto ret = planContext_->env_->kvstore_->get(planContext_->spaceId_,
                                                             partId, eCountIndexKey, &val);
                if (ret != kvstore::ResultCode::SUCCEEDED) {
                    if (ret != kvstore::ResultCode::ERR_KEY_NOT_FOUND) {
                        LOG(ERROR) << "Get statistics index error";
                        return folly::none;
                    }
                    // key does not exist
                } else {
                    countVal += *reinterpret_cast<const int64_t*>(val.c_str());
                }

                if (!eCountIndexKey.empty()) {
                    auto newCount = std::string(reinterpret_cast<const char*>(&countVal),
                                                sizeof(int64_t));
                    auto retRebuild = rebuildingModifyOp(planContext_->spaceId_,
                                                         partId,
                                                         eCountIndexId,
                                                         eCountIndexKey,
                                                         batchHolder.get(),
                                                         newCount);
                    if (retRebuild == folly::none) {
                        return folly::none;
                    }
                }
            }
        }

        /*
         * step 5, insert new edge data
         */
        batchHolder->put(std::move(key_), std::move(nVal));
        return encodeBatchValue(batchHolder->getBatch());
    }

    std::string normalIndexKey(PartitionID partId,
                               RowReader* reader,
                               const cpp2::EdgeKey& edgeKey,
                               std::shared_ptr<nebula::meta::cpp2::IndexItem> index) {
        std::vector<Value::Type> colsType;
        auto values = IndexKeyUtils::collectIndexValues(reader, index->get_fields(), colsType);
        if (!values.ok()) {
            return "";
        }
        return IndexKeyUtils::edgeIndexKey(planContext_->vIdLen_,
                                           partId,
                                           index->get_index_id(),
                                           edgeKey.get_src().getStr(),
                                           edgeKey.get_ranking(),
                                           edgeKey.get_dst().getStr(),
                                           values.value(),
                                           colsType);
    }

private:
    // EDGE_COUNT index, statistic all edge count ont current space
    std::shared_ptr<nebula::meta::cpp2::IndexItem>              allEdgeStatIndex_{nullptr};

    // EDGE index, statistic all edge in every edgetype of current space
    std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>> edgeIndexes_;

    std::unordered_map<EdgeType, IndexID>                       edgeTypeToIndexId_;

    EdgeContext                                                *edgeContext_;
    EdgeType                                                    edgeType_;
    std::string                                                 edgeName_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_FILTERNODE_H_
