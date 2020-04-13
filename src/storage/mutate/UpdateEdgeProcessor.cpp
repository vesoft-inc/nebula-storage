/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/mutate/UpdateEdgeProcessor.h"
#include "common/NebulaKeyUtils.h"
#include "codec/RowWriterV2.h"
#include "kvstore/LogEncoder.h"

namespace nebula {
namespace storage {

void UpdateEdgeProcessor::onProcessFinished() {
    Getters getters;
    getters.getSrcTagProp = [&, this] (const std::string& tagName,
                                       const std::string& prop) -> OptValue {
        auto tagRet = this->env_->schemaMan_->toTagID(this->spaceId_, tagName);
        if (!tagRet.ok()) {
            VLOG(1) << "Can't find tag " << tagName << ", in space " << this->spaceId_;
            return Status::Error("Invalid Filter Tag: " + tagName);
        }
        auto tagId = tagRet.value();
        auto it = tagFilters_.find(std::make_pair(tagId, prop));
        if (it == tagFilters_.end()) {
            return Status::Error("Invalid Tag Filter");
        }
        VLOG(1) << "Hit srcProp filter for tag: " << tagName
                << ", prop: " << prop;
        return it->second;
    };

    getters.getAliasProp = [&, this] (const std::string&,
                                      const std::string& prop) -> OptValue {
        auto it = this->edgeFilters_.find(prop);
        if (it == this->edgeFilters_.end()) {
            return Status::Error("Invalid Edge Filter");
        }
        VLOG(1) << "Hit edgeProp for prop: " << prop;
        return it->second;
    };

    resultDataSet_.colNames.emplace_back("_inserted");
    nebula::Row row;
    row.columns.emplace_back(insert_);

    for (auto& exp : returnPropsExp_) {
        auto value = exp->eval(getters);
        if (!value.ok()) {
            LOG(ERROR) << value.status();
            return;
        }

        resultDataSet_.colNames.emplace_back(folly::stringPrintf("%s:%s", exp_->alias()->c_str(),
                                                                 exp_->prop()->c_str()));
        row.columns.emplace_back(std::move(value.value());
    }
    resultDataSet_.rows.emplace_back(std::move(row));
    resp_.set_props(std::move(resultDataSet_));
}

// update newest version
kvstore::ResultCode UpdateEdgeProcessor::processTagProps(
                            const PartitionID partId,
                            const VertexID vId,
                            const TagID tagId,
                            const std::vector<PropContext>& props) {
    // use key and value, so do not use vertexCache_
    auto prefix = NebulaKeyUtils::vertexPrefix(spaceVidLen_, partId, vId, tagId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env_->kvstore_->prefix(spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        VLOG(3) << "Error! ret = " << static_cast<int32_t>(ret)
                << ", spaceId " << spaceId_;
        return ret;
    }
    // newest value schema version
    if (iter && iter->valid()) {
        ret = collectTagPropIfValid(iter->val(), partId, vId, tagId, props);
    } else {
        VLOG(3) << "Missed partId " << partId << ", vId " << vId << ", tagId " << tagId;
        return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
    }
    return ret;
}

kvstore::ResultCode
UpdateEdgeProcessor::collectTagPropIfValid(folly::StringPiece value,
                                           const PartitionID partId,
                                           const VertexID vId,
                                           const TagID tagId,
                                           const std::vector<PropContext>& props) {
    auto reader = RowReader::getTagPropReader(this->env_->schemaMan_, spaceId_, tagId, value);
    if (!reader) {
        VLOG(1) << "Can't get tag reader of " << tagId;
        return kvstore::ResultCode::ERR_TAG_NOT_FOUND;
    }

    auto schemaIter = tagSchemas_.find(tagId);
    CHECK(schemaIter != tagSchemas_.end());
    const auto* schema = schemaIter->second.back().get();
    auto ttl = getTagTTLInfo(tagId);
    if (ttl.hasValue()) {
        auto ttlValue = ttl.value();
        if (checkDataExpiredForTTL(schema, reader.get(), ttlValue.first, ttlValue.second)) {
            return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
        }
    }
    return collectTagProps(value, tagId, props);
}

// update newest version
kvstore::ResultCode
UpdateEdgeProcessor::collectTagProps(folly::StringPiece value,
                                     const TagID tagId,
                                     const std::vector<PropContext>& props) {
    auto reader = RowReader::getTagPropReader(env_->schemaMan_,
                                              spaceId_,
                                              tagId,
                                              value);
    if (!reader) {
        VLOG(1) << "Can't get tag reader of " << tagId;
        return kvstore::ResultCode::ERR_TAG_NOT_FOUND;
    }

    const auto constSchema = reader->getSchema();
    for (auto& prop : props) {
        VLOG(2) << "Collect prop " << prop.name_ << ", type " << tagId;

        // read prop value
        auto retVal = readValue(reader.get(), prop);
        if (!retVal.ok()) {
            VLOG(1) << "Skip the bad value for tag: " << tagId
                    << ", prop " << prop.name_;
            return kvstore::ResultCode::ERR_TAG_PROP_NOT_FOUND;
        }

        // not only filter prop
        // if (prop.tagFiltered_) {
        tagFilters_.emplace(std::make_pair(tagId, prop.name_), std::move(retVal.value()));
    }
    return kvstore::ResultCode::SUCCEEDED;
}


kvstore::ResultCode UpdateEdgeProcessor::processEdgeProps(const PartitionID partId,
                                                          const cpp2::EdgeKey& edgeKey) {
    auto prefix = NebulaKeyUtils::prefix(spaceVidLen_, partId, edgeKey.src, edgeKey.edge_type,
                                         edgeKey.ranking, edgeKey.dst);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(this->spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        VLOG(3) << "Error! ret = " << static_cast<int32_t>(ret)
                << ", spaceId " << this->spaceId_;
        return ret;
    }
    // Only use the latest version
    if (iter && iter->valid()) {
        key_ = iter->key().str();
        val_ = iter->val().str();
        ret = collectEdgePropIfValid(partId, edgeKey);
    } else if (insertable_) {
        ret = insertEdgeProps(partId, edgeKey);
    } else {
        VLOG(3) << "Missed edge, spaceId: " << this->spaceId_ << ", partId: " << partId
                << ", src: " << edgeKey.src << ", type: " << edgeKey.edge_type
                << ", ranking: " << edgeKey.ranking << ", dst: "<< edgeKey.dst;
        return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
    }
    return ret;
}

kvstore::ResultCode
UpdateEdgeProcessor::collectEdgePropIfValid(const PartitionID partId,
                                            const cpp2::EdgeKey& edgeKey) {
    auto reader = RowReader::getEdgePropReader(this->env_->schemaMan_,
                                               this->spaceId_,
                                               std::abs(edgeKey.edge_type),
                                               val_);
    if (!reader) {
        VLOG(1) << "Can't get edge reader of " << edgeKey.edge_type;
        return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
    }

    auto schemaIter = edgeSchemas_.find(edgeKey.edge_type);
    CHECK(schemaIter != edgeSchemas_.end());
    const auto* schema = schemaIter->second.back().get();
    auto ttl = getEdgeTTLInfo();
    if (ttl.hasValue()) {
        auto ttlValue = ttl.value();
        if (checkDataExpiredForTTL(schema, reader.get(), ttlValue.first, ttlValue.second)) {
            if (insertable_) {
                return insertEdgeProps(partId, edgeKey);
            } else {
                return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
            }
        }
    }
    return collectEdgeProps(partId, edgeKey, reader.get());
}

kvstore::ResultCode
UpdateEdgeProcessor::collectEdgesProps(const PartitionID partId,
                                       const cpp2::EdgeKey& edgeKey,
                                       RowReader* rowReader) {
    const auto constSchema = reader->getSchema();
    for (auto index = 0UL; index < constSchema->getNumFields(); index++) {
        auto propName = std::string(constSchema->getFieldName(index));
        auto res = rowReader->getValueByName(propName);
        if (res = NullType::BAD_TYPE) {
            VLOG(1) << "Skip the bad edge value for prop " << propName;
            return kvstore::ResultCode::ERR_UNKNOWN;
        }
        edgeFilters_.emplace(propName, v);
    }

    // update
    rowWriter_ = std::unique_ptr<RowWriterV2>(constSchema, val_);
    return kvstore::ResultCode::SUCCEEDED;
}

kvstore::ResultCode
UpdateEdgeProcessor::insertEdgeProps(const PartitionID partId,
                                     const cpp2::EdgeKey& edgeKey,
                                     RowReader* rowReader) {
    insert_ = true;
    int64_t ms = time::WallClock::fastNowInMicroSec();
    auto now = std::numeric_limits<int64_t>::max() - ms;
    key_ = NebulaKeyUtils::edgeKey(spaceVidLen_, partId, edgeKey.src, edgeKey.edge_type,
                                   edgeKey.ranking, edgeKey.dst, now);
    const auto constSchema = this->env_->schemaMan_->getEdgeSchema(this->spaceId_,
                                                                   std::abs(edgeKey.edge_type));
    if (constSchema == nullptr) {
        return kvstore::ResultCode::ERR_UNKNOWN;
    }
    for (auto index = 0UL; index < constSchema->getNumFields(); index++) {
        auto propName = std::string(constSchema->getFieldName(index));
        // OptValue value = RowReader::getDefaultProp(constSchema.get(), propName);
        auto field = constSchema->field(propName);
        if (field == nullptr) {
            VLOG(1) << "filed not found " << propName;
            return kvstore::ResultCode::ERR_EDGE_PROP_NOT_FOUND;
        }

        // first use default value, then use null value, last check whether is updated field
        if (field->hasDefault()) {
            edgeFilters_.emplace(propName, field->defaultValue());
        } else if (field_->nullable()) {
            edgeFilters_.emplace(propName, field->NullType::__NULL__);
        } else {
            bool isUpdateProp = false;
            for (auto& updateProp : updatedEdgeProps_) {
                auto name = updateProp.get_name();
                if (!propName.compare(name)) {
                    isUpdateProp = true;
                    // value will update
                    edgeFilters_.emplace(propName, NullType::__NULL__);
                    break;
                }
            }

            if (!isUpdateProp) {
                return kvstore::ResultCode::ERR_INVALID_FIELD_VALUE;
            }
        }
    }

    rowWriter_ = std::unique_ptr<RowWriterV2>(constSchema.get());
    return kvstore::ResultCode::SUCCEEDED;
}


std::string UpdateEdgeProcessor::updateAndWriteBack(PartitionID partId,
                                                    const cpp2::EdgeKey& edgeKey) {
    Getters getters;
    getters.getSrcTagProp = [&, this] (const std::string& tagName,
                                       const std::string& prop) -> OptVariantType {
        auto tagRet = this->schemaMan_->toTagID(this->spaceId_, tagName);
        if (!tagRet.ok()) {
            VLOG(1) << "Can't find tag " << tagName << ", in space " << this->spaceId_;
            return Status::Error("Invalid Filter Tag: " + tagName);
        }
        auto tagId = tagRet.value();
        auto it = tagFilters_.find(std::make_pair(tagId, prop));
        if (it == tagFilters_.end()) {
            return Status::Error("Invalid Tag Filter");
        }
        VLOG(1) << "Hit srcProp filter for tag: " << tagName
                << ", prop: " << prop;
        return it->second;
    };

    getters.getAliasProp = [&, this] (const std::string&,
                                      const std::string& prop) -> OptVariantType {
        auto it = this->edgeFilters_.find(prop);
        if (it == this->edgeFilters_.end()) {
            return Status::Error("Invalid Edge Filter");
        }
        VLOG(1) << "Hit edgeProp for prop: " << prop << ", value: " << it->second;
        return it->second;
    };

    for (auto& updateProp : updateEdgeProps_) {
        auto propName = updateProp.get_name();
        auto exp = Expression::decode(updateProp.get_value());
        if (!exp.ok()) {
            LOG(ERROR) << "Decode item expr failed";
            return std::string("");
        }

        auto vexp = std::move(exp).value();
        vexp->setContext(this->expCtx_.get());
        auto value = vexp->eval(getters);
        if (!value.ok()) {
            LOG(ERROR) << "Eval item expr failed";
            return std::string("");
        }
        auto expValue = value.value();
        edgeFilters_[prop] = expValue;

        // update old value -> new value
        auto wRet = rowWriter_.setValue(propName, expValue);
        if (wRet != WriteResult::SUCCEEDED) {
            VLOG(1) << "Add field faild ";
            return std::string("");
        }
    }
    std::unique_ptr<kvstore::BatchHolder> batchHolder = std::make_unique<kvstore::BatchHolder>();
    auto wRet = rowWriter_->finish();
    if (wRet != WriteResult::SUCCEEDED) {
        VLOG(1) << "Add field faild ";
        return std::string("");
    }
    auto nVal = std::move(rowWriter_->moveEncodedStr());

    // TODO(heng) we don't update the index for reverse edge.
    if (!indexes_.empty() && edgeKey.edge_type > 0) {
        std::unique_ptr<RowReader> reader, rReader;
        for (auto& index : indexes_) {
            auto indexId = index->get_index_id();
            if (index->get_schema_id().get_edge_type() == edgeKey.edge_type) {
                if (!val_.empty()) {
                    if (rReader == nullptr) {
                        rReader = RowReader::getEdgePropReader(this->schemaMan_,
                                                               val_,
                                                               spaceId_,
                                                               edgeKey.edge_type);
                    }
                    auto rValues = collectIndexValues(rReader.get(),
                                                      index->get_fields());
                    auto rIndexKey = NebulaKeyUtils::edgeIndexKey(partId,
                                                                  indexId,
                                                                  edgeKey.src,
                                                                  edgeKey.ranking,
                                                                  edgeKey.dst,
                                                                  rValues);
                    batchHolder->remove(std::move(rIndexKey));
                }
                if (reader == nullptr) {
                    reader = RowReader::getEdgePropReader(this->schemaMan_,
                                                          nVal,
                                                          this->spaceId_,
                                                          edgeKey.edge_type);
                }

                auto values = collectIndexValues(reader.get(),
                                                 index->get_fields());
                auto indexKey = NebulaKeyUtils::edgeIndexKey(partId,
                                                             indexId,
                                                             edgeKey.src,
                                                             edgeKey.ranking,
                                                             edgeKey.dst,
                                                             values);
                batchHolder->put(std::move(indexKey), "");
            }
        }
    }
    batchHolder->put(std::move(key_), std::move(nVal));
    return encodeBatchValue(batchHolder->getBatch());
}


cpp2::ErrorCode UpdateEdgeProcessor::checkFilter(const PartitionID partId,
                                                 const cpp2::EdgeKey& edgeKey) {
    auto ret = processEdgesProps(partId, edgeKey);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        return FilterResult::E_ERROR;
    }
    for (auto& tc : tagContexts_) {
        VLOG(3) << "partId " << partId << ", vId " << edgeKey.src
                << ", tagId " << tc.tagId_ << ", prop size " << tc.props_.size();
        ret = processTagProps(partId, edgeKey.src, tc.tagId_, tc.props_);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return FilterResult::E_ERROR;
        }
    }

    Getters getters;
    getters.getSrcTagProp = [&, this] (const std::string& tagName,
                                       const std::string& prop) -> OptVariantType {
        auto tagRet = this->schemaMan_->toTagID(this->spaceId_, tagName);
        if (!tagRet.ok()) {
            VLOG(1) << "Can't find tag " << tagName << ", in space " << this->spaceId_;
            return Status::Error("Invalid Filter Tag: " + tagName);
        }
        auto tagId = tagRet.value();
        auto it = tagFilters_.find(std::make_pair(tagId, prop));
        if (it == tagFilters_.end()) {
            return Status::Error("Invalid Tag Filter");
        }
        VLOG(1) << "Hit srcProp filter for tag: " << tagName
                << ", prop: " << prop;
        return it->second;
    };

    getters.getAliasProp = [&, this] (const std::string&,
                                      const std::string& prop) -> OptVariantType {
        auto it = this->edgeFilters_.find(prop);
        if (it == this->edgeFilters_.end()) {
            return Status::Error("Invalid Edge Filter");
        }
        VLOG(1) << "Hit edgeProp for prop: " << prop;
        return it->second;
    };

    if (this->exp_ != nullptr) {
        // When insert, default where condition is true
        if (insert_) {
            return cpp2::ErrorCode::SUCCEEDED;
        }

        auto filterResult = this->exp_->eval(getters);
        if (!filterResult.ok()) {
            VLOG(1) << "Invalid filter expression";
            return cpp2::ErrorCode::E_INVALID_FILTER;
        }
        if (!Expression::asBool(filterResult.value())) {
            VLOG(1) << "Filter skips the update";
            return cpp2::ErrorCode::E_FILTER_OUT;
        }
    }
    return cpp2::ErrorCode::SUCCEEDED;
}


cpp2::ErrorCode
UpdateEdgeProcessor::checkAndBuildContexts(const cpp2::UpdateEdgeRequest& req) {
    // TODO expCtx_ is not implemented
    // TODO QueryBaseProcessor::checkExp to implement
    if (expCtx_ == nullptr) {
        expCtx_ = std::make_unique<ExpressionContext>();
    }
    // Return props
    if (req.__isset.return_props) {
        for (auto& prop : *req.get_return_props()) {
            auto colExpRet = Expression::decode(prop);
            if (!colExpRet.ok()) {
                return cpp2::ErrorCode::E_INVALID_UPDATER;
            }
            auto colExp = std::move(colExpRet).value();
            colExp->setContext(expCtx_.get());
            auto status = colExp->prepare();
            if (!status.ok() || !checkExp(colExp.get())) {
                return cpp2::ErrorCode::E_INVALID_UPDATER;
            }
           returnPropsExp_.emplace_back(std::move(colExp));
        }
    }

    // Condition(where/when)
    if (req.__isset.condition) {
        const auto& filterStr = *req.get_condition();
        if (!filterStr.empty()) {
            auto expRet = Expression::decode(filterStr);
            if (!expRet.ok()) {
                VLOG(1) << "Can't decode the filter " << filterStr;
                return cpp2::ErrorCode::E_INVALID_FILTER;
            }
            exp_ = std::move(expRet).value();
            exp_->setContext(expCtx_.get());
            auto status = exp_->prepare();
            if (!status.ok() || !checkExp(exp_.get())) {
                return cpp2::ErrorCode::E_INVALID_FILTER;
            }
        }
    }

    // build context of the update items
    for (auto& edgeProp : updatedEdgeProps_) {
        auto edgeName = env_->schemaMan_->toEdgeName(spaceId_, req.edge_key.edge_type);
        if (!edgeName.ok()) {
            VLOG(1) << "Can't find spaceId " << spaceId_ << " edgeType " << req.edge_key.edge_type;
            return cpp2::ErrorCode::E_EDGE_NOT_FOUND;
        }

        AliasPropertyExpression edgePropExp(new std::string(""),
                                            new std::string(edgeName.value()),
                                            new std::string(edgeProp.get_name()));
        edgePropExp.setContext(expCtx_.get());
        auto status = edgePropExp.prepare();
        if (!status.ok() || !checkExp(&edgePropExp)) {
            VLOG(1) << "Invalid item expression!";
            return cpp2::ErrorCode::E_INVALID_UPDATER;
        }
        auto exp = Expression::decode(updatedEdgeProp.get_value());
        if (!exp.ok()) {
            VLOG(1) << "Can't decode the prop's value " << updatedEdgeProp.get_value();
            return cpp2::ErrorCode::E_INVALID_UPDATER;
        }
        auto vexp = std::move(exp).value();
        vexp->setContext(expCtx_.get());
        status = vexp->prepare();
        if (!status.ok() || !checkExp(vexp.get())) {
            return cpp2::ErrorCode::E_INVALID_UPDATER;
        }
    }

    if (expCtx_->hasDstTagProp() || expCtx_->hasVariableProp()
        || expCtx_->hasInputProp()) {
        LOG(ERROR) << "should only contain SrcTagProp or EdgeProp expression!";
        return cpp2::ErrorCode::E_INVALID_UPDATER;
    }

    // TODO
    auto retCode = buildTagSchema();
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        return retCode;
    }
    retCode = buildEdgeSchema();
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        return retCode;
    }

    buildTagTTLInfo();
    buildEdgeTTLInfo();

    return cpp2::ErrorCode::SUCCEEDED;
}

cpp2::ErrorCode UpdateEdgeProcessor::buildTagSchema() {
    for (const auto& tc : tagContexts_) {
        auto tagId = tc.first;
        auto iter = tagSchemas_.find(tagId);
        if (iter == tagSchemas_.end()) {
            // Build tag schema, only contain newest version
            const auto constSchema = env_->schemaMan_->getTagSchema(spaceId_, tagId);
            if (constSchema == nullptr) {
                return cpp2::ErrorCode::E_TAG_NOT_FOUND;
            }
            tagSchemas_[tagId].push_back(constSchema);
        }
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

cpp2::ErrorCode UpdateEdgeProcessor::buildEdgeSchema() {
    for (const auto& edge : edgeContexts_) {
        auto edgeType = edge.first;
        auto iter = edgeSchemas_.find(edgeType);
        if (iter == edgeSchemas_.end()) {
            // Build edge schema, only contain newest version
            const auto constSchema = env_->schemaMan_->getEdgeSchema(spaceId_, edgeType);
            if (constSchema == nullptr) {
                return cpp2::ErrorCode::E_TAG_NOT_FOUND;
            }
            edgeSchemas_[edgeType].push_back(constSchema);
        }
    }
    return cpp2::ErrorCode::SUCCEEDED;
}


cpp2::ErrorCode UpdateEdgeProcessor::buildEdgeSchema() {
    for (const auto& edge : edgeContexts_) {
        auto edgeType = edge.first;
        auto iter = edgeSchemas_.find(edgeType);
        if (iter == edgeSchemas_.end()) {
            // Build edge schema, only contain newest version
            const auto constSchema = env_->schemaMan_->getEdgeSchema(spaceId_, edgeType);
            if (constSchema == nullptr) {
                return cpp2::ErrorCode::E_EDGE_NOT_FOUND;
            }
            edgeSchemas_[edgeType].push_back(constSchema);
        }
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

void UpdateEdgeProcessor::process(const cpp2::UpdateEdgeRequest& req) {
    spaceId_ = req.get_space_id();
    auto partId = req.get_part_id();
    auto edgeKey = req.get_edge_key();
    updatedEdgeProps_ = req.get_updated_props();
    if (req.__isset.insertable) {
        insertable_ = req.get_insertable();
    }

    auto retCode = getSpaceVidLen(spaceId_);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        pushResultCode(retCode, partId);
        onFinished();
        return;
    }

    if (!NebulaKeyUtils::isValidVidLen(spaceVidLen_, edgeKey.src, edgeKey.dst)) {
        LOG(ERROR) << "Space " << spaceId_ << ", vertex length invalid, "
                   << " space vid len: " << spaceVidLen_ << ",  edge srcVid: " << edgeKey.src
                   << " dstVid: " << edgeKey.dst;
        pushResultCode(cpp2::ErrorCode::E_INVALID_VID, partId);
        onFinished();
        return;
    }

    std::vector<EdgeType> eTypes;
    eTypes.emplace_back(edgeKey.get_edge_type());
    for (eType : eTypes) {
        std::vector<PropContext> prop;
        edgeContexts_.emplace(eType. std::move(prop));
    }

    auto retCode = checkAndBuildContexts(req);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Failure build contexts!";
        this->pushResultCode(retCode, partId);
        this->onFinished();
        return;
    }

    auto iRet = env_->indexMan_->getEdgeIndexes(spaceId_);
    if (iRet.ok()) {
        indexes_ = std::move(iRet).value();
    }

    VLOG(3) << "Update edge, spaceId: " << spaceId_ << ", partId:  " << partId
            << ", src: " << edgeKey.get_src() << ", edge_type: " << edgeKey.get_edge_type()
            << ", dst: " << edgeKey.get_dst() << ", ranking: " << edgeKey.get_ranking();
    CHECK_NOTNULL(env_->kvstore_);
    auto atomic = [partId, edgeKey, this] () -> folly::Optional<std::string> {
        filterResult_ = checkFilter(partId, edgeKey);
        if (filterResult_ == cpp2::ErrorCode::SUCCEEDED) {
            return updateAndWriteBack(partId, edgeKey);
        } else {
            return folly::none;
        }
    };

    auto callback = [this, partId, edgeKey, req] (kvstore::ResultCode code) {
        while (true) {
            if (code == kvstore::ResultCode::SUCCEEDED) {
                onProcessFinished();
                break;
            }
            LOG(ERROR) << "Fail to update edge, spaceId: " << this->spaceId_
                       << ", partId: " << partId
                       << ", src: " << edgeKey.get_src()
                       << ", edge_type: " << edgeKey.get_edge_type()
                       << ", dst: " << edgeKey.get_dst()
                       << ", ranking: " << edgeKey.get_ranking();
            if (code == kvstore::ResultCode::ERR_LEADER_CHANGED) {
                handleLeaderChanged(this->spaceId_, partId);
                break;
            }
            if (code == kvstore::ResultCode::ERR_ATOMIC_OP_FAILED) {
                if (filterResult_ == cpp2::ErrorCode::E_FILTER_OUT) {
                    onProcessFinished();
                }
                this->pushResultCode(filterResult_, partId);
            } else {
               this->pushResultCode(to(code), partId);
            }
            break;
        }
        this->onFinished();
    };
    env_->kvstore_->asyncAtomicOp(spaceId_, partId, atomic, callback);
}

}  // namespace storage
}  // namespace nebula
