/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "storage/mutate/UpdateVertexProcessor.h"
#include "common/NebulaKeyUtils.h"
#include "codec/RowWriterV2.h"
#include "kvstore/LogEncoder.h"

namespace nebula {
namespace storage {

void UpdateVertexProcessor::onProcessFinished() {
    Getters getters;
    getters.getSrcTagProp = [this] (const std::string& tagName,
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
kvstore::ResultCode UpdateVertexProcessor::processTagProps(
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
        ret = collectTagPropIfValid(iter->key(), iter->val(), partId, vId, tagId, props);
    } else if (insertable_ && updateTagIds_.find(tagId) != updateTagIds_.end()) {
        ret = insertTagProps(partId, vId, tagId, props);
    } else {
        VLOG(3) << "Missed partId " << partId << ", vId " << vId << ", tagId " << tagId;
        return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
    }
    return ret;
}

kvstore::ResultCode UpdateVertexProcessor::collectTagPropIfValid(
                        folly::StringPiece key,
                        folly::StringPiece value,
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
            if (insertable_ && updateTagIds_.find(tagId) != updateTagIds_.end()) {
                return insertTagProps(partId, vId, tagId, props);
            } else {
                return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
            }
        }
    }
    return collectTagProps(key, value, tagId, props);
}


// update newest version
kvstore::ResultCode UpdateVertexProcessor::collectTagProps(
                            folly::StringPiece key,
                            folly::StringPiece value,
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

    if (tagPropInsert_.find(tagId) == tagPropInsert_.end()) {
        tagPropInsert_.emplace(tagId, false);
    }

    // update
    if (updateTagIds_.find(tagId) != updateTagIds_.end()) {
        auto rowWriter = std::make_unique<RowWriterV2>(constSchema, value.str());
        tagUpdaters_[tagId] = std::make_pair(key.str(), rowWriter);
    }

    return kvstore::ResultCode::SUCCEEDED;
}

kvstore::ResultCode UpdateVertexProcessor::insertTagProps(const PartitionID partId,
                                                          const VertexID vId,
                                                          const TagID tagId,
                                                          const std::vector<PropContext>& props) {
    // here props is only tag prop
	insert_ = true;

    // newest scheam version
    const auto& constSchema = env_->schemaMan_->getTagSchema(spaceId_, tagId);
    if (constSchema == nullptr) {
        return kvstore::ResultCode::ERR_UNKNOWN;
    }

    // when insert, the condition as true
    for (auto& prop : props) {
        // first use default value, then use null value, last check whether is updated field
        if (prop.field_->hasDefault()) {
            tagFilters_.emplace(std::make_pair(tagId, prop.name_), prop.field_->defaultValue());
        } else if (prop.field_->nullable()) {
            tagFilters_.emplace(std::make_pair(tagId, prop.name_), NullType::__NULL__);
        } else {
            bool isUpdateProp = false;
            for (auto& updateProp : updatedVertexProps_) {
                auto toTagId = updateProp.get_tag_id();
                auto propName = updateProp.get_name();
                if (tagId == toTagId && !prop.name_.compare(propName)) {
                    isUpdateProp = true;
                    // value will update
                    tagFilters_.emplace(std::make_pair(tagId, prop.name_), NullType::__NULL__);
                    break;
                }
            }

            if (!isUpdateProp) {
                return kvstore::ResultCode::ERR_INVALID_FIELD_VALUE;
            }
        }

        auto it = tagPropInsert_.find(tagId);
        if (it == tagPropInsert_.end()) {
            tagPropInsert_.emplace(tagId, true);
        } else if (!it->second) {
            it->second = true;
        }
    }

    // value is emtpy
    auto rowWriter = std::make_unique<RowWriterV2>(constSchema.get());
    int64_t ms = time::WallClock::fastNowInMicroSec();
    auto now = std::numeric_limits<int64_t>::max() - ms;
    auto key = NebulaKeyUtils::vertexKey(spaceVidLen_, partId, vId, tagId, now);
    tagUpdaters_[tagId] = std::make_pair(std::move(key.str()), rowWriter);
    return kvstore::ResultCode::SUCCEEDED;
}

cpp2::ErrorCode UpdateVertexProcessor::checkFilter(const PartitionID partId, const VertexID vId) {
    // TODO QueryBaseProcessor::checkExp build tagContexts_
    for (auto& tc : tagContexts_) {
        VLOG(3) << "partId " << partId << ", vId " << vId
                << ", tagId " << tc.first << ", prop size " << tc.second.size();
        auto ret = processTagProps(partId, vId, tc.first, tc.second);
        if (ret == kvstore::ResultCode::ERR_CORRUPT_DATA) {
            return cpp2::ErrorCode::E_TAG_NOT_FOUND;
        } else if (ret != kvstore::ResultCode::SUCCEEDED) {
            return to(ret);
        }
    }

    Getters getters;
    getters.getSrcTagProp = [&, this] (const std::string& tagName,
                                       const std::string& prop) -> OptValue {
        auto tagRet = this->env_->schemaMan_->toTagID(this->spaceId_, tagName);
        if (!tagRet.ok()) {
            VLOG(1) << "Can't find tag " << tagName << ", in space " << this->spaceId_;
            return Status::Error("Invalid Filter Tag: " + tagName);
        }
        auto tagId = tagRet.value();
        auto it = this->tagFilters_.find(std::make_pair(tagId, prop));
        if (it == this->tagFilters_.end()) {
            return Status::Error("Invalid Tag Filter");
        }
        VLOG(1) << "Hit srcProp filter for tag: " << tagName
                << ", prop: " << prop;
        return it->second;
    };

    if (this->exp_ != nullptr) {
        // When insert, default where condition is true
        auto ret = this->isInsert(*(this->exp_->alias()));
        if (!ret.ok()) {
            return to(ret);;
        }
        if (ret.value()) {
            this->insert_ = true;
            return cpp2::ErrorCode::SUCCEEDED;
        }

        auto filterResult = this->exp_->eval(getters);
        if (!filterResult.ok()) {
            return cpp2::ErrorCode::E_INVALID_FILTER;
        }
        if (!Expression::asBool(filterResult.value())) {
            VLOG(1) << "Filter skips the update";
            return cpp2::ErrorCode::E_FILTER_OUT;
        }
    }
    return cpp2::ErrorCode::SUCCEEDED;
}


StatusOr<bool> UpdateVertexProcessor::isInsert(const std::string& tagName) {
    auto tagRet = this->env_->schemaMan_->toTagID(this->spaceId_, tagName);
    if (!tagRet.ok()) {
        VLOG(1) << "Can't find tag " << tagName << ", in space " << this->spaceId_;
        return Status::Error("Invalid Filter Tag: " + tagName);
    }
    auto tagId = tagRet.value();
    auto it = this->tagPropInsert_.find(tagId);
    if (it == this->tagPropInsert_.end()) {
        return Status::Error("Invalid Tag Filter");
    }
    return it->second;
}

std::string UpdateVertexProcessor::updateAndWriteBack(const PartitionID partId,
                                                      const VertexID vId) {
    UNUSED(partId);
    UNUSED(vId);
    Getters getters;
    getters.getSrcTagProp = [this] (const std::string& tagName,
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

    for (auto& updateProp : updatedVertexProps_) {
        auto tagId = updateProp.get_tag_id();
        auto propName = updateProp.get_name();
        auto exp = Expression::decode(updateProp.get_value());
        if (!exp.ok()) {
            return std::string("");
        }
        auto vexp = std::move(exp).value();
        vexp->setContext(this->expCtx_.get());
        auto value = vexp->eval(getters);
        if (!value.ok()) {
            return std::string("");
        }
        auto expValue = value.value();
        tagFilters_[std::make_pair(tagId, propName)] = expValue;

        // update old value -> new value
        auto wRet = tagUpdaters_[tagId].second->setValue(propName, expValue);
        if (wRet != WriteResult::SUCCEEDED) {
            VLOG(1) << "Add field faild ";
            return std::string("");
        }
    }

    std::unique_ptr<kvstore::BatchHolder> batchHolder = std::make_unique<kvstore::BatchHolder>();
    for (const auto& u : tagUpdaters_) {
        auto nKey = u.second.first;
        auto wRet = u.second.second->finish();
        if (wRet != WriteResult::SUCCEEDED) {
            VLOG(1) << "Add field faild ";
            return std::string("");
        }
        auto nVal = std::move(u.second.second->moveEncodedStr());

        /*
        if (!indexes_.empty()) {
            std::unique_ptr<RowReader> reader, oReader;
            for (auto &index : indexes_) {
                if (index->get_schema_id().get_tag_id() == u.first) {
                    if (!(u.second->kv.second.empty())) {
                        if (oReader == nullptr) {
                            oReader = RowReader::getTagPropReader(this->schemaMan_,
                                                                  u.second->kv.second,
                                                                  spaceId_,
                                                                  u.first);
                        }
                        const auto &oCols = index->get_fields();
                        auto oValues = collectIndexValues(oReader.get(), oCols);
                        auto oIndexKey = NebulaKeyUtils::vertexIndexKey(partId,
                                                                        index->index_id,
                                                                        vId,
                                                                        oValues);
                        batchHolder->remove(std::move(oIndexKey));
                    }
                    if (reader == nullptr) {
                        reader = RowReader::getTagPropReader(this->schemaMan_,
                                                             nVal,
                                                             spaceId_,
                                                             u.first);
                    }
                    const auto &cols = index->get_fields();
                    auto values = collectIndexValues(reader.get(), cols);
                    auto indexKey = NebulaKeyUtils::vertexIndexKey(partId,
                                                                   index->get_index_id(),
                                                                   vId,
                                                                   values);
                    batchHolder->put(std::move(indexKey), "");
                }
            }
        }
        */

        batchHolder->put(std::move(nKey), std::move(nVal));
    }
    return encodeBatchValue(batchHolder->getBatch());
}


cpp2::ErrorCode
UpdateVertexProcessor::checkAndBuildContexts(const cpp2::UpdateVertexRequest& req) {
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

    auto partId = req.get_part_id();
    auto vId = req.get_vertex_id();
    // Build context of the update vertex prop
    for (auto& vertexProp : updatedVertexProps_) {
        auto tagId = vertexProp.get_tag_id();

        auto tagName = env_->schemaMan_->toTagName(spaceId_, tagId);
        if (!tagName.ok()) {
            VLOG(1) << "Can't find spaceId " << spaceId_ << " tagId " << tagId;
            return cpp2::ErrorCode::E_TAG_NOT_FOUND;
        }

        SourcePropertyExpression sourcePropExp(new std::string(tagName.value()),
                                               new std::string(vertexProp.get_name()));
        sourcePropExp.setContext(expCtx_.get());
        auto status = sourcePropExp.prepare();
        if (!status.ok() || !checkExp(&sourcePropExp)) {
            return cpp2::ErrorCode::E_INVALID_UPDATER;
        }
        if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
            VLOG(3) << "Evict cache for vId " << vId << ", tagId " << tagId;
            vertexCache_->evict(std::make_pair(vId, tagId), partId);
        }
        updateTagIds_.emplace(tagId);
        auto exp = Expression::decode(vertexProp.get_value());
        if (!exp.ok()) {
            VLOG(1) << "Can't decode the prop's value " << vertexProp.get_value();
            return cpp2::ErrorCode::E_INVALID_UPDATER;
        }
        auto vexp = std::move(exp).value();
        vexp->setContext(expCtx_.get());
        status = vexp->prepare();
        if (!status.ok() || !checkExp(vexp.get())) {
            return cpp2::ErrorCode::E_INVALID_UPDATER;
        }
    }

    if (expCtx_->hasDstTagProp() || expCtx_->hasEdgeProp()
        || expCtx_->hasVariableProp() || expCtx_->hasInputProp()) {
        LOG(ERROR) << "should only contain SrcTagProp expression!";
        return cpp2::ErrorCode::E_INVALID_UPDATER;
    }

    // Have builded tagContexts_
    auto retCode = buildTagSchema();
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        return retCode;
    }

    buildTagTTLInfo();

    return cpp2::ErrorCode::SUCCEEDED;
}

cpp2::ErrorCode UpdateVertexProcessor::buildTagSchema() {
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

void UpdateVertexProcessor::process(const cpp2::UpdateVertexRequest& req) {
    spaceId_ = req.get_space_id();
    auto partId = req.get_part_id();
    auto vId = req.get_vertex_id();
    updatedVertexProps_ = req.get_updated_props();
    if (req.__isset.insertable) {
        insertable_ = *req.get_insertable();
    }

    auto retCode = getSpaceVidLen(spaceId_);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        pushResultCode(retCode, partId);
        onFinished();
        return;
    }

    if (!NebulaKeyUtils::isValidVidLen(spaceVidLen_, vId)) {
        LOG(ERROR) << "Space " << spaceId_ << ", vertex length invalid, "
                   << " space vid len: " << spaceVidLen_ << ",  vid is " << vId;
        pushResultCode(cpp2::ErrorCode::E_INVALID_VID, partId);
        onFinished();
        return;
    }

    retCode = checkAndBuildContexts(req);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Failure build contexts!";
        pushResultCode(retCode, partId);
        onFinished();
        return;
    }

    CHECK_NOTNULL(env_->indexMan_);
    auto iRet = env_->indexMan_->getTagIndexes(spaceId_);
    if (iRet.ok()) {
        indexes_ = std::move(iRet).value();
    }

    VLOG(3) << "Update vertex, spaceId: " << spaceId_
            << ", partId: " << partId << ", vId: " << vId;

    CHECK_NOTNULL(env_->kvstore_);
    auto atomic = [partId, vId, this] () -> folly::Optional<std::string> {
        filterResult_ = checkFilter(partId, vId);
        if (filterResult_ == cpp2::ErrorCode::SUCCEEDED) {
                return updateAndWriteBack(partId, vId);
            } else {
                return folly::none;
            }
    };

    auto callback = [this, partId, vId, req] (kvstore::ResultCode code) {
        while (true) {
            if (code == kvstore::ResultCode::SUCCEEDED) {
                onProcessFinished();
                break;
            }
            LOG(ERROR) << "Fail to update vertex, spaceId: " << this->spaceId_
                       << ", partId: " << partId << ", vId: " << vId;
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
