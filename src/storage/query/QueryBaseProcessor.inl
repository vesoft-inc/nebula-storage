/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

DECLARE_int32(max_handlers_per_req);
DECLARE_int32(min_vertices_per_bucket);
DECLARE_int32(max_edge_returned_per_vertex);
DECLARE_bool(enable_vertex_cache);
DECLARE_bool(enable_reservoir_sampling);

namespace nebula {
namespace storage {

template<typename REQ, typename RESP>
cpp2::ErrorCode QueryBaseProcessor<REQ, RESP>::handleVertexProps(
        const std::vector<ReturnProp>& vertexProps) {
    for (size_t i = 0; i < vertexProps.size(); i++) {
        auto tagId = vertexProps[i].entryId_;
        auto tagSchema = this->env_->schemaMan_->getTagSchema(spaceId_, tagId);
        if (!tagSchema) {
            VLOG(1) << "Can't find spaceId " << spaceId_ << " tagId " << tagId;
            return cpp2::ErrorCode::E_TAG_NOT_FOUND;
        }
        auto tagName = this->env_->schemaMan_->toTagName(spaceId_, tagId);
        if (!tagName.ok()) {
            VLOG(1) << "Can't find spaceId " << spaceId_ << " tagId " << tagId;
            return cpp2::ErrorCode::E_TAG_NOT_FOUND;
        }
        resultDataSet_.colNames.emplace_back(
            folly::stringPrintf("%d:%s", tagId, tagName.value().c_str()));

        std::vector<PropContext> ctxs;
        auto& props = vertexProps[i].names_;
        for (size_t propIdx = 0; propIdx < props.size(); propIdx++) {
            auto& name = props[propIdx];
            auto field = tagSchema->field(name);
            if (field == nullptr) {
                VLOG(1) << "Can't find prop " << name << " tagId " << tagId;
                return cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND;
            }

            PropContext ctx(name.c_str());
            ctx.returned_ = true;
            ctx.field_ = field;
            ctxs.emplace_back(std::move(ctx));
        }
        tagContexts_.emplace_back(tagId, std::move(ctxs));
        tagIndexMap_.emplace(tagId, tagContexts_.size() - 1);
        tagSchemas_.emplace(tagId, std::move(tagSchema));
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

template<typename REQ, typename RESP>
cpp2::ErrorCode QueryBaseProcessor<REQ, RESP>::handleEdgeProps(
        const std::vector<ReturnProp>& edgeProps) {
    for (size_t i = 0; i < edgeProps.size(); i++) {
        auto edgeType = edgeProps[i].entryId_;
        auto edgeSchema = this->env_->schemaMan_->getEdgeSchema(spaceId_, std::abs(edgeType));
        if (!edgeSchema) {
            VLOG(1) << "Can't find spaceId " << spaceId_ << " edgeType " << edgeType;
            return cpp2::ErrorCode::E_EDGE_NOT_FOUND;
        }
        auto edgeName = this->env_->schemaMan_->toEdgeName(spaceId_, std::abs(edgeType));
        if (!edgeName.ok()) {
            VLOG(1) << "Can't find spaceId " << spaceId_ << " edgeType " << edgeType;
            return cpp2::ErrorCode::E_EDGE_NOT_FOUND;
        }
        resultDataSet_.colNames.emplace_back(
            folly::stringPrintf("%d:%s", edgeType, edgeName.value().c_str()));

        std::vector<PropContext> ctxs;
        auto& props = edgeProps[i].names_;
        for (size_t propIdx = 0; propIdx < props.size(); propIdx++) {
            auto& name = props[propIdx];
            // because there are some reserved edge prop in key (src/dst/type/rank),
            // we can't find those prop in schema
            PropContext ctx(name.c_str());
            auto iter = std::find_if(kPropsInKey_.begin(), kPropsInKey_.end(),
                                     [&] (const auto& entry) { return entry.first == name; });
            if (iter != kPropsInKey_.end()) {
                ctx.propInKeyType_ = iter->second;
            } else {
                auto field = edgeSchema->field(name);
                if (field == nullptr) {
                    VLOG(1) << "Can't find prop " << name << " edgeType " << edgeType;
                    return cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
                }
                ctx.field_ = field;
            }
            ctx.returned_ = true;
            ctxs.emplace_back(std::move(ctx));
        }
        edgeContexts_.emplace_back(edgeType, std::move(ctxs));
        edgeIndexMap_.emplace(edgeType, edgeContexts_.size() - 1);
        edgeSchemas_.emplace(edgeType, std::move(edgeSchema));
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

template<typename REQ, typename RESP>
cpp2::ErrorCode QueryBaseProcessor<REQ, RESP>::buildFilter(const REQ& req) {
    if (!req.__isset.filter) {
        return cpp2::ErrorCode::SUCCEEDED;
    }
    const auto& filterStr = *req.get_filter();
    if (!filterStr.empty()) {
        // todo(doodle): wait Expression ready, check if filter valid,
        // and add tag filter to tagContexts_
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

template<typename REQ, typename RESP>
void QueryBaseProcessor<REQ, RESP>::buildTagTTLInfo() {
    for (const auto& tc : tagContexts_) {
        auto tagId = tc.first;
        auto iter = tagSchemas_.find(tagId);
        CHECK(iter != tagSchemas_.end());
        const auto& tagSchema = iter->second;

        auto ttlInfo = tagSchema->getTTLInfo();
        if (ttlInfo.ok()) {
            VLOG(2) << "Add ttl col " << ttlInfo.value().first << " of tag " << tagId;
            tagTTLInfo_.emplace(tagId, std::move(ttlInfo.value()));
        }
    }
}

template<typename REQ, typename RESP>
void QueryBaseProcessor<REQ, RESP>::buildEdgeTTLInfo() {
    for (const auto& ec : edgeContexts_) {
        auto edgeType = ec.first;
        auto iter = edgeSchemas_.find(edgeType);
        CHECK(iter != tagSchemas_.end());
        const auto& edgeSchema = iter->second;

        auto ttlInfo = edgeSchema->getTTLInfo();
        if (ttlInfo.ok()) {
            VLOG(2) << "Add ttl col " << ttlInfo.value().first << " of edge " << edgeType;
            edgeTTLInfo_.emplace(edgeType, std::move(ttlInfo.value()));
        }
    }
}

template<typename REQ, typename RESP>
folly::Optional<std::pair<std::string, int64_t>>
QueryBaseProcessor<REQ, RESP>::getTagTTLInfo(TagID tagId) {
    folly::Optional<std::pair<std::string, int64_t>> ret;
    auto tagFound = tagTTLInfo_.find(tagId);
    if (tagFound != tagTTLInfo_.end()) {
        ret.emplace(tagFound->second.first, tagFound->second.second);
    }
    return ret;
}

template<typename REQ, typename RESP>
folly::Optional<std::pair<std::string, int64_t>>
QueryBaseProcessor<REQ, RESP>::getEdgeTTLInfo(EdgeType edgeType) {
    folly::Optional<std::pair<std::string, int64_t>> ret;
    auto edgeFound = edgeTTLInfo_.find(std::abs(edgeType));
    if (edgeFound != edgeTTLInfo_.end()) {
        ret.emplace(edgeFound->second.first, edgeFound->second.second);
    }
    return ret;
}

template<typename REQ, typename RESP>
cpp2::ErrorCode QueryBaseProcessor<REQ, RESP>::getSpaceVidLen(GraphSpaceID spaceId) {
    auto len = this->env_->schemaMan_->getSpaceVidLen(spaceId);
    if (!len.ok()) {
        return cpp2::ErrorCode::E_SPACE_NOT_FOUND;
    }
    vIdLen_ = len.value();
    return cpp2::ErrorCode::SUCCEEDED;
}

template <typename REQ, typename RESP>
kvstore::ResultCode QueryBaseProcessor<REQ, RESP>::processTagProps(
        PartitionID partId,
        const VertexID& vId,
        TagID tagId,
        const std::vector<PropContext>& returnProps,
        FilterContext& fcontext,
        nebula::DataSet& dataSet) {
    // use latest schema to check if value is expired for ttl
    auto schemaIter = tagSchemas_.find(tagId);
    CHECK(schemaIter != tagSchemas_.end());
    const auto* latestSchema = schemaIter->second.get();
    if (FLAGS_enable_vertex_cache && this->vertexCache_ != nullptr) {
        auto result = this->vertexCache_->get(std::make_pair(vId, tagId), partId);
        if (result.ok()) {
            auto value = std::move(result).value();
            auto ret = collectTagPropIfValid(latestSchema, value, tagId,
                                             returnProps, fcontext, dataSet);
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                VLOG(1) << "Evict from cache vId " << vId << ", tagId " << tagId;
                this->vertexCache_->evict(std::make_pair(vId, tagId), partId);
            }
            return ret;
        } else {
            VLOG(1) << "Miss cache for vId " << vId << ", tagId " << tagId;
        }
    }

    auto prefix = NebulaKeyUtils::vertexPrefix(vIdLen_, partId, vId, tagId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = this->env_->kvstore_->prefix(spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        VLOG(1) << "Error! ret = " << static_cast<int32_t>(ret) << ", spaceId " << spaceId_;
        return ret;
    }
    if (iter && iter->valid()) {
        // Will decode the properties according to the schema version
        // stored along with the properties
        ret = collectTagPropIfValid(latestSchema, iter->val(), tagId,
                                    returnProps, fcontext, dataSet);
        if (ret == kvstore::ResultCode::SUCCEEDED &&
            FLAGS_enable_vertex_cache &&
            this->vertexCache_ != nullptr) {
            this->vertexCache_->insert(std::make_pair(vId, tagId), iter->val().str(), partId);
            VLOG(1) << "Insert cache for vId " << vId << ", tagId " << tagId;
        }
        return ret;
    } else {
        VLOG(1) << "Missed partId " << partId << ", vId " << vId << ", tagId " << tagId;
        return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
    }
}

template <typename REQ, typename RESP>
kvstore::ResultCode QueryBaseProcessor<REQ, RESP>::collectTagPropIfValid(
        const meta::NebulaSchemaProvider* schema,
        folly::StringPiece value,
        TagID tagId,
        const std::vector<PropContext>& returnProps,
        FilterContext& fcontext,
        nebula::DataSet& dataSet) {
    auto reader = RowReader::getTagPropReader(this->env_->schemaMan_, spaceId_, tagId, value);
    if (!reader) {
        VLOG(1) << "Can't get tag reader of " << tagId;
        return kvstore::ResultCode::ERR_TAG_NOT_FOUND;
    }
    auto ttl = getTagTTLInfo(tagId);
    if (ttl.hasValue()) {
        auto ttlValue = ttl.value();
        if (checkDataExpiredForTTL(schema, reader.get(), ttlValue.first, ttlValue.second)) {
            return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
        }
    }
    return collectTagProps(tagId, reader.get(), returnProps, fcontext, dataSet);
}

template <typename REQ, typename RESP>
kvstore::ResultCode QueryBaseProcessor<REQ, RESP>::collectTagProps(
        TagID tagId,
        RowReader* reader,
        const std::vector<PropContext>& props,
        FilterContext& fcontext,
        nebula::DataSet& dataSet) {
    nebula::Row row;
    for (auto& prop : props) {
        VLOG(2) << "Collect prop " << prop.name_ << ", type " << tagId;
        if (reader != nullptr) {
            auto status = readValue(reader, prop);
            if (!status.ok()) {
                return kvstore::ResultCode::ERR_TAG_PROP_NOT_FOUND;
            }
            auto value = std::move(status.value());

            if (prop.tagFiltered_) {
                fcontext.tagFilters_.emplace(std::make_pair(tagId, prop.name_), value);
            }
            if (prop.returned_) {
                row.columns.emplace_back(std::move(value));
            }
        }
    }
    dataSet.rows.emplace_back(std::move(row));
    return kvstore::ResultCode::SUCCEEDED;
}

template <typename REQ, typename RESP>
kvstore::ResultCode QueryBaseProcessor<REQ, RESP>::collectEdgeProps(
        EdgeType edgeType,
        RowReader* reader,
        folly::StringPiece key,
        const std::vector<PropContext>& props,
        nebula::DataSet& dataSet,
        std::vector<PropStat>* stats) {
    nebula::Row row;
    for (size_t i = 0; i < props.size(); i++) {
        auto& prop = props[i];
        VLOG(2) << "Collect prop " << prop.name_ << ", type " << edgeType;
        nebula::Value value;
        switch (prop.propInKeyType_) {
            // prop in value
            case PropContext::PropInKeyType::NONE: {
                if (reader != nullptr) {
                    auto status = readValue(reader, prop);
                    if (!status.ok()) {
                        return kvstore::ResultCode::ERR_EDGE_PROP_NOT_FOUND;
                    }
                    value = std::move(status.value());
                }
                break;
            }
            case PropContext::PropInKeyType::SRC: {
                value = std::move(NebulaKeyUtils::getSrcId(vIdLen_, key));
                break;
            }
            case PropContext::PropInKeyType::DST: {
                value = std::move(NebulaKeyUtils::getDstId(vIdLen_, key));
                break;
            }
            case PropContext::PropInKeyType::TYPE: {
                value = std::move(NebulaKeyUtils::getEdgeType(vIdLen_, key));
                break;
            }
            case PropContext::PropInKeyType::RANK: {
                value = std::move(NebulaKeyUtils::getRank(vIdLen_, key));
                break;
            }
        }
        if (prop.hasStat_ && stats != nullptr) {
            addStatValue(value, (*stats)[prop.statIndex_]);
        }
        if (prop.returned_) {
            row.columns.emplace_back(std::move(value));
        }
    }
    dataSet.rows.emplace_back(std::move(row));
    return kvstore::ResultCode::SUCCEEDED;
}

template<typename REQ, typename RESP>
StatusOr<nebula::Value> QueryBaseProcessor<REQ, RESP>::readValue(RowReader* reader,
                                                                 const PropContext& ctx) {
    auto value = reader->getValueByName(ctx.name_);
    if (value.type() == Value::Type::NULLVALUE) {
        // read null value
        auto nullType = value.getNull();
        if (nullType == NullType::BAD_DATA ||
            nullType == NullType::BAD_TYPE ||
            nullType == NullType::UNKNOWN_PROP) {
            VLOG(1) << "Fail to read prop " << ctx.name_;
            if (ctx.field_ != nullptr) {
                if (ctx.field_->hasDefault()) {
                    return ctx.field_->defaultValue();
                } else if (ctx.field_->nullable()) {
                    return NullType::__NULL__;
                }
            }
        } else if (nullType == NullType::__NULL__ || nullType == NullType::NaN) {
            return value;
        }
        return Status::Error(folly::stringPrintf("Fail to read prop %s ", ctx.name_.c_str()));
    }
    return value;
}

template<typename REQ, typename RESP>
void QueryBaseProcessor<REQ, RESP>::addStatValue(const Value& value, PropStat& stat) {
    if (value.type() == Value::Type::INT || value.type() == Value::Type::FLOAT) {
        stat.sum_ = stat.sum_ + value;
        ++stat.count_;
    }
}

template<typename REQ, typename RESP>
std::vector<ReturnProp> QueryBaseProcessor<REQ, RESP>::buildAllTagProps(GraphSpaceID spaceId) {
    auto schemas = this->env_->schemaMan_->listLatestTagSchema(spaceId);
    std::vector<ReturnProp> result;
    for (const auto& entry : schemas) {
        ReturnProp prop;
        prop.entryId_ = entry.first;
        std::vector<std::string> names;
        const auto& schema = entry.second;
        auto count = schema->getNumFields();
        for (size_t i = 0; i < count; i++) {
            auto name = schema->getFieldName(i);
            names.emplace_back(std::move(name));
        }
        prop.names_ = std::move(names);
        result.emplace_back(std::move(prop));
    }
    return result;
}

template<typename REQ, typename RESP>
std::vector<ReturnProp> QueryBaseProcessor<REQ, RESP>::buildAllEdgeProps(
        GraphSpaceID spaceId,
        const cpp2::EdgeDirection& direction) {
    auto schemas = this->env_->schemaMan_->listLatestEdgeSchema(spaceId);
    std::vector<ReturnProp> result;
    for (const auto& entry : schemas) {
        ReturnProp prop;
        prop.entryId_ = entry.first;
        if (direction == cpp2::EdgeDirection::IN_EDGE) {
            prop.entryId_ = -prop.entryId_;
        }
        std::vector<std::string> names;
        auto& schema = entry.second;
        auto count = schema->getNumFields();
        for (size_t i = 0; i < count; i++) {
            auto name = schema->getFieldName(i);
            names.emplace_back(std::move(name));
        }
        prop.names_ = std::move(names);
        if (direction == cpp2::EdgeDirection::BOTH) {
            ReturnProp reverse = prop;
            reverse.entryId_ = -prop.entryId_;
            result.emplace_back(std::move(reverse));
        }
        result.emplace_back(std::move(prop));
    }
    return result;
}

}  // namespace storage
}  // namespace nebula
