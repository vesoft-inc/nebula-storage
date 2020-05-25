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
        auto iter = this->tagContext_.schemas_.find(tagId);
        if (iter == this->tagContext_.schemas_.end()) {
            VLOG(1) << "Can't find spaceId " << spaceId_ << " tagId " << tagId;
            return cpp2::ErrorCode::E_TAG_NOT_FOUND;
        }
        CHECK(!iter->second.empty());
        const auto& tagSchema = iter->second.back();
        auto tagName = this->env_->schemaMan_->toTagName(spaceId_, tagId);
        if (!tagName.ok()) {
            VLOG(1) << "Can't find spaceId " << spaceId_ << " tagId " << tagId;
            return cpp2::ErrorCode::E_TAG_NOT_FOUND;
        }
        resultDataSet_.colNames.emplace_back(
            folly::stringPrintf("%d:%s", tagId, tagName.value().c_str()));

        std::vector<PropContext> ctxs;
        auto& props = vertexProps[i].names_;
        for (const auto& name : props) {
            auto field = tagSchema->field(name);
            if (field == nullptr) {
                VLOG(1) << "Can't find prop " << name << " tagId " << tagId;
                return cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND;
            }

            // todo(doodle): perhaps need to dedup here
            PropContext ctx(name.c_str());
            ctx.returned_ = true;
            ctx.field_ = field;
            tagContext_.tagIdProps_.emplace(std::make_pair(tagId, name));
            ctxs.emplace_back(std::move(ctx));
        }
        this->tagContext_.propContexts_.emplace_back(tagId, std::move(ctxs));
        this->tagContext_.indexMap_.emplace(tagId, this->tagContext_.propContexts_.size() - 1);
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

template<typename REQ, typename RESP>
cpp2::ErrorCode QueryBaseProcessor<REQ, RESP>::handleEdgeProps(
        const std::vector<ReturnProp>& edgeProps) {
    for (size_t i = 0; i < edgeProps.size(); i++) {
        auto edgeType = edgeProps[i].entryId_;
        auto iter = this->edgeContext_.schemas_.find(std::abs(edgeType));
        if (iter == this->edgeContext_.schemas_.end()) {
            VLOG(1) << "Can't find spaceId " << spaceId_ << " edgeType " << edgeType;
            return cpp2::ErrorCode::E_EDGE_NOT_FOUND;
        }
        CHECK(!iter->second.empty());
        const auto& edgeSchema = iter->second.back();
        auto edgeName = this->env_->schemaMan_->toEdgeName(spaceId_, std::abs(edgeType));
        if (!edgeName.ok()) {
            VLOG(1) << "Can't find spaceId " << spaceId_ << " edgeType " << edgeType;
            return cpp2::ErrorCode::E_EDGE_NOT_FOUND;
        }
        resultDataSet_.colNames.emplace_back(
            folly::stringPrintf("%d:%s", edgeType, edgeName.value().c_str()));

        std::vector<PropContext> ctxs;
        auto& props = edgeProps[i].names_;
        for (const auto& name : props) {
            // because there are some reserved edge prop in key (src/dst/type/rank),
            // we can't find those prop in schema
            PropContext ctx(name.c_str());
            auto propIter = std::find_if(kPropsInKey_.begin(), kPropsInKey_.end(),
                                         [&] (const auto& entry) { return entry.first == name; });
            if (propIter != kPropsInKey_.end()) {
                ctx.propInKeyType_ = propIter->second;
            } else {
                auto field = edgeSchema->field(name);
                if (field == nullptr) {
                    VLOG(1) << "Can't find prop " << name << " edgeType " << edgeType;
                    return cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
                }
                ctx.field_ = field;
            }
            ctx.returned_ = true;
            // todo(doodle): perhaps need to dedup here
            edgeContext_.edgeTypeProps_.emplace(std::make_pair(edgeType, name));
            ctxs.emplace_back(std::move(ctx));
        }
        this->edgeContext_.propContexts_.emplace_back(edgeType, std::move(ctxs));
        this->edgeContext_.indexMap_.emplace(edgeType, this->edgeContext_.propContexts_.size() - 1);
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
    for (const auto& tc : this->tagContext_.propContexts_) {
        auto tagId = tc.first;
        auto iter = this->tagContext_.schemas_.find(tagId);
        CHECK(iter != this->tagContext_.schemas_.end());
        const auto& tagSchema = iter->second.back();

        auto ttlInfo = tagSchema->getTTLInfo();
        if (ttlInfo.ok()) {
            VLOG(2) << "Add ttl col " << ttlInfo.value().first << " of tag " << tagId;
            tagContext_.ttlInfo_.emplace(tagId, std::move(ttlInfo).value());
        }
    }
}

template<typename REQ, typename RESP>
void QueryBaseProcessor<REQ, RESP>::buildEdgeTTLInfo() {
    for (const auto& ec : this->edgeContext_.propContexts_) {
        auto edgeType = ec.first;
        auto iter = this->edgeContext_.schemas_.find(std::abs(edgeType));
        CHECK(iter != this->edgeContext_.schemas_.end());
        const auto& edgeSchema = iter->second.back();

        auto ttlInfo = edgeSchema->getTTLInfo();
        if (ttlInfo.ok()) {
            VLOG(2) << "Add ttl col " << ttlInfo.value().first << " of edge " << edgeType;
            edgeContext_.ttlInfo_.emplace(edgeType, std::move(ttlInfo).value());
        }
    }
}

template<typename REQ, typename RESP>
std::vector<ReturnProp> QueryBaseProcessor<REQ, RESP>::buildAllTagProps() {
    std::vector<ReturnProp> result;
    for (const auto& entry : this->tagContext_.schemas_) {
        ReturnProp prop;
        prop.entryId_ = entry.first;
        std::vector<std::string> names;
        const auto& schema = entry.second.back();
        auto count = schema->getNumFields();
        for (size_t i = 0; i < count; i++) {
            auto name = schema->getFieldName(i);
            names.emplace_back(std::move(name));
        }
        prop.names_ = std::move(names);
        result.emplace_back(std::move(prop));
    }
    std::sort(result.begin(), result.end(),
              [&] (const auto& a, const auto& b) { return a.entryId_ < b.entryId_; });
    return result;
}

template<typename REQ, typename RESP>
std::vector<ReturnProp> QueryBaseProcessor<REQ, RESP>::buildAllEdgeProps(
        const cpp2::EdgeDirection& direction) {
    std::vector<ReturnProp> result;
    for (const auto& entry : this->edgeContext_.schemas_) {
        ReturnProp prop;
        prop.entryId_ = entry.first;
        if (direction == cpp2::EdgeDirection::IN_EDGE) {
            prop.entryId_ = -prop.entryId_;
        }
        std::vector<std::string> names;
        const auto& schema = entry.second.back();
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
    std::sort(result.begin(), result.end(),
              [&] (const auto& a, const auto& b) { return a.entryId_ < b.entryId_; });
    return result;
}

template<typename REQ, typename RESP>
cpp2::ErrorCode QueryBaseProcessor<REQ, RESP>::prepareVertexProps(
        const std::vector<cpp2::PropExp>& vertexProps,
        std::vector<ReturnProp>& returnProps) {
    // todo(doodle): wait
    /*
    for (auto& vertexProp : vertexProps) {
        // If there is no property specified, add all property of latest schema to vertexProps
        if (vertexProp.names.empty()) {
            auto tagId = vertexProp.tag;
            auto tagSchema = env_->schemaMan_->getTagSchema(spaceId_, tagId);
            if (!tagSchema) {
                VLOG(1) << "Can't find spaceId " << spaceId_ << " tag " << tagId;
                return cpp2::ErrorCode::E_TAG_NOT_FOUND;
            }

            auto count = tagSchema->getNumFields();
            for (size_t i = 0; i < count; i++) {
                auto name = tagSchema->getFieldName(i);
                vertexProp.names.emplace_back(std::move(name));
            }
        }
    }
    */
    UNUSED(vertexProps);
    UNUSED(returnProps);
    return cpp2::ErrorCode::SUCCEEDED;
}

template <typename REQ, typename RESP>
cpp2::ErrorCode QueryBaseProcessor<REQ, RESP>::prepareEdgeProps(
        const std::vector<cpp2::PropExp>& edgeProps,
        std::vector<ReturnProp>& returnProps) {
    // todo(doodle): wait
    /*
    for (auto& edgeProp : edgeProps) {
        // If there is no property specified, add all property of latest schema to edgeProps
        if (edgeProp.names.empty()) {
            auto edgeType = edgeProp.type;
            auto edgeSchema = env_->schemaMan_->getEdgeSchema(spaceId_, std::abs(edgeType));
            if (!edgeSchema) {
                VLOG(1) << "Can't find spaceId " << spaceId_ << " edgeType " << edgeType;
                return cpp2::ErrorCode::E_EDGE_NOT_FOUND;
            }

            auto count = edgeSchema->getNumFields();
            for (size_t i = 0; i < count; i++) {
                auto name = edgeSchema->getFieldName(i);
                edgeProp.names.emplace_back(std::move(name));
            }
        }
    }
    */
    UNUSED(edgeProps);
    UNUSED(returnProps);
    return cpp2::ErrorCode::SUCCEEDED;
}

template <typename REQ, typename RESP>
cpp2::ErrorCode QueryBaseProcessor<REQ, RESP>::getSpaceVertexSchema() {
    auto tags = this->env_->schemaMan_->getAllVerTagSchema(spaceId_);
    if (!tags.ok()) {
        return cpp2::ErrorCode::E_SPACE_NOT_FOUND;
    }
    this->tagContext_.schemas_ = std::move(tags).value();
    return cpp2::ErrorCode::SUCCEEDED;
}

template <typename REQ, typename RESP>
cpp2::ErrorCode QueryBaseProcessor<REQ, RESP>::getSpaceEdgeSchema() {
    auto edges = this->env_->schemaMan_->getAllVerEdgeSchema(spaceId_);
    if (!edges.ok()) {
        return cpp2::ErrorCode::E_SPACE_NOT_FOUND;
    }

    this->edgeContext_.schemas_ = std::move(edges).value();
    return cpp2::ErrorCode::SUCCEEDED;
}

template <typename REQ, typename RESP>
bool QueryBaseProcessor<REQ, RESP>::checkExp(const Expression* exp) {
    switch (exp->kind()) {
        // TODO constant expression
        case Expression::Kind::kConstant:
            return true;
        case Expression::Kind::kAdd:
        case Expression::Kind::kMinus:
        case Expression::Kind::kMultiply:
        case Expression::Kind::kDivision:
        case Expression::Kind::kMod: {
            auto* ariExp = static_cast<const ArithmeticExpression*>(exp);
            return checkExp(ariExp->left()) && checkExp(ariExp->right());
        }
        case Expression::Kind::kRelEQ:
        case Expression::Kind::kRelNE:
        case Expression::Kind::kRelLT:
        case Expression::Kind::kRelLE:
        case Expression::Kind::kRelGT:
        case Expression::Kind::kRelGE:
        case Expression::Kind::kRelIn: {
            auto* relExp = static_cast<const RelationalExpression*>(exp);
            return checkExp(relExp->left()) && checkExp(relExp->right());
        }
        case Expression::Kind::kLogicalAnd:
        case Expression::Kind::kLogicalOr:
        case Expression::Kind::kLogicalXor: {
            auto* logExp = static_cast<const LogicalExpression*>(exp);
            return checkExp(logExp->left()) && checkExp(logExp->right());
        }
        case Expression::Kind::kUnaryPlus:
        case Expression::Kind::kUnaryNegate:
        case Expression::Kind::kUnaryNot:
        case Expression::Kind::kUnaryIncr:
        case Expression::Kind::kUnaryDecr: {
            auto* unaExp = static_cast<const UnaryExpression*>(exp);
            return checkExp(unaExp->operand());
        }
        case Expression::Kind::kTypeCasting: {
            auto* typExp = static_cast<const TypeCastingExpression*>(exp);
            return checkExp(typExp->operand());
        }
        case Expression::Kind::kFunctionCall: {
            auto* funcExp = static_cast<const FunctionCallExpression*>(exp);
            // auto* name = funcExp->name();
            auto args = funcExp->args();
            // auto func = FunctionManager::get(*name, args.size());
            // if (!func.ok()) {
            //     return false;
            // }
            for (auto& arg : args) {
                if (!checkExp(arg)) {
                    return false;
                }
            }
            // funcExp->setFunc(std::move(func).value());
            return true;
        }
        case Expression::Kind::kSrcProperty: {
            auto* sourceExp = static_cast<const SourcePropertyExpression*>(exp);
            const auto* tagName = sourceExp->sym();
            const auto* propName = sourceExp->prop();

            auto tagRet = this->env_->schemaMan_->toTagID(spaceId_, *tagName);
            if (!tagRet.ok()) {
                VLOG(1) << "Can't find tag " << *tagName << ", in space " << spaceId_;
                return false;
            }
            auto tagId = tagRet.value();

            auto propIter = tagContext_.tagIdProps_.find(std::make_pair(tagId, *propName));
            if (propIter == tagContext_.tagIdProps_.end()) {
                VLOG(1) << "There is no related tag prop existed in tagContexts!";
                PropContext ctx(propName->c_str());
                auto iter = tagContext_.schemas_.find(tagId);
                if (iter == tagContext_.schemas_.end()) {
                    VLOG(1) << "Can't find spaceId " << spaceId_ << " tagId " << tagId;
                    return false;
                }

                CHECK(!iter->second.empty());
                // Use newest version
                const auto& tagSchema = iter->second.back();
                CHECK_NOTNULL(tagSchema);

                auto field = tagSchema->field(*propName);
                if (field == nullptr) {
                    VLOG(1) << "Can't find related prop " << *propName << " on tag " << tagName;
                    return false;
                }
                ctx.field_ = field;
                auto indexIter = tagContext_.indexMap_.find(tagId);
                if (indexIter != tagContext_.indexMap_.end()) {
                    // add tag prop
                    auto index = indexIter->second;
                    tagContext_.propContexts_[index].second.emplace_back(std::move(ctx));
                } else {
                    std::vector<PropContext> ctxs;
                    ctxs.emplace_back(ctx);
                    tagContext_.propContexts_.emplace_back(tagId, std::move(ctxs));
                    tagContext_.indexMap_.emplace(tagId, tagContext_.propContexts_.size() - 1);
                }
                tagContext_.tagIdProps_.emplace(std::make_pair(tagId, *propName));
            }
            return true;
        }
        case Expression::Kind::kEdgeRank:
        case Expression::Kind::kEdgeDst:
        case Expression::Kind::kEdgeSrc:
        case Expression::Kind::kEdgeType: {
            return true;
        }
        case Expression::Kind::kEdgeProperty: {
            if (edgeContext_.propContexts_.empty()) {
                VLOG(1) << "No edge requested!";
                return false;
            }

            auto* edgeExp = static_cast<const EdgePropertyExpression*>(exp);

            // TODO(simon.liu) we need handle rename.
            auto edgeStatus = this->env_->schemaMan_->toEdgeType(spaceId_, *edgeExp->sym());
            if (!edgeStatus.ok()) {
                VLOG(1) << "Can't find edge " << *(edgeExp->sym());
                return false;
            }

            auto edgeType = edgeStatus.value();
            auto iter = edgeContext_.schemas_.find(std::abs(edgeType));
            if (iter == edgeContext_.schemas_.end()) {
                VLOG(1) << "Can't find spaceId " << spaceId_ << " edgeType " << std::abs(edgeType);
                return false;
            }
            CHECK(!iter->second.empty());
            // Use newest version
            const auto& edgeSchema = iter->second.back();
            CHECK_NOTNULL(edgeSchema);
            auto schema = this->env_->schemaMan_->getEdgeSchema(spaceId_, std::abs(edgeType));
            if (!schema) {
                VLOG(1) << "Cant find edgeType " << edgeType;
                return false;
            }

            const auto* propName = edgeExp->prop();
            auto field = schema->field(*propName);
            if (field == nullptr) {
                VLOG(1) << "Can't find related prop " << *propName << " on edge "
                        << *(edgeExp->sym());
                return false;
            }
            return true;
        }
        case Expression::Kind::kVarProperty:
        case Expression::Kind::kDstProperty:
        case Expression::Kind::kInputProperty: {
            return false;
        }
        default: {
            VLOG(1) << "Unsupport expression type! kind = "
                    << std::to_string(static_cast<uint8_t>(exp->kind()));
            return false;
        }
    }
}

}  // namespace storage
}  // namespace nebula
