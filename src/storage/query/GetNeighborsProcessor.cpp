/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/query/GetNeighborsProcessor.h"
#include "common/NebulaKeyUtils.h"

DEFINE_int32(max_edge_returned_per_vertex, INT_MAX, "Max edge number returnred searching vertex");
DEFINE_bool(enable_vertex_cache, true, "Enable vertex cache");
DEFINE_bool(enable_reservoir_sampling, false, "Will do reservoir sampling if set true.");

namespace nebula {
namespace storage {

void GetNeighborsProcessor::process(const cpp2::GetNeighborsRequest& req) {
    spaceId_ = req.get_space_id();
    auto retCode = getSpaceVidLen(spaceId_);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        for (auto& p : req.get_parts()) {
            this->pushResultCode(retCode, p.first);
        }
        this->onFinished();
        return;
    }

    retCode = checkAndBuildContexts(req);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        for (auto& p : req.get_parts()) {
            this->pushResultCode(retCode, p.first);
        }
        this->onFinished();
        return;
    }

    std::unordered_set<PartitionID> failedParts;
    for (const auto& partEntry : req.get_parts()) {
        auto partId = partEntry.first;
        for (const auto& row : partEntry.second) {
            CHECK_GE(row.columns.size(), 1);
            // the first column of each row would be the vertex id
            auto ret = processOneVertex(partId, row.columns[0].getStr());
            if (ret != kvstore::ResultCode::SUCCEEDED &&
                failedParts.find(partId) == failedParts.end()) {
                failedParts.emplace(partId);
                handleErrorCode(ret, spaceId_, partId);
            }
        }
    }
    onProcessFinished();
    onFinished();
}

cpp2::ErrorCode GetNeighborsProcessor::checkAndBuildContexts(const cpp2::GetNeighborsRequest& req) {
    resultDataSet_.colNames.emplace_back("_vid");
    resultDataSet_.colNames.emplace_back("_stats");

    auto code = buildTagContext(req);
    if (code != cpp2::ErrorCode::SUCCEEDED) {
        return code;
    }
    code = buildEdgeContext(req);
    if (code != cpp2::ErrorCode::SUCCEEDED) {
        return code;
    }
    code = buildFilter(req);
    if (code != cpp2::ErrorCode::SUCCEEDED) {
        return code;
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

cpp2::ErrorCode GetNeighborsProcessor::buildTagContext(const cpp2::GetNeighborsRequest& req) {
    std::vector<ReturnProp> returnProps;
    // generate related props if no tagId or property specified
    auto ret = prepareVertexProps(req.vertex_props, returnProps);
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        return ret;
    }
    // generate tagContexts_
    ret = handleVertexProps(returnProps);
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        return ret;
    }
    buildTagTTLInfo();
    return cpp2::ErrorCode::SUCCEEDED;
}

cpp2::ErrorCode GetNeighborsProcessor::prepareVertexProps(
        const std::vector<std::string>& vertexProps,
        std::vector<ReturnProp>& returnProps) {
    // If no edgeType specified, get all property of all tagId in space
    if (vertexProps.empty()) {
        returnProps = buildAllTagProps(spaceId_);
        return cpp2::ErrorCode::SUCCEEDED;
    }
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
    return cpp2::ErrorCode::SUCCEEDED;
}

cpp2::ErrorCode GetNeighborsProcessor::buildEdgeContext(const cpp2::GetNeighborsRequest& req) {
    std::vector<ReturnProp> returnProps;
    // generate related props if no edge type or property specified
    auto ret = prepareEdgeProps(req.edge_props, req.edge_direction, returnProps);
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        return ret;
    }
    // generate edgeContexts_
    ret = handleEdgeProps(returnProps);
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        return ret;
    }
    ret = handleEdgeStatProps(req.stat_props);
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        return ret;
    }
    buildEdgeTTLInfo();
    return cpp2::ErrorCode::SUCCEEDED;
}

cpp2::ErrorCode GetNeighborsProcessor::prepareEdgeProps(const std::vector<std::string>& edgeProps,
                                                        const cpp2::EdgeDirection& direction,
                                                        std::vector<ReturnProp>& returnProps) {
    // If no edgeType specified, get all property of all edge type in space
    if (edgeProps.empty()) {
        scanAllEdges_ = true;
        returnProps = buildAllEdgeProps(spaceId_, direction);
        return cpp2::ErrorCode::SUCCEEDED;
    }
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
    return cpp2::ErrorCode::SUCCEEDED;
}

cpp2::ErrorCode GetNeighborsProcessor::handleEdgeStatProps(
        const std::vector<cpp2::StatProp>& statProps) {
    statCount_ = statProps.size();
    // todo(doodle): since we only keep one kind of stat in PropContext, there could be a problem
    // if we specified multiple stat of same prop
    for (size_t idx = 0; idx < statCount_; idx++) {
        // todo(doodle): wait
        /*
        const auto& prop = statProps[idx];
        const auto edgeType = prop.type;
        const auto& name = prop.name;

        auto schema = env_->schemaMan_->getEdgeSchema(spaceId_, std::abs(edgeType));
        if (!schema) {
            VLOG(1) << "Can't find spaceId " << spaceId_ << " edgeType " << edgeType;
            return cpp2::ErrorCode::E_EDGE_NOT_FOUND;
        }
        const meta::SchemaProviderIf::Field* field = nullptr;
        if (name != "_rank") {
            field = schema->field(name);
            if (field == nullptr) {
                VLOG(1) << "Can't find prop " << name << " edgeType " << edgeType;
                return cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
            }
            auto ret = checkStatType(field->type(), prop.stat);
            if (ret != cpp2::ErrorCode::SUCCEEDED) {
                return ret;
            }
        }

        // find if corresponding edgeType contexts exists
        auto edgeIter = edgeIndexMap_.find(edgeType);
        if (edgeIter != edgeIndexMap_.end()) {
            // find if corresponding PropContext exists
            auto& ctxs = edgeContexts_[edgeIter->second].second;
            auto propIter = std::find_if(ctxs.begin(), ctxs.end(),
                [&] (const auto& propContext) {
                    return propContext.name_ == name;
                });
            if (propIter != ctxs.end()) {
                propIter->hasStat_ = true;
                propIter->statIndex_ = idx;
                propIter->statType_ = prop.stat;
                continue;
            } else {
                auto ctx = buildPropContextWithStat(name, idx, prop.stat, field);
                ctxs.emplace_back(std::move(ctx));
            }
        } else {
            std::vector<PropContext> ctxs;
            auto ctx = buildPropContextWithStat(name, idx, prop.stat, field);
            ctxs.emplace_back(std::move(ctx));
            edgeContexts_.emplace_back(edgeType, std::move(ctxs));
            edgeIndexMap_.emplace(edgeType, edgeContexts_.size() - 1);
        }
        */
    }

    return cpp2::ErrorCode::SUCCEEDED;
}

PropContext GetNeighborsProcessor::buildPropContextWithStat(
        const std::string& name,
        size_t idx,
        const cpp2::StatType& statType,
        const meta::SchemaProviderIf::Field* field) {
    PropContext ctx(name.c_str());
    ctx.hasStat_ = true;
    ctx.statIndex_ = idx;
    ctx.statType_ = statType;
    ctx.field_ = field;
    // for rank stat
    if (name == "_rank") {
        ctx.propInKeyType_ = PropContext::PropInKeyType::RANK;
    }
    return ctx;
}

cpp2::ErrorCode GetNeighborsProcessor::checkStatType(const meta::cpp2::PropertyType& fType,
                                                     cpp2::StatType statType) {
    switch (statType) {
        case cpp2::StatType::SUM:
        case cpp2::StatType::AVG:
        case cpp2::StatType::MIN:
        case cpp2::StatType::MAX: {
            if (fType == meta::cpp2::PropertyType::INT64 ||
                fType == meta::cpp2::PropertyType::INT32 ||
                fType == meta::cpp2::PropertyType::INT16 ||
                fType == meta::cpp2::PropertyType::INT8 ||
                fType == meta::cpp2::PropertyType::FLOAT ||
                fType == meta::cpp2::PropertyType::DOUBLE) {
                return cpp2::ErrorCode::SUCCEEDED;
            }
            return cpp2::ErrorCode::E_INVALID_STAT_TYPE;
        }
        case cpp2::StatType::COUNT: {
             break;
        }
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

kvstore::ResultCode GetNeighborsProcessor::processOneVertex(
        PartitionID partId, const VertexID& vId) {
    nebula::Row row;
    // vertexId is the first column
    row.columns.emplace_back(vId);
    // reserve second column for stat
    row.columns.emplace_back(NullType::__NULL__);

    FilterContext fcontext;
    auto ret = processTag(partId, vId, fcontext, row);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        return ret;
    }

    if (scanAllEdges_) {
        ret = scanEdges(partId, vId, fcontext, row);
    } else {
        ret = processEdge(partId, vId, fcontext, row);
    }
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        return ret;
    }

    // We will return an row with only Id if it doesn't have specified tag or edge,
    // this is different from 1.0
    resultDataSet_.rows.emplace_back(std::move(row));
    return kvstore::ResultCode::SUCCEEDED;
}

kvstore::ResultCode GetNeighborsProcessor::scanEdges(PartitionID partId,
                                                     const VertexID& vId,
                                                     FilterContext& fcontext,
                                                     nebula::Row& row) {
    auto prefix = NebulaKeyUtils::vertexPrefix(vIdLen_, partId, vId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env_->kvstore_->prefix(spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        return ret;
    }
    if (!iter || !iter->valid()) {
        return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
    }

    CHECK_EQ(tagContexts_.size() + 2, row.columns.size());
    row.columns.resize(tagContexts_.size() + edgeContexts_.size() + 2,
                       NullType::__NULL__);

    auto stats = initStatValue();
    int32_t lastType = 0;
    size_t lastEdgeTypeIdx = 0;
    EdgeRanking lastRank = 0;
    VertexID lastDstId = "";
    const nebula::meta::NebulaSchemaProvider* lastSchema;
    folly::Optional<std::pair<std::string, int64_t>> ttl;
    auto reader = std::unique_ptr<RowReader>();
    nebula::DataSet dataSet;
    int64_t edgeRowCount = 0;

    for (; iter->valid(); iter->next()) {
        if (edgeRowCount >= FLAGS_max_edge_returned_per_vertex) {
            break;
        }
        auto key = iter->key();
        if (!NebulaKeyUtils::isEdge(vIdLen_, key)) {
            continue;
        }

        auto edgeType = NebulaKeyUtils::getEdgeType(vIdLen_, key);
        auto rank = NebulaKeyUtils::getRank(vIdLen_, key);
        auto dstId = NebulaKeyUtils::getDstId(vIdLen_, key);
        if (edgeType == lastType && rank == lastRank && lastDstId == dstId.str()) {
            VLOG(1) << "Only get the latest version for each edge.";
            continue;
        }

        if (lastSchema == nullptr || edgeType != lastType) {
            // add dataSet of lastType to row
            if (lastType != 0) {
                auto idx = lastEdgeTypeIdx + tagContexts_.size() + 2;
                row.columns[idx] = std::move(dataSet);
            }

            auto idxIter = edgeIndexMap_.find(edgeType);
            if (idxIter == edgeIndexMap_.end()) {
                // skip the edges should not return
                continue;
            }

            auto schemaIter = edgeSchemas_.find(edgeType);
            if (schemaIter == edgeSchemas_.end()) {
                // skip the edges which is obsolete
                continue;
            }
            lastType = edgeType;
            lastSchema = schemaIter->second.get();
            lastEdgeTypeIdx = idxIter->second;
            ttl = getEdgeTTLInfo(edgeType);
        }

        lastRank = rank;
        lastDstId = dstId.str();
        auto val = iter->val();
        if (!reader) {
            reader = RowReader::getEdgePropReader(env_->schemaMan_, spaceId_,
                                                    std::abs(edgeType), val);
            if (!reader) {
                return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
            }
        } else if (!reader->resetEdgePropReader(env_->schemaMan_, spaceId_,
                                                std::abs(edgeType), val)) {
            return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
        }

        if (ttl.has_value() &&
            checkDataExpiredForTTL(lastSchema, reader.get(),
                                   ttl.value().first, ttl.value().second)) {
            continue;
        }

        UNUSED(fcontext);

        ret = collectEdgeProps(edgeType, reader.get(), key,
                               edgeContexts_[lastEdgeTypeIdx].second, dataSet, &stats);
    }

    if (lastType != 0) {
        auto idx = lastEdgeTypeIdx + tagContexts_.size() + 2;
        row.columns[idx] = std::move(dataSet);
    }

    auto statsValueList = calculateStat(stats);
    if (!statsValueList.empty()) {
        row.columns[kStatReturnIndex_] = std::move(statsValueList);
    }
    return kvstore::ResultCode::SUCCEEDED;
}

kvstore::ResultCode GetNeighborsProcessor::processTag(PartitionID partId,
                                                      const VertexID& vId,
                                                      FilterContext& fcontext,
                                                      nebula::Row& row) {
    for (auto& tc : this->tagContexts_) {
        nebula::DataSet dataSet;
        VLOG(1) << "partId " << partId << ", vId " << vId << ", tagId " << tc.first
                << ", prop size " << tc.second.size();
        auto ret = processTagProps(partId, vId, tc.first, tc.second, fcontext, dataSet);
        if (ret == kvstore::ResultCode::SUCCEEDED) {
            row.columns.emplace_back(std::move(dataSet));
            continue;
        } else if (ret == kvstore::ResultCode::ERR_KEY_NOT_FOUND) {
            row.columns.emplace_back(NullType::__NULL__);
            continue;
        } else {
            return ret;
        }
    }
    return kvstore::ResultCode::SUCCEEDED;
}

kvstore::ResultCode GetNeighborsProcessor::processEdge(PartitionID partId,
                                                       const VertexID& vId,
                                                       const FilterContext& fcontext,
                                                       nebula::Row& row) {
    auto stats = initStatValue();
    int64_t edgeRowCount = 0;
    for (const auto& ec : edgeContexts_) {
        VLOG(1) << "partId " << partId << ", vId " << vId << ", edgeType " << ec.first
                << ", prop size " << ec.second.size();
        if (edgeRowCount >= FLAGS_max_edge_returned_per_vertex) {
            // add a null field if return size exceeds, make sure the resp has correct columns
            row.columns.emplace_back(NullType::__NULL__);
            continue;
        }

        nebula::DataSet dataSet;
        auto edgeType = ec.first;
        auto& returnProps = ec.second;

        // use latest schema to check if value is expired for ttl
        auto schemaIter = edgeSchemas_.find(edgeType);
        CHECK(schemaIter != edgeSchemas_.end());
        const auto& latestSchema = schemaIter->second;

        auto ttl = getEdgeTTLInfo(edgeType);
        auto ret = processEdgeProps(partId, vId, edgeType, edgeRowCount,
                   [this, edgeType, &fcontext, &ttl, schema = latestSchema.get(),
                    &returnProps, &dataSet, &stats]
                   (std::unique_ptr<RowReader>* reader,
                    folly::StringPiece key,
                    folly::StringPiece val)
                   -> kvstore::ResultCode {
            if (reader->get() == nullptr) {
                *reader = RowReader::getEdgePropReader(env_->schemaMan_, spaceId_,
                                                       std::abs(edgeType), val);
                if (!reader) {
                    return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
                }
            } else if (!(*reader)->resetEdgePropReader(env_->schemaMan_, spaceId_,
                                                       std::abs(edgeType), val)) {
                return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
            }

            if (ttl.has_value() &&
                checkDataExpiredForTTL(schema, reader->get(),
                                       ttl.value().first, ttl.value().second)) {
                return kvstore::ResultCode::ERR_RESULT_EXPIRED;
            }

            UNUSED(fcontext);

            return collectEdgeProps(edgeType, reader->get(), key, returnProps, dataSet, &stats);
        });

        if (ret == kvstore::ResultCode::SUCCEEDED ||
            ret == kvstore::ResultCode::ERR_RESULT_OVERFLOW) {
            row.columns.emplace_back(std::move(dataSet));
            continue;
        } else if (ret == kvstore::ResultCode::ERR_KEY_NOT_FOUND) {
            // just add a null field if there is no corresponding edge
            row.columns.emplace_back(NullType::__NULL__);
            continue;
        } else if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }
    }

    auto statsValueList = calculateStat(stats);
    if (!statsValueList.empty()) {
        row.columns[kStatReturnIndex_] = std::move(statsValueList);
    }
    return kvstore::ResultCode::SUCCEEDED;
}

kvstore::ResultCode GetNeighborsProcessor::processEdgeProps(PartitionID partId,
                                                            const VertexID& vId,
                                                            EdgeType edgeType,
                                                            int64_t& edgeRowCount,
                                                            EdgeProcessor proc) {
    auto prefix = NebulaKeyUtils::edgePrefix(vIdLen_, partId, vId, edgeType);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env_->kvstore_->prefix(spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        return ret;
    }
    if (!iter || !iter->valid()) {
        return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
    }

    EdgeRanking lastRank = -1;
    VertexID lastDstId = "";
    lastDstId.reserve(vIdLen_);
    bool firstLoop = true;
    int64_t count = edgeRowCount;

    auto reader = std::unique_ptr<RowReader>();
    for (; iter->valid(); iter->next()) {
        if (edgeRowCount >= FLAGS_max_edge_returned_per_vertex) {
            return kvstore::ResultCode::ERR_RESULT_OVERFLOW;
        }

        auto key = iter->key();
        auto val = iter->val();
        auto rank = NebulaKeyUtils::getRank(vIdLen_, key);
        auto dstId = NebulaKeyUtils::getDstId(vIdLen_, key);
        if (!firstLoop && rank == lastRank && lastDstId == dstId.str()) {
            VLOG(1) << "Only get the latest version for each edge.";
            continue;
        }
        lastRank = rank;
        lastDstId = dstId.str();

        ret = proc(&reader, key, val);
        if (ret == kvstore::ResultCode::ERR_RESULT_EXPIRED) {
            continue;
        } else if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        edgeRowCount++;
        if (firstLoop) {
            firstLoop = false;
        }
    }

    // return not found if none of the edges is valid, so we can add a null field to resp row
    return edgeRowCount == count ? kvstore::ResultCode::ERR_KEY_NOT_FOUND :
                                   kvstore::ResultCode::SUCCEEDED;
}

std::vector<PropStat> GetNeighborsProcessor::initStatValue() {
    if (statCount_ <= 0) {
        return {};
    }

    // initialize all stat value of all edgeTypes
    std::vector<PropStat> stats;
    stats.resize(statCount_);
    for (const auto& ec : edgeContexts_) {
        for (const auto& ctx : ec.second) {
            if (ctx.hasStat_) {
                PropStat stat(ctx.statType_);
                stats[ctx.statIndex_] = std::move(stat);
            }
        }
    }
    return stats;
}

nebula::Value GetNeighborsProcessor::calculateStat(const std::vector<PropStat>& stats) {
    if (statCount_ <= 0) {
        return NullType::__NULL__;
    }
    nebula::List result;
    result.values.reserve(statCount_);
    for (const auto& stat : stats) {
        if (stat.statType_ == cpp2::StatType::SUM) {
            result.values.emplace_back(stat.sum_);
        } else if (stat.statType_ == cpp2::StatType::COUNT) {
            result.values.emplace_back(stat.count_);
        } else if (stat.statType_ == cpp2::StatType::AVG) {
            if (stat.count_ > 0) {
                result.values.emplace_back(stat.sum_ / stat.count_);
            } else {
                result.values.emplace_back(NullType::NaN);
            }
        }
    }
    return result;
}

void GetNeighborsProcessor::onProcessFinished() {
    resp_.set_vertices(std::move(resultDataSet_));
}


}  // namespace storage
}  // namespace nebula
