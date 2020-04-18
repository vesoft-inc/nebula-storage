/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "storage/index/IndexExecutor.h"

DECLARE_int32(max_rows_returned_per_lookup);
DECLARE_bool(enable_vertex_cache);

namespace nebula {
namespace storage {

template <typename RESP>
cpp2::ErrorCode IndexExecutor<RESP>::prepareRequest(const cpp2::LookupIndexRequest &req) {
    // step 1 : setup space id
    spaceId_ = req.get_space_id();

    // step 2 : check and setup lookup type, such as tag index or edge index
    tagOrEdgeId_ = req.get_tag_or_edge_id();

    // step 3 : check and setup return columns.
    if (req.__isset.return_columns && !req.get_return_columns()->empty()) {
        returnColumns_ = *req.get_return_columns();
    }

    // TODO : (sky) get the fixed length VId len from space meta.
    vIdLen_ = 8;

    // step 5 : setup execution contexts.
    const auto& contexts = req.get_contexts();
    int32_t hintId = 1;
    for (const auto& context : contexts) {
        auto ret = buildExecutionPlan(hintId++, context);
        if (ret != cpp2::ErrorCode::SUCCEEDED) {
            LOG(ERROR) << "Build execution plan error , index id : " << context.get_index_id();
            return ret;
        }
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

template <typename RESP>
cpp2::ErrorCode
IndexExecutor<RESP>::buildExecutionPlan(int32_t hintId,
                                        const cpp2::IndexQueryContext& queryContext) {
    auto indexId = queryContext.get_index_id();

    StatusOr<std::shared_ptr<IndexItem>> index;
    if (isEdgeIndex_) {
        index = this->env_->indexMan_->getEdgeIndex(spaceId_, indexId);
    } else {
        index = this->env_->indexMan_->getTagIndex(spaceId_, indexId);
    }
    if (!index.ok()) {
        return cpp2::ErrorCode::E_INDEX_NOT_FOUND;
    }
    std::map<std::string, Value::Type> indexCols;
    auto ret = setupIndexColumns(indexCols, index.value());
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        return ret;
    }

    auto columnHints = queryContext.get_column_hints();
    auto executionPlan = std::make_unique<ExecutionPlan>(std::move(index).value(),
                                                         std::move(indexCols),
                                                         std::move(columnHints));
    executionPlans_[hintId] = std::move(executionPlan);

    return cpp2::ErrorCode::SUCCEEDED;
}

template <typename RESP>
cpp2::ErrorCode IndexExecutor<RESP>::setupIndexColumns(std::map<std::string, Value::Type> &cols,
                                                       std::shared_ptr<IndexItem> index) {
    const auto& columns = index->get_fields();
    for (const auto& colDef : columns) {
        auto type = toValueType(colDef.get_type());
        if (type == Value::Type::__EMPTY__) {
            VLOG(1) << "Invalid prop type , column : " << colDef.get_name();
            return cpp2::ErrorCode();
        }
        cols.emplace(colDef.get_name(), std::move(type));
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

template <typename RESP>
Value::Type IndexExecutor<RESP>::toValueType(nebula::meta::cpp2::PropertyType type) noexcept {
    switch (type) {
        case nebula::meta::cpp2::PropertyType::BOOL : {
            return Value::Type::BOOL;
        }
        case nebula::meta::cpp2::PropertyType::DATE : {
            return Value::Type::DATE;
        }
        case nebula::meta::cpp2::PropertyType::DATETIME : {
            return Value::Type::DATETIME;
        }
        case nebula::meta::cpp2::PropertyType::FIXED_STRING :
        case nebula::meta::cpp2::PropertyType::STRING : {
            return Value::Type::STRING;
        }
        case nebula::meta::cpp2::PropertyType::INT8 :
        case nebula::meta::cpp2::PropertyType::INT16 :
        case nebula::meta::cpp2::PropertyType::INT32 :
        case nebula::meta::cpp2::PropertyType::INT64 :
        case nebula::meta::cpp2::PropertyType::TIMESTAMP : {
            return Value::Type::INT;
        }
        case nebula::meta::cpp2::PropertyType::DOUBLE :
        case nebula::meta::cpp2::PropertyType::FLOAT : {
            return Value::Type::FLOAT;
        }
        default : {
            return Value::Type::__EMPTY__;
        }
    }
}

template <typename RESP>
kvstore::ResultCode IndexExecutor<RESP>::executeExecutionPlan(PartitionID part) {
    for (const auto& executionPlan : executionPlans_) {
        auto ret = executeIndexScan(executionPlan.first, part);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }
    }
    return nebula::kvstore::SUCCEEDED;
}

template <typename RESP>
kvstore::ResultCode IndexExecutor<RESP>::executeIndexScan(int32_t hintId, PartitionID part) {
    const auto& hint = executionPlans_[hintId];
    if (hint->isRangeScan()) {
        return executeIndexRangeScan(hintId, part);
    } else {
        return executeIndexPrefixScan(hintId, part);
    }
}

template <typename RESP>
kvstore::ResultCode IndexExecutor<RESP>::executeIndexRangeScan(int32_t hintId, PartitionID part) {
    const auto& hint = executionPlans_[hintId];
    auto rangeRet = hint->getRangeStartStr(part);
    if (!rangeRet.ok()) {
        return kvstore::ResultCode::ERR_UNKNOWN;
    }
    auto rang = std::move(rangeRet).value();
    std::unique_ptr<kvstore::KVIterator> iter;
    std::vector<std::string> keys;
    auto ret = this->env_->kvstore_->range(spaceId_, part, rang.first, rang.second, &iter);
    if (ret != nebula::kvstore::SUCCEEDED) {
        return ret;
    }
    while (iter->valid() &&
           rowNum_ < FLAGS_max_rows_returned_per_lookup) {
        auto key = iter->key();
        /**
         * Need to filter result with expression if is not accurate scan.
         */
        if (!hint->getFilter().empty() &&  !conditionsCheck(hintId, key)) {
            iter->next();
            continue;
        }
        keys.emplace_back(key);
        iter->next();
    }
    for (auto& item : keys) {
        ret = getDataRow(part, item);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }
    }
    return ret;
}

template <typename RESP>
kvstore::ResultCode IndexExecutor<RESP>::executeIndexPrefixScan(int32_t hintId, PartitionID part) {
    const auto& hint = executionPlans_[hintId];
    auto prefixRet = hint->getPrefixStr(part);
    if (!prefixRet.ok()) {
        return kvstore::ResultCode::ERR_UNKNOWN;
    }
    std::unique_ptr<kvstore::KVIterator> iter;
    std::vector<std::string> keys;
    auto ret = this->env_->kvstore_->prefix(spaceId_, part, prefixRet.value(), &iter);
    if (ret != nebula::kvstore::SUCCEEDED) {
        return ret;
    }
    while (iter->valid() &&
           rowNum_ < FLAGS_max_rows_returned_per_lookup) {
        auto key = iter->key();
        /**
         * Need to filter result with expression if is not accurate scan.
         */
        if (!hint->getFilter().empty() &&  !conditionsCheck(hintId, key)) {
            iter->next();
            continue;
        }
        keys.emplace_back(key);
        iter->next();
    }
    for (auto& item : keys) {
        ret = getDataRow(part, item);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }
    }
    return ret;
}

template<typename RESP>
kvstore::ResultCode IndexExecutor<RESP>::getDataRow(PartitionID partId,
                                                    const folly::StringPiece& key) {
    kvstore::ResultCode ret;
    if (isEdgeIndex_) {
        cpp2::EdgeIndexData data;
        ret = getEdgeRow(partId, key, &data);
        if (ret == kvstore::SUCCEEDED) {
            edgeRows_.emplace_back(std::move(data));
            ++rowNum_;
        }
    } else {
        cpp2::VertexIndexData data;
        ret = getVertexRow(partId, key, &data);
        if (ret == kvstore::SUCCEEDED) {
            vertexRows_.emplace_back(std::move(data));
            ++rowNum_;
        }
    }
    return ret;
}

template<typename RESP>
kvstore::ResultCode IndexExecutor<RESP>::getVertexRow(PartitionID partId,
                                                      const folly::StringPiece& key,
                                                      cpp2::VertexIndexData* data) {
    auto vId = IndexKeyUtils::getIndexVertexID(vIdLen_, key);
    data->set_id(vId.str());
    // DOTO : (sky) vertex cache
    auto prefix = NebulaKeyUtils::vertexPrefix(vIdLen_, partId, vId.str(), tagOrEdgeId_);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = this->env_->kvstore_->prefix(spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Error! ret = " << static_cast<int32_t>(ret) << ", spaceId " << spaceId_;
        return ret;
    }
    if (iter && iter->valid()) {
        auto reader = RowReader::getTagPropReader(this->env_->schemaMan_,
                                                  spaceId_,
                                                  tagOrEdgeId_,
                                                  iter->val());
        if (reader == nullptr) {
            return kvstore::ResultCode::ERR_UNKNOWN;
        }
        std::vector<Value> values;
        for (const auto col : returnColumns_) {
            auto v = reader->getValueByName(col);
            values.emplace_back(std::move(v));
        }
        data->set_props(std::move(values));
    } else {
        LOG(ERROR) << "Missed partId " << partId << ", vId " << vId << ", tagId " << tagOrEdgeId_;
        return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
    }
    
    return kvstore::ResultCode::SUCCEEDED;
}

template<typename RESP>
kvstore::ResultCode IndexExecutor<RESP>::getEdgeRow(PartitionID partId,
                                                    const folly::StringPiece& key,
                                                    cpp2::EdgeIndexData* data) {
    auto src = IndexKeyUtils::getIndexSrcId(0, key);
    auto rank = IndexKeyUtils::getIndexRank(0, key);
    auto dst = IndexKeyUtils::getIndexDstId(0, key);
    cpp2::EdgeKey edge;
    edge.set_src(src.data());
    edge.set_edge_type(tagOrEdgeId_);
    edge.set_ranking(rank);
    edge.set_dst(dst.data());
    data->set_edge(edge);
    
    if (returnColumns_.empty()) {
        return kvstore::ResultCode::SUCCEEDED;
    }

    auto prefix = NebulaKeyUtils::edgePrefix(vIdLen_, partId, src.str(),
                                             tagOrEdgeId_, rank, dst.str());
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = this->env_->kvstore_->prefix(spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Error! ret = "
                   << static_cast<int32_t>(ret)
                   << ", spaceId " << spaceId_;
        return ret;
    }
    if (iter && iter->valid()) {
        auto reader = RowReader::getEdgePropReader(this->env_->schemaMan_,
                                                   spaceId_,
                                                   tagOrEdgeId_,
                                                   iter->val());
        if (reader == nullptr) {
            return kvstore::ResultCode::ERR_UNKNOWN;
        }
        std::vector<Value> values;
        for (const auto col : returnColumns_) {
            auto v = reader->getValueByName(col);
            values.emplace_back(std::move(v));
        }
        data->set_props(std::move(values));
    } else {
        LOG(ERROR) << "Missed partId " << partId
                   << ", src " << src << ", edgeType "
                   << tagOrEdgeId_ << ", Rank "
                   << rank << ", dst " << dst;
        return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
    }
    return kvstore::ResultCode::SUCCEEDED;
}

template<typename RESP>
bool IndexExecutor<RESP>::conditionsCheck(int32_t, const folly::StringPiece&) {
    // TODO : BaseContext for expression
    return true;
}

template<typename RESP>
StatusOr<Value> IndexExecutor<RESP>::decodeValue(int32_t hintId,
                                                 const folly::StringPiece& key,
                                                 const folly::StringPiece& prop) {
    auto type = executionPlans_[hintId]->getIndexColType(prop);
    /**
     * Here need a string copy to avoid memory change
     */
    auto propVal = getIndexVal(hintId, key, prop).str();
    return IndexKeyUtils::decodeValue(std::move(propVal), type);
}

template<typename RESP>
folly::StringPiece IndexExecutor<RESP>::getIndexVal(int32_t hintId,
                                                    const folly::StringPiece& key,
                                                    const folly::StringPiece& prop) {
    auto tailLen = (!isEdgeIndex_) ? vIdLen_ :
                                     vIdLen_ * 2 + sizeof(EdgeRanking);
    using nebula::meta::cpp2::PropertyType;
    size_t offset = sizeof(PartitionID) + sizeof(IndexID);
    size_t len = 0;
    int32_t vCount = vColNum_;
    for (const auto& col : executionPlans_[hintId]->getIndex()->get_fields()) {
        switch (col.get_type()) {
            case PropertyType::BOOL: {
                len = sizeof(bool);
                break;
            }
            case PropertyType::TIMESTAMP:
            case PropertyType::INT64: {
                len = sizeof(int64_t);
                break;
            }
            case PropertyType::FLOAT:
            case PropertyType::DOUBLE: {
                len = sizeof(double);
                break;
            }
            case PropertyType::DATE: {
                len = sizeof(nebula::Date);
                break;
            }
            case PropertyType::DATETIME: {
                len = sizeof(nebula::DateTime);
                break;
            }
            case PropertyType::STRING:
            case PropertyType::FIXED_STRING: {
                auto off = key.size() - vCount * sizeof(int32_t) - tailLen;
                len = *reinterpret_cast<const int32_t*>(key.data() + off);
                --vCount;
                break;
            }
            default:
                len = 0;
        }
        if (col.get_name() == prop.str()) {
            break;
        }
        offset += len;
    }
    return key.subpiece(offset, len);
}

}  // namespace storage
}  // namespace nebula
