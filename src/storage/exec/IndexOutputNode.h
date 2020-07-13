/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef STORAGE_EXEC_INDEXOUTPUTNODE_H_
#define STORAGE_EXEC_INDEXOUTPUTNODE_H_

#include "common/base/Base.h"
#include "storage/exec/RelNode.h"

namespace nebula {
namespace storage {

template<typename T>
class IndexOutputNode final : public RelNode<T> {
public:
    enum class IndexResultType : int8_t {
        edgeFromIndexScan,
        edgeFromIndexFilter,
        edgeFromDataScan,
        edgeFromDataFilter,
        vertexFromIndexScan,
        vertexFromIndexFilter,
        vertexFromDataScan,
        vertexFromDataFilter,
    };

    IndexOutputNode(nebula::DataSet* result,
                    PlanContext* planCtx,
                    IndexScanNode<T>* indexScanNode,
                    std::vector<std::pair<std::string, Value::Type>>& cols,
                    int32_t vColNum,
                    bool hasNullableCol)
        : result_(result)
        , planContext_(planCtx)
        , indexScanNode_(indexScanNode)
        , cols_(std::move(cols))
        , vColNum_(vColNum)
        , hasNullableCol_(hasNullableCol) {
        type_ = planContext_->isEdge_
            ? IndexResultType::edgeFromIndexScan
            : IndexResultType::vertexFromIndexScan;
    }

    IndexOutputNode(nebula::DataSet* result,
                    PlanContext* planCtx,
                    IndexEdgeNode<T>* indexEdgeNode)
        : result_(result)
        , planContext_(planCtx)
        , indexEdgeNode_(indexEdgeNode) {
        type_ = IndexResultType::edgeFromDataScan;
    }

    IndexOutputNode(nebula::DataSet* result,
                    PlanContext* planCtx,
                    IndexVertexNode<T>* indexVertexNode)
        : result_(result)
        , planContext_(planCtx)
        , indexVertexNode_(indexVertexNode) {
        type_ = IndexResultType::vertexFromDataScan;
    }

    IndexOutputNode(nebula::DataSet* result,
                    PlanContext* planCtx,
                    IndexFilterNode<T>* indexFilterNode,
                    bool indexFilter = false)
        : result_(result)
        , planContext_(planCtx)
        , indexFilterNode_(indexFilterNode) {
        vColNum_ = indexFilterNode_->vColNum();
        hasNullableCol_ = indexFilterNode->hasNullableCol();
        cols_ = indexFilterNode_->indexCols();
        if (indexFilter) {
            type_ = planContext_->isEdge_
                    ? IndexResultType::edgeFromIndexFilter
                    : IndexResultType::vertexFromIndexFilter;
        } else {
            type_ = planContext_->isEdge_
                    ? IndexResultType::edgeFromDataFilter
                    : IndexResultType::vertexFromDataFilter;
        }
    }

    kvstore::ResultCode execute(PartitionID partId) override {
        auto ret = RelNode<T>::execute(partId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        switch (type_) {
            case IndexResultType::edgeFromIndexScan : {
                ret = collectResult(indexScanNode_->getData());
                break;
            }
            case IndexResultType::edgeFromIndexFilter : {
                ret = collectResult(indexFilterNode_->getData());
                break;
            }
            case IndexResultType::edgeFromDataScan : {
                ret = collectResult(indexEdgeNode_->getData());
                break;
            }
            case IndexResultType::edgeFromDataFilter : {
                ret = collectResult(indexFilterNode_->getData());
                break;
            }
            case IndexResultType::vertexFromIndexScan : {
                ret = collectResult(indexScanNode_->getData());
                break;
            }
            case IndexResultType::vertexFromIndexFilter : {
                ret = collectResult(indexFilterNode_->getData());
                break;
            }
            case IndexResultType::vertexFromDataScan : {
                ret = collectResult(indexVertexNode_->getData());
                break;
            }
            case IndexResultType::vertexFromDataFilter : {
                ret = collectResult(indexFilterNode_->getData());
                break;
            }
        }
        return ret;
    }

private:
    kvstore::ResultCode collectResult(const std::vector<kvstore::KV>& data) {
        kvstore::ResultCode ret = kvstore::ResultCode::SUCCEEDED;
        switch (type_) {
            case IndexResultType::edgeFromIndexScan :
            case IndexResultType::edgeFromIndexFilter : {
                ret = edgeRowsFromIndex(data);
                break;
            }
            case IndexResultType::edgeFromDataScan :
            case IndexResultType::edgeFromDataFilter : {
                ret = edgeRowsFromData(data);
                break;
            }
            case IndexResultType::vertexFromIndexScan :
            case IndexResultType::vertexFromIndexFilter : {
                ret = vertexRowsFromIndex(data);
                break;
            }
            case IndexResultType::vertexFromDataScan :
            case IndexResultType::vertexFromDataFilter : {
                ret = vertexRowsFromData(data);
                break;
            }
        }
        return ret;
    }

    kvstore::ResultCode vertexRowsFromData(const std::vector<kvstore::KV>& data) {
        const auto* schema = type_ == IndexResultType::vertexFromDataScan
                             ? indexVertexNode_->getSchema()
                             : indexFilterNode_->getSchema();
        if (schema == nullptr) {
            return kvstore::ResultCode::ERR_TAG_NOT_FOUND;
        }
        auto returnCols = result_->colNames;
        for (const auto& val : data) {
            Row row;
            auto vId = NebulaKeyUtils::getVertexId(planContext_->vIdLen_, val.first);
            row.emplace_back(Value(vId));
            auto reader = RowReader::getRowReader(schema, val.second);
            if (!reader) {
                VLOG(1) << "Can't get tag reader";
                return kvstore::ResultCode::ERR_TAG_NOT_FOUND;
            }
            // skip vertexID
            for (size_t i = 1; i < returnCols.size(); i++) {
                auto v = reader->getValueByName(returnCols[i]);
                row.emplace_back(std::move(v));
            }
            result_->rows.emplace_back(std::move(row));
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

    kvstore::ResultCode vertexRowsFromIndex(const std::vector<kvstore::KV>& data) {
        auto returnCols = result_->colNames;
        for (const auto& val : data) {
            Row row;
            auto vId = IndexKeyUtils::getIndexVertexID(planContext_->vIdLen_, val.first);
            row.emplace_back(Value(vId));

            // skip vertexID
            for (size_t i = 1; i < returnCols.size(); i++) {
                auto v = IndexKeyUtils::getValueFromIndexKey(planContext_->vIdLen_,
                                                             vColNum_,
                                                             val.first,
                                                             returnCols[i],
                                                             cols_,
                                                             false,
                                                             hasNullableCol_);
                row.emplace_back(std::move(v));
            }
            result_->rows.emplace_back(std::move(row));
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

    kvstore::ResultCode edgeRowsFromData(const std::vector<kvstore::KV>& data) {
        const auto* schema = type_ == IndexResultType::edgeFromDataScan
                             ? indexEdgeNode_->getSchema()
                             : indexFilterNode_->getSchema();
        if (schema == nullptr) {
            return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
        }
        auto returnCols = result_->colNames;
        for (const auto& val : data) {
            Row row;
            auto src = NebulaKeyUtils::getSrcId(planContext_->vIdLen_, val.first);
            auto rank = NebulaKeyUtils::getRank(planContext_->vIdLen_, val.first);
            auto dst = NebulaKeyUtils::getDstId(planContext_->vIdLen_, val.first);
            row.emplace_back(Value(src));
            row.emplace_back(Value(rank));
            row.emplace_back(Value(dst));
            auto reader = RowReader::getRowReader(schema, val.second);
            if (!reader) {
                VLOG(1) << "Can't get tag reader";
                return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
            }
            // skip column src_ , edgeType, ranking, dst_
            for (size_t i = 3; i < returnCols.size(); i++) {
                auto v = reader->getValueByName(returnCols[i]);
                row.emplace_back(std::move(v));
            }
            result_->rows.emplace_back(std::move(row));
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

    kvstore::ResultCode edgeRowsFromIndex(const std::vector<kvstore::KV>& data) {
        auto returnCols = result_->colNames;
        for (const auto& val : data) {
            Row row;
            auto src = IndexKeyUtils::getIndexSrcId(planContext_->vIdLen_, val.first);
            auto rank = IndexKeyUtils::getIndexRank(planContext_->vIdLen_, val.first);
            auto dst = IndexKeyUtils::getIndexDstId(planContext_->vIdLen_, val.first);

            row.emplace_back(Value(std::move(src)));
            row.emplace_back(Value(std::move(rank)));
            row.emplace_back(Value(std::move(dst)));

            // skip column src_ , ranking, dst_
            for (size_t i = 3; i < returnCols.size(); i++) {
                auto v = IndexKeyUtils::getValueFromIndexKey(planContext_->vIdLen_,
                                                             vColNum_,
                                                             val.first,
                                                             returnCols[i],
                                                             cols_,
                                                             true,
                                                             hasNullableCol_);
                row.emplace_back(std::move(v));
            }
            result_->rows.emplace_back(std::move(row));
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

private:
    nebula::DataSet*                                  result_;
    PlanContext*                                      planContext_;
    IndexResultType                                   type_;
    IndexScanNode<T>*                                 indexScanNode_{nullptr};
    IndexEdgeNode<T>*                                 indexEdgeNode_{nullptr};
    IndexVertexNode<T>*                               indexVertexNode_{nullptr};
    IndexFilterNode<T>*                               indexFilterNode_{nullptr};
    std::vector<std::pair<std::string, Value::Type>>  cols_{};
    int32_t                                           vColNum_{};
    bool                                              hasNullableCol_{};
};

}  // namespace storage
}  // namespace nebula

#endif   // STORAGE_EXEC_INDEXOUTPUTNODE_H_
