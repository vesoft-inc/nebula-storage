/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef STORAGE_EXEC_INDEXOUTPUTNODE_H_
#define STORAGE_EXEC_INDEXOUTPUTNODE_H_

#include <utility>

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
                    size_t vIdLen,
                    IndexScanNode<T>* indexScanNode,
                    std::vector<std::pair<std::string, Value::Type>>& cols,
                    int32_t vColNum,
                    bool hasNullableCol,
                    bool isEdge)
        : result_(result)
        , vIdLen_(vIdLen)
        , indexScanNode_(indexScanNode)
        , cols_(std::move(cols))
        , vColNum_(vColNum)
        , hasNullableCol_(hasNullableCol) {
        type_ = isEdge ? IndexResultType::edgeFromIndexScan : IndexResultType::vertexFromIndexScan;
    }

    IndexOutputNode(nebula::DataSet* result,
                    size_t vIdLen,
                    IndexEdgeNode<T>* indexEdgeNode,
                    std::shared_ptr<const meta::NebulaSchemaProvider> schema)
        : result_(result)
        , vIdLen_(vIdLen)
        , indexEdgeNode_(indexEdgeNode)
        , schema_(std::move(schema)) {
        type_ = IndexResultType::edgeFromDataScan;
    }

    IndexOutputNode(nebula::DataSet* result,
                    size_t vIdLen,
                    IndexVertexNode<T>* indexVertexNode,
                    std::shared_ptr<const meta::NebulaSchemaProvider> schema)
        : result_(result)
        , vIdLen_(vIdLen)
        , indexVertexNode_(indexVertexNode)
        , schema_(std::move(schema)) {
        type_ = IndexResultType::vertexFromDataScan;
    }

    IndexOutputNode(nebula::DataSet* result,
                    size_t vIdLen,
                    IndexFilterNode<T>* indexFilterNode,
                    std::shared_ptr<const meta::NebulaSchemaProvider> schema,
                    bool isEdge)
        : result_(result)
        , vIdLen_(vIdLen)
        , indexFilterNode_(indexFilterNode)
        , schema_(std::move(schema)) {
        type_ = isEdge ? IndexResultType::edgeFromDataFilter
                       : IndexResultType::vertexFromDataFilter;
    }

    IndexOutputNode(nebula::DataSet* result,
                    size_t vIdLen,
                    IndexFilterNode<T>* indexFilterNode,
                    bool isEdge)
        : result_(result)
        , vIdLen_(vIdLen)
        , indexFilterNode_(indexFilterNode) {
        type_ = isEdge ? IndexResultType::edgeFromIndexFilter
                       : IndexResultType::vertexFromIndexFilter;
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
    kvstore::ResultCode collectResult(const std::vector<std::string>& datas) {
        kvstore::ResultCode ret;
        switch (type_) {
            case IndexResultType::edgeFromIndexScan :
            case IndexResultType::edgeFromIndexFilter : {
                ret = edgeRowsFromIndex(datas);
                break;
            }
            case IndexResultType::edgeFromDataScan :
            case IndexResultType::edgeFromDataFilter : {
                ret = edgeRowsFromData(datas);
                break;
            }
            case IndexResultType::vertexFromIndexScan :
            case IndexResultType::vertexFromIndexFilter : {
                ret = vertexRowsFromIndex(datas);
                break;
            }
            case IndexResultType::vertexFromDataScan :
            case IndexResultType::vertexFromDataFilter : {
                ret = vertexRowsFromData(datas);
                break;
            }
        }
        return ret;
    }

    kvstore::ResultCode vertexRowsFromData(const std::vector<std::string>&) {
        if (schema_ == nullptr) {
            return kvstore::ResultCode::ERR_TAG_NOT_FOUND;
        }
        auto datas = type_ == IndexResultType::vertexFromDataScan
                     ? indexVertexNode_->getData()
                     : indexFilterNode_->getData();
        auto returnCols = result_->colNames;
        for (const auto& data : datas) {
            Row row;
            auto vId = NebulaKeyUtils::getVertexId(vIdLen_, data);
            row.emplace_back(Value(vId));
            auto reader = RowReader::getRowReader(schema_.get(), data);
            if (!reader) {
                VLOG(1) << "Can't get tag reader";
                return kvstore::ResultCode::ERR_TAG_NOT_FOUND;
            }
            // skip vertexID
            for (auto i = 1; i < returnCols.size(); i++) {
                auto v = reader->getValueByName(returnCols[i]);
                row.emplace_back(std::move(v));
            }
            result_->rows.emplace_back(std::move(row));
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

    kvstore::ResultCode vertexRowsFromIndex(const std::vector<std::string>&) {
        auto datas = type_ == IndexResultType::vertexFromIndexScan
                              ? indexScanNode_->getData()
                              : indexFilterNode_->getData();
        auto returnCols = result_->colNames;
        for (const auto& data : datas) {
            Row row;
            auto vId = IndexKeyUtils::getIndexVertexID(vIdLen_, data);
            row.emplace_back(Value(vId));

            // skip vertexID
            for (auto i = 1; i < returnCols.size(); i++) {
                auto v = IndexKeyUtils::getValueFromIndexKey(vIdLen_,
                                                             vColNum_,
                                                             data,
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

    kvstore::ResultCode edgeRowsFromData(const std::vector<std::string>&) {
        if (schema_ == nullptr) {
            return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
        }
        auto datas = type_ == IndexResultType::edgeFromDataScan
                     ? indexEdgeNode_->getData()
                     : indexFilterNode_->getData();
        auto returnCols = result_->colNames;
        for (const auto& data : datas) {
            Row row;
            auto src = NebulaKeyUtils::getSrcId(vIdLen_, data);
            auto edgeType = NebulaKeyUtils::getEdgeType(vIdLen_, data);
            auto rank = NebulaKeyUtils::getRank(vIdLen_, data);
            auto dst = NebulaKeyUtils::getDstId(vIdLen_, data);
            row.emplace_back(Value(src));
            row.emplace_back(Value(edgeType));
            row.emplace_back(Value(rank));
            row.emplace_back(Value(dst));
            auto reader = RowReader::getRowReader(schema_.get(), data);
            if (!reader) {
                VLOG(1) << "Can't get tag reader";
                return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
            }
            // skip column src_ , edgeType, ranking, dst_
            for (auto i = 4; i < returnCols.size(); i++) {
                auto v = reader->getValueByName(returnCols[i]);
                row.emplace_back(std::move(v));
            }
            result_->rows.emplace_back(std::move(row));
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

    kvstore::ResultCode edgeRowsFromIndex(const std::vector<std::string>&) {
        auto datas = type_ == IndexResultType::edgeFromIndexScan
                     ? indexScanNode_->getData()
                     : indexFilterNode_->getData();
        auto returnCols = result_->colNames;
        for (const auto& data : datas) {
            Row row;
            auto src = IndexKeyUtils::getIndexSrcId(vIdLen_, data);
            auto rank = IndexKeyUtils::getIndexRank(vIdLen_, data);
            auto dst = IndexKeyUtils::getIndexDstId(vIdLen_, data);

            row.emplace_back(Value(std::move(src)));
            // TODO (sky) : get edgeType.
            row.emplace_back(Value(0));
            row.emplace_back(Value(std::move(rank)));
            row.emplace_back(Value(std::move(dst)));

            // skip column src_ , edgeType, ranking, dst_
            for (auto i = 4; i < returnCols.size(); i++) {
                auto v = IndexKeyUtils::getValueFromIndexKey(vIdLen_,
                                                             vColNum_,
                                                             data,
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
    size_t                                            vIdLen_;
    IndexResultType                                   type_;
    IndexScanNode<T>*                                 indexScanNode_{nullptr};
    IndexEdgeNode<T>*                                 indexEdgeNode_{nullptr};
    IndexVertexNode<T>*                               indexVertexNode_{nullptr};
    IndexFilterNode<T>*                               indexFilterNode_{nullptr};
    std::vector<std::pair<std::string, Value::Type>>  cols_{};
    int32_t                                           vColNum_{};
    bool                                              hasNullableCol_{};
    std::shared_ptr<const meta::NebulaSchemaProvider> schema_{nullptr};
};

}  // namespace storage
}  // namespace nebula

#endif   // STORAGE_EXEC_INDEXOUTPUTNODE_H_
