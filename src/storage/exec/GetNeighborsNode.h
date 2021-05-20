/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_GETNEIGHBORSNODE_H_
#define STORAGE_EXEC_GETNEIGHBORSNODE_H_

#include "common/base/Base.h"
#include "common/algorithm/ReservoirSampling.h"
#include "storage/exec/AggregateNode.h"
#include "storage/exec/HashJoinNode.h"
#include "storage/StorageFlags.h"


namespace nebula {
namespace storage {

// GetNeighborsNode will generate a row in response of GetNeighbors, so it need to get the tag
// result from HashJoinNode, and the stat info and edge iterator from AggregateNode. Then collect
// some edge props, and put them into the target cell of a row.
template<typename T>
class GetNeighborsNode : public QueryNode<T> {
public:
    using RelNode<T>::execute;

    GetNeighborsNode(PlanContext* planCtx,
                     IterateNode<T>* hashJoinNode,
                     IterateNode<T>* upstream,
                     EdgeContext* edgeContext,
                     nebula::DataSet* resultDataSet,
                     int64_t limit = 0)
        : planContext_(planCtx)
        , hashJoinNode_(hashJoinNode)
        , upstream_(upstream)
        , edgeContext_(edgeContext)
        , resultDataSet_(resultDataSet)
        , limit_(limit) {}

    nebula::cpp2::ErrorCode execute(PartitionID partId, const T& input) override {
        VertexID vId;
        if constexpr (std::is_same_v<T, VertexID>) {
            vId = input;
        } else if constexpr (std::is_same_v<T, std::pair<VertexID, VertexID>>) {
            vId = input.first;
        } else {
            LOG(FATAL) << "Invalid key type.";
        }
        auto ret = RelNode<T>::execute(partId, input);
        if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
            return ret;
        }

        if (planContext_->resultStat_ == ResultStatus::ILLEGAL_DATA) {
            return nebula::cpp2::ErrorCode::E_INVALID_DATA;
        }

        std::vector<Value> row;
        // vertexId is the first column
        if (planContext_->isIntId_) {
            row.emplace_back(*reinterpret_cast<const int64_t*>(vId.data()));
        } else {
            row.emplace_back(vId);
        }
        // second column is reserved for stat
        row.emplace_back(Value());

        auto tagResult = hashJoinNode_->result().getList();
        for (auto& value : tagResult.values) {
            row.emplace_back(std::move(value));
        }

        // add default null for each edge node and the last column of yield expression
        row.resize(row.size() + edgeContext_->propContexts_.size() + 1, Value());

        ret = iterateEdges(row);
        if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
            return ret;
        }

        if (edgeContext_->statCount_ > 0) {
            auto agg = dynamic_cast<AggregateNode<VertexID>*>(upstream_);
            CHECK_NOTNULL(agg);
            agg->calculateStat();
            // set stat list to second columns
            row[1].setList(agg->mutableResult().moveList());
        }

        resultDataSet_->rows.emplace_back(std::move(row));
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    }

protected:
    GetNeighborsNode() = default;

    virtual nebula::cpp2::ErrorCode iterateEdges(std::vector<Value>& row) {
        int64_t edgeRowCount = 0;
        nebula::List list;
        for (; upstream_->valid(); upstream_->next(), ++edgeRowCount) {
            if (edgeRowCount >= limit_) {
                return nebula::cpp2::ErrorCode::SUCCEEDED;
            }
            auto key = upstream_->key();
            auto reader = upstream_->reader();
            auto props = planContext_->props_;
            auto columnIdx = planContext_->columnIdx_;

            list.reserve(props->size());
            // collect props need to return
            if (!QueryUtils::collectEdgeProps(key, planContext_->vIdLen_, planContext_->isIntId_,
                                              reader, props, list).ok()) {
                return nebula::cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
            }

            // add edge prop value to the target column
            if (row[columnIdx].empty()) {
                row[columnIdx].setList(nebula::List());
            }
            auto& cell = row[columnIdx].mutableList();
            cell.values.emplace_back(std::move(list));
        }
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    }

    PlanContext* planContext_;
    IterateNode<T>* hashJoinNode_;
    IterateNode<T>* upstream_;
    EdgeContext* edgeContext_;
    nebula::DataSet* resultDataSet_;
    int64_t limit_;
};

template<typename T>
class GetNeighborsSampleNode : public GetNeighborsNode<T> {
public:
    GetNeighborsSampleNode(PlanContext* planCtx,
                           IterateNode<T>* hashJoinNode,
                           IterateNode<T>* upstream,
                           EdgeContext* edgeContext,
                           nebula::DataSet* resultDataSet,
                           int64_t limit)
        : GetNeighborsNode<T>(planCtx, hashJoinNode, upstream, edgeContext, resultDataSet, limit) {
            sampler_ = std::make_unique<nebula::algorithm::ReservoirSampling<Sample>>(limit);
        }

private:
    using Sample = std::tuple<EdgeType,
                              std::string,
                              std::string,
                              const std::vector<PropContext>*,
                              size_t>;

    nebula::cpp2::ErrorCode iterateEdges(std::vector<Value>& row) override {
        int64_t edgeRowCount = 0;
        nebula::List list;
        for (; this->upstream_->valid(); this->upstream_->next(), ++edgeRowCount) {
            auto val = this->upstream_->val();
            auto key = this->upstream_->key();
            auto edgeType = this->planContext_->edgeType_;
            auto props = this->planContext_->props_;
            auto columnIdx = this->planContext_->columnIdx_;
            sampler_->sampling(std::make_tuple(edgeType, val.str(), key.str(), props, columnIdx));
        }

        RowReaderWrapper reader;
        auto samples = std::move(*sampler_).samples();
        for (auto& sample : samples) {
            auto columnIdx = std::get<4>(sample);
            // add edge prop value to the target column
            if (row[columnIdx].empty()) {
                row[columnIdx].setList(nebula::List());
            }

            auto edgeType = std::get<0>(sample);
            const auto& val = std::get<1>(sample);
            reader = RowReaderWrapper::getEdgePropReader(this->planContext_->env_->schemaMan_,
                                                         this->planContext_->spaceId_,
                                                         std::abs(edgeType),
                                                         val);
            if (!reader) {
                continue;
            }

            const auto& key = std::get<2>(sample);
            const auto& props = std::get<3>(sample);
            if (!QueryUtils::collectEdgeProps(key, this->planContext_->vIdLen_,
                                              this->planContext_->isIntId_,
                                              reader.get(), props, list).ok()) {
                return nebula::cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
            }
            auto& cell = row[columnIdx].mutableList();
            cell.values.emplace_back(std::move(list));
        }

        return nebula::cpp2::ErrorCode::SUCCEEDED;
    }

    std::unique_ptr<nebula::algorithm::ReservoirSampling<Sample>> sampler_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_GETNEIGHBORSNODE_H_
