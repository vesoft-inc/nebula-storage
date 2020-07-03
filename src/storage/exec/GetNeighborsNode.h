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

namespace nebula {
namespace storage {

// GetNeighborsNode will generate a row in response of GetNeighbors, so it need to get the tag
// result from HashJoinNode, and the stat info and edge iterator from AggregateNode. Then collect
// some edge props, and put them into the target cell of a row.
class GetNeighborsNode : public QueryNode<VertexID> {
    FRIEND_TEST(ScanEdgePropBench, ProcessEdgeProps);

public:
    GetNeighborsNode(PlanContext* planCtx,
                     HashJoinNode* hashJoinNode,
                     AggregateNode* aggregateNode,
                     EdgeContext* edgeContext,
                     nebula::DataSet* resultDataSet,
                     int64_t limit = 0)
        : planContext_(planCtx)
        , hashJoinNode_(hashJoinNode)
        , aggregateNode_(aggregateNode)
        , edgeContext_(edgeContext)
        , resultDataSet_(resultDataSet)
        , limit_(limit) {}

    kvstore::ResultCode execute(PartitionID partId, const VertexID& vId) override {
        auto ret = RelNode::execute(partId, vId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        std::vector<Value> row;
        // vertexId is the first column
        row.emplace_back(vId);
        // second column is reserved for stat
        row.emplace_back(NullType::__NULL__);

        auto tagResult = hashJoinNode_->result().getList();
        for (auto& value : tagResult.values) {
            row.emplace_back(std::move(value));
        }

        // add default null for each edge node and the last column of yield expression
        row.resize(row.size() + edgeContext_->propContexts_.size() + 1,
                   NullType::__NULL__);

        ret = iterateEdges(row);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        aggregateNode_->calculateStat();
        if (aggregateNode_->result().type() == Value::Type::LIST) {
            // set stat list to second columns
            row[1].setList(aggregateNode_->mutableResult().moveList());
        }

        resultDataSet_->rows.emplace_back(std::move(row));
        return kvstore::ResultCode::SUCCEEDED;
    }

protected:
    GetNeighborsNode() = default;

    virtual kvstore::ResultCode iterateEdges(std::vector<Value>& row) {
        int64_t edgeRowCount = 0;
        nebula::List list;
        for (; aggregateNode_->valid(); aggregateNode_->next(), ++edgeRowCount) {
            if (limit_ > 0 && edgeRowCount >= limit_) {
                return kvstore::ResultCode::SUCCEEDED;
            }
            auto edgeType = aggregateNode_->edgeType();
            auto key = aggregateNode_->key();
            auto reader = aggregateNode_->reader();
            auto props = aggregateNode_->props();
            auto columnIdx = aggregateNode_->idx();
            const auto& edgeName = aggregateNode_->edgeName();

            // collect props need to return
            auto ret = collectEdgeProps(edgeType,
                                        edgeName,
                                        reader,
                                        key,
                                        planContext_->vIdLen_,
                                        props,
                                        list);
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                return ret;
            }

            // add edge prop value to the target column
            if (row[columnIdx].type() == Value::Type::NULLVALUE) {
                row[columnIdx].setList(nebula::List());
            }
            auto& cell = row[columnIdx].mutableList();
            cell.values.emplace_back(std::move(list));
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

    PlanContext* planContext_;
    HashJoinNode* hashJoinNode_;
    AggregateNode* aggregateNode_;
    EdgeContext* edgeContext_;
    nebula::DataSet* resultDataSet_;
    int64_t limit_;
};

class GetNeighborsSampleNode : public GetNeighborsNode {
public:
    GetNeighborsSampleNode(PlanContext* planCtx,
                           HashJoinNode* hashJoinNode,
                           AggregateNode* aggregateNode,
                           EdgeContext* edgeContext,
                           nebula::DataSet* resultDataSet,
                           int64_t limit)
        : GetNeighborsNode(planCtx, hashJoinNode, aggregateNode, edgeContext, resultDataSet, limit)
        , sampler_(std::make_unique<nebula::algorithm::ReservoirSampling<Sample>>(limit_)) {}

private:
    using Sample = std::tuple<EdgeType,
                              std::string,
                              std::string,
                              std::string,
                              const std::vector<PropContext>*,
                              size_t>;

    kvstore::ResultCode iterateEdges(std::vector<Value>& row) override {
        int64_t edgeRowCount = 0;
        nebula::List list;
        for (; aggregateNode_->valid(); aggregateNode_->next(), ++edgeRowCount) {
            auto edgeType = aggregateNode_->edgeType();
            const auto& edgeName = aggregateNode_->edgeName();
            auto val = aggregateNode_->val();
            auto key = aggregateNode_->key();
            auto props = aggregateNode_->props();
            auto columnIdx = aggregateNode_->idx();

            sampler_->sampling(std::make_tuple(
                edgeType, edgeName, val.str(), key.str(), props, columnIdx));
        }

        std::unique_ptr<RowReader> reader;
        auto samples = std::move(*sampler_).samples();
        for (auto& sample : samples) {
            auto edgeType = std::get<0>(sample);
            auto val = std::get<2>(sample);
            auto columnIdx = std::get<5>(sample);
            // add edge prop value to the target column
            if (row[columnIdx].type() == Value::Type::NULLVALUE) {
                row[columnIdx].setList(nebula::List());
            }

            if (!reader) {
                reader = RowReader::getEdgePropReader(planContext_->env_->schemaMan_,
                                                      planContext_->spaceId_,
                                                      std::abs(edgeType),
                                                      val);
                if (!reader) {
                    continue;
                }
            } else if (!reader->resetEdgePropReader(planContext_->env_->schemaMan_,
                                                    planContext_->spaceId_,
                                                    std::abs(edgeType),
                                                    val)) {
                continue;
            }

            auto ret = collectEdgeProps(std::get<0>(sample),
                                        std::get<1>(sample),
                                        reader.get(),
                                        std::get<3>(sample),
                                        planContext_->vIdLen_,
                                        std::get<4>(sample),
                                        list);
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                continue;
            }
            auto& cell = row[columnIdx].mutableList();
            cell.values.emplace_back(std::move(list));
        }

        return kvstore::ResultCode::SUCCEEDED;
    }

    std::unique_ptr<nebula::algorithm::ReservoirSampling<Sample>> sampler_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_GETNEIGHBORSNODE_H_
