/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_AGGREGATENODE_H_
#define STORAGE_EXEC_AGGREGATENODE_H_

#include "common/base/Base.h"
#include "storage/exec/RelNode.h"
#include "storage/exec/GetNeighborsNode.h"

namespace nebula {
namespace storage {

// AggregateNode is the node which would generate a DataSet of Response, most likely it would use
// the result of QueryNode, do some works, and then put it into the output DataSet.
// Some other AggregateNode will receive the whole DataSet, do some works, and output again, such
// as SortNode and GroupByNode
template<typename T>
class AggregateNode : public RelNode<T> {
public:
    AggregateNode(QueryNode<T>* node,
                  nebula::DataSet* result)
        : node_(node)
        , result_(result) {}

    explicit AggregateNode(nebula::DataSet* result)
        : result_(result) {}

    kvstore::ResultCode execute(PartitionID partId, const T& input) override {
        auto ret = RelNode<T>::execute(partId, input);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        // The AggregateNode just get the result of QueryNode, add it to DataSet
        const auto& row = node_->result();
        result_->rows.emplace_back(std::move(row));
        return kvstore::ResultCode::SUCCEEDED;
    }

protected:
    QueryNode<T>* node_;
    nebula::DataSet* result_;
};

// StatNode will only be used in GetNeighbors for now, it need to calculate some stat of a vertex
class StatNode final : public AggregateNode<VertexID> {
public:
    StatNode(GetNeighborsNode* node, nebula::DataSet* result, EdgeContext* edgeContext)
        : AggregateNode(node, result)
        , edgeContext_(edgeContext)
        , rowNode_(node) {}

    kvstore::ResultCode execute(PartitionID partId, const VertexID& vId) override {
        auto ret = RelNode::execute(partId, vId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }
        auto& row = rowNode_->mutableResult();
        const auto& stats = rowNode_->stats();
        if (edgeContext_->statCount_ > 0) {
            // set the stat result to column[1]
            auto values = calculateStat(stats);
            row.columns[1].setList(std::move(values));
        }
        result_->rows.emplace_back(std::move(row));
        return kvstore::ResultCode::SUCCEEDED;
    }

private:
    nebula::List calculateStat(const std::vector<PropStat>& stats) {
        nebula::List result;
        result.values.reserve(edgeContext_->statCount_);
        for (const auto& stat : stats) {
            if (stat.statType_ == cpp2::StatType::SUM) {
                result.values.emplace_back(stat.sum_);
            } else if (stat.statType_ == cpp2::StatType::COUNT) {
                result.values.emplace_back(stat.count_);
            } else if (stat.statType_ == cpp2::StatType::AVG) {
                result.values.emplace_back(stat.sum_ / stat.count_);
            } else if (stat.statType_ == cpp2::StatType::MAX) {
                result.values.emplace_back(stat.max_);
            } else if (stat.statType_ == cpp2::StatType::MIN) {
                result.values.emplace_back(stat.min_);
            }
        }
        return result;
    }

    EdgeContext* edgeContext_;
    GetNeighborsNode* rowNode_;
};


}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_AGGREGATENODE_H_
