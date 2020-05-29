/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_GETNEIGHBORSNODE_H_
#define STORAGE_EXEC_GETNEIGHBORSNODE_H_

#include "common/base/Base.h"
#include "storage/exec/FilterNode.h"

namespace nebula {
namespace storage {

class GetNeighborsNode : public QueryNode<VertexID> {
    FRIEND_TEST(ScanEdgePropBench, ProcessEdgeProps);

public:
    GetNeighborsNode(FilterNode* filterNode,
                     EdgeContext* edgeContext)
        : filterNode_(filterNode)
        , edgeContext_(edgeContext) {}

    kvstore::ResultCode execute(PartitionID partId, const VertexID& vId) override {
        auto ret = RelNode::execute(partId, vId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        result_.columns.clear();
        // vertexId is the first column
        result_.columns.emplace_back(vId);
        // reserve second column for stat
        result_.columns.emplace_back(NullType::__NULL__);
        stats_ = initStatValue();

        auto tagResult = filterNode_->tagResult().moveList();
        for (auto& value : tagResult.values) {
            result_.columns.emplace_back(std::move(value));
        }

        // add default null for each edge node
        result_.columns.resize(result_.columns.size() + edgeContext_->propContexts_.size(),
                               NullType::__NULL__);
        int64_t edgeRowCount = 0;
        nebula::List list;
        for (; filterNode_->valid(); filterNode_->next(), ++edgeRowCount) {
            auto srcId = filterNode_->srcId();
            auto edgeType = filterNode_->edgeType();
            auto edgeRank = filterNode_->edgeRank();
            auto dstId = filterNode_->dstId();
            auto reader = filterNode_->reader();
            auto props = filterNode_->props();
            auto columnIdx = filterNode_->idx();

            ret = collectEdgeProps(srcId, edgeType, edgeRank, dstId, reader, props, list, &stats_);
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                return ret;
            }

            // add edge prop value to the target column
            if (result_.columns[columnIdx].type() == Value::Type::NULLVALUE) {
                result_.columns[columnIdx].setList(nebula::List());
            }
            auto& cell = result_.columns[columnIdx].mutableList();
            cell.values.emplace_back(std::move(list));
        }


        DVLOG(1) << vId << " process " << edgeRowCount << " edges in total.";
        return kvstore::ResultCode::SUCCEEDED;
    }

    const std::vector<PropStat>& stats() {
        return stats_;
    }

private:
    GetNeighborsNode() = default;

    std::vector<PropStat> initStatValue() {
        // initialize all stat value of all edgeTypes
        std::vector<PropStat> stats;
        if (edgeContext_->statCount_ > 0) {
            stats.resize(edgeContext_->statCount_);
            for (const auto& ec : edgeContext_->propContexts_) {
                for (const auto& ctx : ec.second) {
                    if (ctx.hasStat_) {
                        PropStat stat(ctx.statType_);
                        stats[ctx.statIndex_] = std::move(stat);
                    }
                }
            }
        }
        return stats;
    }

    FilterNode* filterNode_;
    EdgeContext* edgeContext_;
    std::vector<PropStat> stats_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_GETNEIGHBORSNODE_H_
