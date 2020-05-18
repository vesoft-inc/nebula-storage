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
    GetNeighborsNode(std::vector<TagNode*> tagNodes,
                     FilterNode* filterNode,
                     TagContext* tagContext,
                     EdgeContext* edgeContext,
                     size_t vIdLen,
                     nebula::DataSet* result)
        : QueryNode(vIdLen)
        , tagNodes_(std::move(tagNodes))
        , filterNode_(filterNode)
        , tagContext_(tagContext)
        , edgeContext_(edgeContext)
        , result_(result) {}

    kvstore::ResultCode execute(PartitionID partId, const VertexID& vId) override {
        auto ret = RelNode::execute(partId, vId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        nebula::Row row;
        // vertexId is the first column
        row.columns.emplace_back(vId);

        // reserve second column for stat
        row.columns.emplace_back(NullType::__NULL__);
        std::vector<PropStat>* stats = nullptr;
        if (edgeContext_->statCount_ > 0) {
            *stats = initStatValue();
        }

        // add result of each tag node
        for (auto* tagNode : tagNodes_) {
            ret = tagNode->collectTagPropsIfValid(
                [&row] (const std::vector<PropContext>*) -> kvstore::ResultCode {
                    row.columns.emplace_back(NullType::__NULL__);
                    return kvstore::ResultCode::SUCCEEDED;
                },
                [this, &row, filterCtx = filterNode_->filterCtx()]
                (TagID tagId, RowReader* reader, const std::vector<PropContext>* props)
                -> kvstore::ResultCode {
                    nebula::List list;
                    auto code = collectTagProps(tagId, reader, props, list, filterCtx);
                    if (code != kvstore::ResultCode::SUCCEEDED) {
                        return code;
                    }
                    row.columns.emplace_back(std::move(list));
                    return kvstore::ResultCode::SUCCEEDED;
                });
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                return ret;
            }
        }

        // add default null for each edge node
        row.columns.resize(row.columns.size() + edgeContext_->propContexts_.size(),
                           NullType::__NULL__);
        int64_t edgeRowCount = 0;
        nebula::List list;
        for (; filterNode_->valid(); filterNode_->next(), ++edgeRowCount) {
            auto edgeType = filterNode_->edgeType();
            auto key = filterNode_->key();
            auto reader = filterNode_->reader();
            auto props = filterNode_->props();
            auto columnIdx = filterNode_->idx();

            ret = collectEdgeProps(edgeType, key, reader, props, list, stats);
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                return ret;
            }

            // add edge prop value to the target column
            if (row.columns[columnIdx].type() == Value::Type::NULLVALUE) {
                row.columns[columnIdx].setList(nebula::List());
            }
            auto& cell = row.columns[columnIdx].mutableList();
            cell.values.emplace_back(std::move(list));
        }

        if (edgeContext_->statCount_ > 0) {
            // set the stat result to column[1]
            row.columns[1].setList(calculateStat(*stats));
        }

        DVLOG(1) << vId << " process " << edgeRowCount << " edges in total.";
        result_->rows.emplace_back(std::move(row));
        return kvstore::ResultCode::SUCCEEDED;
    }

private:
    explicit GetNeighborsNode(size_t vIdLen) : QueryNode(vIdLen) {}

    std::vector<PropStat> initStatValue() {
        // initialize all stat value of all edgeTypes
        std::vector<PropStat> stats;
        stats.resize(edgeContext_->statCount_);
        for (const auto& ec : edgeContext_->propContexts_) {
            for (const auto& ctx : ec.second) {
                if (ctx.hasStat_) {
                    PropStat stat(ctx.statType_);
                    stats[ctx.statIndex_] = std::move(stat);
                }
            }
        }
        return stats;
    }

    nebula::List calculateStat(const std::vector<PropStat>& stats) {
        nebula::List result;
        result.values.reserve(edgeContext_->statCount_);
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
            // todo(doodle): MIN/MAX
        }
        return result;
    }

    std::vector<TagNode*> tagNodes_;
    FilterNode* filterNode_;
    TagContext* tagContext_;
    EdgeContext* edgeContext_;
    nebula::DataSet* result_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_GETNEIGHBORSNODE_H_
