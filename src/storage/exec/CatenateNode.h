/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_CATENATENODE_H_
#define STORAGE_EXEC_CATENATENODE_H_

#include "base/Base.h"
#include "storage/exec/FilterNode.h"

namespace nebula {
namespace storage {

class CatenateNode : public RelNode {
    FRIEND_TEST(ScanEdgePropBench, ProcessEdgeProps);

public:
    CatenateNode(std::vector<TagNode*> tagNodes,
                 FilterNode* filterNode,
                 TagContext* tagContext,
                 EdgeContext* edgeContext,
                 size_t vIdLen,
                 nebula::DataSet* result)
        : tagNodes_(std::move(tagNodes))
        , filterNode_(filterNode)
        , tagContext_(tagContext)
        , edgeContext_(edgeContext)
        , vIdLen_(vIdLen)
        , result_(result) {}

    folly::Future<kvstore::ResultCode> execute(PartitionID, const VertexID& vId) override {
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
            row.columns.emplace_back(std::move(tagNode->result()));
        }

        // add default null for each edge node
        row.columns.resize(row.columns.size() + edgeContext_->propContexts_.size(),
                           NullType::__NULL__);
        int64_t edgeRowCount = 0;
        for (; filterNode_->valid(); filterNode_->next(), ++edgeRowCount) {
            auto edgeType = filterNode_->edgeType();
            auto key = filterNode_->key();
            auto reader = filterNode_->reader();
            auto props = filterNode_->props();
            auto columnIdx = filterNode_->idx();
            // add edge prop value to the target column
            auto ret = collectEdgeProps(edgeType, key, reader, props, row, columnIdx, stats);
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                return ret;
            }
        }

        if (edgeContext_->statCount_ > 0) {
            // set the stat result to column[1]
            row.columns[1].setList(std::move(calculateStat(*stats)));
        }

        DVLOG(1) << vId << " process " << edgeRowCount << " edges in total.";
        result_->rows.emplace_back(std::move(row));
        return kvstore::ResultCode::SUCCEEDED;
    }

private:
    explicit CatenateNode(size_t vIdLen): vIdLen_(vIdLen) {}

    kvstore::ResultCode collectEdgeProps(EdgeType edgeType,
                                         folly::StringPiece key,
                                         RowReader* reader,
                                         const std::vector<PropContext>* props,
                                         nebula::Row& row,
                                         size_t idx,
                                         std::vector<PropStat>* stats = nullptr) {
        nebula::Row propRow;
        for (size_t i = 0; i < props->size(); i++) {
            const auto& prop = (*props)[i];
            VLOG(2) << "Collect prop " << prop.name_ << ", type " << edgeType;
            nebula::Value value;
            switch (prop.propInKeyType_) {
                // prop in value
                case PropContext::PropInKeyType::NONE: {
                    if (reader != nullptr) {
                        auto status = readValue(reader, prop);
                        if (!status.ok()) {
                            return kvstore::ResultCode::ERR_EDGE_PROP_NOT_FOUND;
                        }
                        value = std::move(status).value();
                    }
                    break;
                }
                case PropContext::PropInKeyType::SRC: {
                    value = NebulaKeyUtils::getSrcId(vIdLen_, key);
                    break;
                }
                case PropContext::PropInKeyType::DST: {
                    value = NebulaKeyUtils::getDstId(vIdLen_, key);
                    break;
                }
                case PropContext::PropInKeyType::TYPE: {
                    value = NebulaKeyUtils::getEdgeType(vIdLen_, key);
                    break;
                }
                case PropContext::PropInKeyType::RANK: {
                    value = NebulaKeyUtils::getRank(vIdLen_, key);
                    break;
                }
            }
            if (prop.hasStat_ && stats != nullptr) {
                addStatValue(value, (*stats)[prop.statIndex_]);
            }
            if (prop.returned_) {
                propRow.columns.emplace_back(std::move(value));
            }
        }
        if (row.columns[idx].type() == Value::Type::NULLVALUE) {
            row.columns[idx].setDataSet(nebula::DataSet());
        }
        auto& cell = row.columns[idx].mutableDataSet();
        cell.rows.emplace_back(std::move(propRow));
        return kvstore::ResultCode::SUCCEEDED;
    }

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

    void addStatValue(const Value& value, PropStat& stat) {
        if (value.type() == Value::Type::INT || value.type() == Value::Type::FLOAT) {
            stat.sum_ = stat.sum_ + value;
            ++stat.count_;
        }
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
    size_t vIdLen_;
    nebula::DataSet* result_;
};


class CatenateUpdateNode : public RelNode {
public:
    CatenateNode(StorageEnv* env,
                 GraphSpaceID spaceId,
                 UpdateNode* updateNode,
                 std::vector<std::unique_ptr<Expression>>& returnPropsExp,
                 nebula::DataSet* result)
        : env_(env)
        , spaceId_(spaceId)
        , updateNode_(updateNode)
        , returnPropsExp_(returnPropsExp);
        , result_(result) {
            filter_ = updateNode_->getFilterCont();
            insert_ = updateNode_-getInsert();
        }

    folly::Future<kvstore::ResultCode> execute(PartitionID, const VertexID& vId) override {
        Getters getters;
        getters.getSrcTagProp = [this] (const std::string& tagName,
                                        const std::string& prop) -> OptValue {
            auto tagRet = this->env_->schemaMan_->toTagID(this->spaceId_, tagName);
            if (!tagRet.ok()) {
                VLOG(1) << "Can't find tag " << tagName << ", in space " << this->spaceId_;
                return Status::Error("Invalid Filter Tag: " + tagName);
            }
            auto tagId = tagRet.value();
            auto tagFilters = this->filter_->getTagFilter();
            auto it = tagFilters.find(std::make_pair(tagId, prop));
            if (it == tagFilters.end()) {
                return Status::Error("Invalid Tag Filter");
            }
            VLOG(1) << "Hit srcProp filter for tag: " << tagName
                    << ", prop: " << prop;
            return it->second;
        };
    
        result_->colNames.emplace_back("_inserted");
        nebula::Row row;
        row.columns.emplace_back(insert_);
    
        for (auto& exp : returnPropsExp_) {
            auto value = exp->eval(getters);
            if (!value.ok()) {
                LOG(ERROR) << value.status();
                return kvstore::ResultCode::ERR_INVALID_RETURN_EXP;
            }
            result_->colNames.emplace_back(folly::stringPrintf("%s:%s", exp->alias()->c_str(),
                                                                        exp->prop()->c_str()));
            row.columns.emplace_back(std::move(value.value());
        }
        result_->rows.emplace_back(std::move(row));
        return kvstore::ResultCode::SUCCEEDED;
    }

private:
    // ================= input =========================================================
    StorageEnv                                                                     *env_;
    UpdateNode                                                                     *updateNode_;
    GraphSpaceID                                                                    spaceId_;
    FilterContext                                                                  *filter_;
    bool                                                                            insert_{false};
    std::vector<std::unique_ptr<Expression>>                                        returnPropsExp_;
    // ===================output========================================================
    nebula::DataSet                                                                *result_;
};


}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_CATENATENODE_H_
