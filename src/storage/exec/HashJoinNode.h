/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_HASHJOINNODE_H_
#define STORAGE_EXEC_HASHJOINNODE_H_

#include "common/base/Base.h"
#include "storage/context/StorageExpressionContext.h"
#include "storage/exec/TagNode.h"
#include "storage/exec/EdgeNode.h"
#include "storage/exec/StorageIterator.h"

namespace nebula {
namespace storage {

// HashJoinNode has input of serveral TagNode and EdgeNode, the EdgeNode is several
// SingleEdgeNode of different edge types all edges of a vertex.
// The output would be the result of tag, it is a List, each cell save a list of property values,
// if tag not found, it will be a NullType::__NULL__.
// Also it will return a iterator of edges which can pass ttl check and ready to be read.
class HashJoinNode : public IterateNode<VertexID> {
public:
    HashJoinNode(PlanContext* planCtx,
                 const std::vector<TagNode*>& tagNodes,
                 const std::vector<EdgeNode<VertexID>*>& edgeNodes,
                 TagContext* tagContext,
                 EdgeContext* edgeContext,
                 StorageExpressionContext* expCtx,
                 std::unordered_set<std::string> reservedProps = {})
        : planContext_(planCtx)
        , tagNodes_(tagNodes)
        , edgeNodes_(edgeNodes)
        , tagContext_(tagContext)
        , edgeContext_(edgeContext)
        , expCtx_(expCtx),
          reservedProps_(std::move(reservedProps)) {
        UNUSED(tagContext_);
        for (std::size_t i = 0; i < tagNodes.size(); ++i) {
            if (tagNodes[i]->props()->empty()) {
                continue;
            }
            tagColIndexes_.emplace(tagNodes[i]->getTagName(), i);
        }
    }

    kvstore::ResultCode execute(PartitionID partId, const VertexID& vId) override {
        auto ret = RelNode::execute(partId, vId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        if (expCtx_ != nullptr) {
            expCtx_->clear();
        }
        result_.setList(nebula::List());
        auto& result = result_.mutableList();
        if (planContext_->resultStat_ == ResultStatus::ILLEGAL_DATA) {
            return kvstore::ResultCode::ERR_INVALID_DATA;
        }

        result.values.resize(tagColIndexes_.size(), NullType::__NULL__);
        // add result of each tag node to tagResult
        for (auto* tagNode : tagNodes_) {
            const auto& tagName = tagNode->getTagName();
            ret = tagNode->collectTagPropsIfValid(
                [this, &result, &tagName] (const std::vector<PropContext>*) -> kvstore::ResultCode {
                    const auto foundTag = tagColIndexes_.find(tagName);
                    if (foundTag == tagColIndexes_.end()) {
                        return kvstore::ResultCode::SUCCEEDED;
                    }
                    result.values[foundTag->second] = NullType::__NULL__;
                    return kvstore::ResultCode::SUCCEEDED;
                },
                [this, &result, &tagName] (TagID tagId,
                                           RowReader* reader,
                                           const std::vector<PropContext>* props)
                -> kvstore::ResultCode {
                    if (reservedProps_.find("_tags") != reservedProps_.end()) {
                        auto index = tagColIndexes_["__dummy_tag"];
                        result.values[index].setList(List());
                        auto &dummyTagList = result.values[index].mutableList();
                        dummyTagList.values.resize(1);
                        auto &tagsList = dummyTagList.values[0];
                        if (tagsList.empty()) {
                            tagsList.setList(List());
                        }
                        tagsList.mutableList().emplace_back(tagName);
                    }

                    nebula::List list;
                    auto code = collectTagProps(tagId,
                                                tagName,
                                                reader,
                                                props,
                                                list,
                                                expCtx_);
                    if (code != kvstore::ResultCode::SUCCEEDED) {
                        return code;
                    }
                    result.values[tagColIndexes_[tagName]] = std::move(list);
                    return kvstore::ResultCode::SUCCEEDED;
                });
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                return ret;
            }
        }

        std::vector<SingleEdgeIterator*> iters;
        for (auto* edgeNode : edgeNodes_) {
            iters.emplace_back(edgeNode->iter());
        }
        iter_.reset(new MultiEdgeIterator(std::move(iters)));
        if (iter_->valid()) {
            setCurrentEdgeInfo();
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

    bool valid() const override {
        return iter_->valid();
    }

    void next() override {
        iter_->next();
        if (iter_->valid()) {
            setCurrentEdgeInfo();
        }
    }

    folly::StringPiece key() const override {
        return iter_->key();
    }

    folly::StringPiece val() const override {
        return iter_->val();
    }

    // return the edge row reader which could pass filter
    RowReader* reader() const override {
        return iter_->reader();
    }

private:
    // return true when the value iter points to a value which can pass ttl and filter
    void setCurrentEdgeInfo() {
        EdgeType type = iter_->edgeType();
        // update info when edgeType changes while iterating over different edgeTypes
        if (type != planContext_->edgeType_) {
            auto idxIter = edgeContext_->indexMap_.find(type);
            CHECK(idxIter != edgeContext_->indexMap_.end());
            auto schemaIter = edgeContext_->schemas_.find(std::abs(type));
            CHECK(schemaIter != edgeContext_->schemas_.end());
            CHECK(!schemaIter->second.empty());

            planContext_->edgeSchema_ = schemaIter->second.back().get();
            // idx is the index in all edges need to return
            auto idx = idxIter->second;
            planContext_->edgeType_ = type;
            planContext_->edgeName_ = edgeNodes_[iter_->getIdx()]->getEdgeName();
            // the columnIdx_ would be the column index in a response row, so need to add
            // the offset of tags and other fields
            planContext_->columnIdx_ = edgeContext_->offset_ + idx;
            planContext_->props_ = &(edgeContext_->propContexts_[idx].second);

            expCtx_->resetSchema(planContext_->edgeName_,  planContext_->edgeSchema_, true);
        }
    }

private:
    PlanContext* planContext_;
    std::vector<TagNode*> tagNodes_;
    std::vector<EdgeNode<VertexID>*> edgeNodes_;
    TagContext* tagContext_;
    EdgeContext* edgeContext_;
    StorageExpressionContext* expCtx_;

    std::unordered_map<std::string, std::size_t> tagColIndexes_;
    std::unordered_set<std::string> reservedProps_;

    std::unique_ptr<MultiEdgeIterator> iter_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_HASHJOINNODE_H_
