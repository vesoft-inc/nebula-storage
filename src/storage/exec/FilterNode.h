/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_FILTERNODE_H_
#define STORAGE_EXEC_FILTERNODE_H_

#include "common/base/Base.h"
#include "common/expression/Expression.h"
#include "common/context/ExpressionContext.h"
#include "storage/exec/TagNode.h"
#include "storage/exec/EdgeNode.h"

namespace nebula {
namespace storage {

// FilterNode has input of serveral TagNode and EdgeNode, the EdgeNode could be either several
// EdgeTypePrefixScanNode of different edge types, or a single VertexPrefixScanNode which scan
// all edges of a vertex.
// The output would be the result of tag, it is a List, each cell save a list of property values,
// if tag not found, it will be a NullType::__NULL__. Also it will return a iterator of edges
// which can pass through filter
class FilterNode : public QueryNode<VertexID>, public EdgeIterator {
public:
    FilterNode(const std::vector<TagNode*>& tagNodes,
               const std::vector<EdgeNode<VertexID>*>& edgeNodes,
               TagContext* tagContext,
               EdgeContext* edgeContext,
               const Expression* exp)
        : tagNodes_(tagNodes)
        , edgeNodes_(edgeNodes)
        , tagContext_(tagContext)
        , edgeContext_(edgeContext)
        , exp_(exp) {
        UNUSED(tagContext_);
    }

    kvstore::ResultCode execute(PartitionID partId, const VertexID& vId) override {
        auto ret = RelNode::execute(partId, vId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        result_.setList(nebula::List());
        auto& result = result_.mutableList();

        // add result of each tag node to tagResult
        for (auto* tagNode : tagNodes_) {
            ret = tagNode->collectTagPropsIfValid(
                [&result] (const std::vector<PropContext>*) -> kvstore::ResultCode {
                    result.values.emplace_back(NullType::__NULL__);
                    return kvstore::ResultCode::SUCCEEDED;
                },
                [this, &result] (TagID tagId,
                                 RowReader* reader,
                                 const std::vector<PropContext>* props) -> kvstore::ResultCode {
                    nebula::List list;
                    auto code = collectTagProps(tagId, reader, props, list, &filter_);
                    if (code != kvstore::ResultCode::SUCCEEDED) {
                        return code;
                    }
                    result.values.emplace_back(std::move(list));
                    return kvstore::ResultCode::SUCCEEDED;
                });
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                return ret;
            }
        }

        std::vector<EdgeIterator*> iters;
        for (auto* edgeNode : edgeNodes_) {
            iters.emplace_back(edgeNode->iter());
        }
        iter_.reset(new MultiEdgeIterator(std::move(iters)));
        while (iter_->valid() && !check()) {
            iter_->next();
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

    bool valid() const override {
        // todo(doodle): could add max rows limit here
        return iter_->valid();
    }

    void next() override {
        do {
            iter_->next();
        } while (iter_->valid() && !check());
    }

    folly::StringPiece key() const override {
        return iter_->key();
    }

    folly::StringPiece val() const override {
        return iter_->val();
    }

    VertexID srcId() const override {
        return iter_->srcId();
    }

    EdgeType edgeType() const override {
        return edgeType_;
    }

    EdgeRanking edgeRank() const override {
        return iter_->edgeRank();
    }

    VertexID dstId() const override {
        return iter_->dstId();
    }

    // return the column index in result row

    size_t idx() {
        return columnIdx_;
    }

    // return the edge row reader which could pass filter
    RowReader* reader() {
        return reader_.get();
    }

    // return the edge props need to return
    const std::vector<PropContext>* props() {
        return props_;
    }

private:
    // return true when the value iter points to a value which can pass ttl and filter
    bool check() {
        EdgeType type = iter_->edgeType();
        // update info when edgeType changes while iterating over different edgeTypes
        if (type != edgeType_) {
            auto idxIter = edgeContext_->indexMap_.find(type);
            CHECK(idxIter != edgeContext_->indexMap_.end());
            auto schemaIter = edgeContext_->schemas_.find(std::abs(type));
            CHECK(schemaIter != edgeContext_->schemas_.end());
            CHECK(!schemaIter->second.empty());

            auto idx = idxIter->second;
            edgeType_ = type;
            props_ = &(edgeContext_->propContexts_[idx].second);
            columnIdx_ = edgeContext_->offset_ + idx;
            schemas_ = &(schemaIter->second);
            ttl_ = getEdgeTTLInfo(edgeContext_, edgeType_);
        }

        // if we can't read this value, just pass it, which is different from 1.0
        auto val = iter_->val();
        if (!reader_) {
            reader_ = RowReader::getRowReader(*schemas_, val);
            if (!reader_) {
                return false;
            }
        } else if (!reader_->reset(*schemas_, val)) {
            return false;
        }

        const auto& latestSchema = schemas_->back();
        if (ttl_.has_value() &&
            CommonUtils::checkDataExpiredForTTL(latestSchema.get(), reader_.get(),
                                                ttl_.value().first, ttl_.value().second)) {
            return false;
        }

        if (exp_ != nullptr) {
            // todo(doodle)
            // exp_->eval();
        }
        return true;
    }

    folly::Optional<std::pair<std::string, int64_t>> getEdgeTTLInfo() {
        folly::Optional<std::pair<std::string, int64_t>> ret;
        auto edgeFound = edgeContext_->ttlInfo_.find(std::abs(edgeType_));
        if (edgeFound != edgeContext_->ttlInfo_.end()) {
            ret.emplace(edgeFound->second.first, edgeFound->second.second);
        }
        return ret;
    }

private:
    const Expression* exp_;
    std::vector<TagNode*> tagNodes_;
    std::vector<EdgeNode*> edgeNodes_;
    TagContext* tagContext_;
    EdgeContext* edgeContext_;
    FilterContext* filter_;

    EdgeType edgeType_ = 0;
    size_t columnIdx_;
    const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>* schemas_ = nullptr;
    const std::vector<PropContext>* props_ = nullptr;
    folly::Optional<std::pair<std::string, int64_t>> ttl_;

    std::unique_ptr<RowReader> reader_;
    std::unique_ptr<StorageIterator> iter_;
};

class TagFilterNode : public RelNode {
public:
    TagFilterNode(StorageEnv* env,
                  GraphSpaceID spaceId,
                  Expression* filterExp,
                  std::vector<TagUpdateNode*>& tagUpdates,
                  TagContext* tagContext)
        : env_(env)
        , spaceId_(spaceId)
        , filterExp_(filterExp)
        , tagUpdates_(tagUpdates)
        , tagContext_(tagContext) {
            filter_ = std::make_unique<FilterContext>();
        }

    folly::Future<kvstore::ResultCode> execute(PartitionID partId, const VertexID& vId) override {
        UNUSED(partId);
        UNUSED(vId);
        std::vector<StorageIterator*> iters;
        // collect tags results

        for (auto* tagUpdate : tagUpdates_) {
            if (tagUpdate->getInsert()) {
                insert_ = true;
            }
            auto updateKV = tagUpdate->getUpdateKV();
            tagUpdateKV_.emplace(tagUpdate->getTagID(), updateKV);


            auto tagFilter = tagUpdate->getFilter();
            for (auto &e : tagFilter->getTagFilter()) {
                filter_->fillTagProp(e.first.first, e.first.second, e.second);
            }
        }

        return kvstore::ResultCode::SUCCEEDED;
    }

    kvstore::ResultCode checkFilter() {
        Getters getters;
        getters.getSrcTagProp = [&, this] (const std::string& tagName,
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

        if (filterExp_ != nullptr) {
            auto filterResult = filterExp_->eval(getters);
            if (!filterResult.ok()) {
                return  kvstore::ResultCode::ERR_INVALID_FILTERED;
            }
            if (!Expression::asBool(filterResult.value())) {
                VLOG(1) << "Filter skips the update";
                return kvstore::ResultCode::ERR_RESULT_FILTERED;
            }
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

    FilterContext* getFilterCont() {
        return filter_.get();
    }

    std::unordered_map<TagID, std::pair<std::string, std::unique_ptr<RowWriterV2>>> getUpdateKV() {
        return tagUpdateKV_;
    }

    bool getInsert() {
        return insert_;
    }

private:
    // ============== input ====================================================================
    StorageEnv                                                                     *env_;
    GraphSpaceID                                                                    spaceId_;
    // filter expression
    Expression                                                                     *filterExp_;
    // Dependent node
    std::vector<TagUpdateNode*>                                                     tagUpdates_;
    TagContext                                                                     *tagContext_;

    // ==================output ==============================================================
    std::unique_ptr<FilterContext>                                                  filter_;
    bool                                                                            insert_{false};
    std::unordered_map<TagID, std::pair<std::string, std::unique_ptr<RowWriterV2>>> tagUpdateKV_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_FILTERNODE_H_
