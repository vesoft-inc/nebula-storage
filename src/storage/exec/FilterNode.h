/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_FILTERNODE_H_
#define STORAGE_EXEC_FILTERNODE_H_

#include "common/base/Base.h"
#include "common/expression/Expression.h"
#include "storage/exec/TagNode.h"
#include "storage/exec/EdgeNode.h"
#include "storage/context/UpdateExpressionContext.h"

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
               Expression* exp)
        : tagNodes_(tagNodes)
        , edgeNodes_(edgeNodes)
        , tagContext_(tagContext)
        , edgeContext_(edgeContext)
        , filterExp_(exp) {
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
                [&result] (TagID,
                           const std::vector<PropContext>*) -> kvstore::ResultCode {
                    result.values.emplace_back(NullType::__NULL__);
                    return kvstore::ResultCode::SUCCEEDED;
               },
                [this, &result] (TagID tagId,
                                 RowReader* reader,
                                 const std::vector<PropContext>* props,
                                 const folly::StringPiece& key,
                                 const folly::StringPiece& row) -> kvstore::ResultCode {
                    UNUSED(key);
                    UNUSED(row);
                    nebula::List list;
                    auto code = collectTagProps(tagId, reader, props, list, filter_);
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

        if (filterExp_ != nullptr) {
            // todo(doodle)
            // filterExp_->eval();
        }
        return true;
    }

protected:
    std::vector<TagNode*> tagNodes_;
    std::vector<EdgeNode<VertexID>*> edgeNodes_;
    TagContext* tagContext_;
    EdgeContext* edgeContext_;
    Expression* filterExp_;
    FilterContext* filter_;
    EdgeType edgeType_ = 0;
    size_t columnIdx_;
    const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>* schemas_ = nullptr;
    const std::vector<PropContext>* props_ = nullptr;
    folly::Optional<std::pair<std::string, int64_t>> ttl_;

    std::unique_ptr<RowReader> reader_;
    std::unique_ptr<EdgeIterator> iter_;
};

// UpdateFilterNode only use for update vertex/edge
// Collect the result of all tags/edges, and judge the filter expression
class UpdateFilterNode final : public FilterNode {
public:
    UpdateFilterNode(const std::vector<TagNode*>& tagUpdates,
                     const std::vector<EdgeNode<VertexID>*>& edgeNodes,
                     TagContext* tagContext,
                     EdgeContext* edgeContext,
                     Expression* filterExp,
                     StorageEnv* env,
                     GraphSpaceID spaceId,
                     UpdateExpressionContext* expCtx,
                     std::vector<storage::cpp2::UpdatedVertexProp>& updatedVertexProps,
                     bool insertable,
                     std::unordered_set<TagID>& updateTagIds,
                     size_t vIdLen)
        : FilterNode(tagUpdates, edgeNodes, tagContext, edgeContext, filterExp)
        , env_(env)
        , spaceId_(spaceId)
        , expCtx_(expCtx)
        , updatedVertexProps_(updatedVertexProps)
        , insertable_(insertable)
        , updateTagIds_(updateTagIds)
        , vIdLen_(vIdLen) {
            filter_ = std::make_unique<FilterContext>();
        }

    // Only update tag
    kvstore::ResultCode execute(PartitionID partId, const VertexID& vId) override {
        auto ret = RelNode::execute(partId, vId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        CHECK_EQ(1, tagNodes_.size());

        // This tagId needs insert props row
        // Failed when the props neither updated value nor has default value, nullable
        auto insertTagProps = [&partId, &vId, this] (TagID tagId,
                              const std::vector<PropContext>* props) -> kvstore::ResultCode {
            if (!this->insertable_ ||
                this->updateTagIds_.find(tagId) == this->updateTagIds_.end()) {
                return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
            }

            this->insert_ = true;
            auto schemaIter = this->tagContext_->schemas_.find(tagId);
            CHECK(schemaIter != this->tagContext_->schemas_.end());
            CHECK(!schemaIter->second.empty());
            auto schema = schemaIter->second.back();
            // When insert, the filter condition is always true
            // the tagId props of props_ need default value, nullable, update value
#if 0
            for (auto& prop : *props) {
                // first check whether is updated field, then use default value
                if (prop.field_->hasDefault()) {
                    // all fields new value puts filter_
                    this->filter_->fillTagProp(tagId, prop.name_, prop.field_->defaultValue());
                //} else if (prop.field_->nullable()) {
                //    this->filter_->fillTagProp(tagId, prop.name_, NullType::__NULL__);

                } else {
                    return kvstore::ResultCode::ERR_INVALID_FIELD_VALUE;
                /*
                } else {
                    bool isUpdateProp = false;
                    for (auto& updateProp : this->updatedVertexProps_) {
                        auto toTagId = updateProp.get_tag_id();
                        auto propName = updateProp.get_name();
                        if (tagId == toTagId && !prop.name_.compare(propName)) {
                            isUpdateProp = true;
                            // insert when upsert, filter is always true

                            break;
                        }
                    }

                    // no default value, no nullable, no update prop
                    if (!isUpdateProp) {
                        return kvstore::ResultCode::ERR_INVALID_FIELD_VALUE;
                    }
                }
                */
                }
            }
#endif

            UNUSED(props);
            for (size_t i = 0; i < schema->getNumFields(); i++) {
                auto prop = schema->getFieldName(i);
                // read prop value
                auto field = schema->field(i);
                if (field->hasDefault()) {
                    auto defalutVal = field->defaultValue();
                    this->filter_->fillTagProp(tagId, prop, std::move(defalutVal));
                } else {
                    return kvstore::ResultCode::ERR_TAG_PROP_NOT_FOUND;
                }
            }

            // build key, value is emtpy
            auto version =
                std::numeric_limits<int64_t>::max() - time::WallClock::fastNowInMicroSec();
            // Switch version to big-endian, make sure the key is in ordered.
            version = folly::Endian::big(version);
            this->key_ = NebulaKeyUtils::vertexKey(this->vIdLen_, partId, vId, tagId, version);
            this->rowWriter_ = std::make_unique<RowWriterV2>(schema.get());
            this->tagId_ = tagId;

            return kvstore::ResultCode::SUCCEEDED;
        };

        // collect tag prop
        auto collTagProp = [this] (TagID tagId,
                                   RowReader* reader,
                                   const std::vector<PropContext>* props,
                                   const folly::StringPiece& key,
                                   const folly::StringPiece& row)
                                   -> kvstore::ResultCode {
            auto schemaIter = this->tagContext_->schemas_.find(tagId);
            CHECK(schemaIter != this->tagContext_->schemas_.end());
            CHECK(!schemaIter->second.empty());
            auto schema = schemaIter->second.back();
            /*
            for (auto& prop : *props) {
                VLOG(1) << "Collect prop " << prop.name_ << ", type " << tagId;

                // read prop value
                auto retVal = QueryUtils::readValue(reader, prop);
                if (!retVal.ok()) {
                    VLOG(1) << "Bad value for tag: " << tagId
                            << ", prop " << prop.name_;
                    return kvstore::ResultCode::ERR_TAG_PROP_NOT_FOUND;
                }
                auto cloValue = std::move(retVal.value());

                // This is different from others node
                // filter, update, return props fields of this tag, value puts filter_
                this->filter_->fillTagProp(tagId, prop.name_, cloValue);
            }
            */

            UNUSED(props);
            UNUSED(row);
            for (size_t i = 0; i < schema->getNumFields(); i++) {
                auto prop = schema->getFieldName(i);
                // read prop value
                auto retVal = reader->getValueByIndex(i);;
                if (retVal == NullType::BAD_TYPE) {
                    VLOG(1) << "Bad value for tag: " << tagId
                            << ", prop " << prop;
                    return kvstore::ResultCode::ERR_TAG_PROP_NOT_FOUND;
                }
                this->filter_->fillTagProp(tagId, prop, std::move(retVal));
            }

            // update info
            if (this->updateTagIds_.find(tagId) != this->updateTagIds_.end()) {
                // this->rowWriter_ = std::make_unique<RowWriterV2>(schema.get(), row.str());
                this->rowWriter_ = std::make_unique<RowWriterV2>(schema.get());
                this->tagId_ = tagId;
                this->key_ = key.str();
            }
            return kvstore::ResultCode::SUCCEEDED;
        };


        ret = tagNodes_[0]->collectTagPropsIfValid(insertTagProps, collTagProp);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        // build expression context
        for (auto &e : filter_->getTagFilter()) {
            auto tagName = env_->schemaMan_->toTagName(spaceId_, e.first.first);
            if (!tagName.ok()) {
                VLOG(1) << "Can't find spaceId " << spaceId_ << " tagId " << e.first.first;
                return kvstore::ResultCode::ERR_TAG_NOT_FOUND;
            }
            expCtx_->setSrcProp(tagName.value(), e.first.second, e.second);
        }

        if (!insert_) {
            return checkFilter();
        }
        return kvstore::ResultCode::SUCCEEDED;
    }


    kvstore::ResultCode checkFilter() {
        if (filterExp_ != nullptr) {
            // use SourcePropertyExpression eval
            auto filterVal = filterExp_->eval(*expCtx_);
            // filterExp is false, will filter out
            // NULL is always false
            auto filterRet = filterVal.toBool();
            if (!filterRet.ok() || !(filterRet.value())) {
                VLOG(1) << "Filter skips the update";
                return kvstore::ResultCode::ERR_RESULT_FILTERED;
            }
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

    FilterContext* getFilterCont() {
        return filter_.get();
    }

    TagID getTagId() {
        return tagId_;
    }

    std::string getKey() {
        return key_;
    }

    RowWriterV2* getRowWriter() {
        return rowWriter_.get();
    }

    bool getInsert() {
        return insert_;
    }

    UpdateExpressionContext* getExpressionContext() {
        return expCtx_;
    }

private:
    // ================================= input ==============================================
    StorageEnv                                                            *env_;
    GraphSpaceID                                                           spaceId_;
    UpdateExpressionContext                                               *expCtx_;

    std::vector<storage::cpp2::UpdatedVertexProp>                          updatedVertexProps_;
    // Whether to allow insert
    bool                                                                   insertable_{false};

    // BuildTagContext set this, only update tagId
    std::unordered_set<TagID>                                              updateTagIds_;

    size_t                                                                 vIdLen_;

    // ================================== output  =============================================
    // filter stores the newest value of tag props so far:  update prop, return prop, filter prop
    std::unique_ptr<FilterContext>                                         filter_;

    // Whether an insert has occurred
    // Because update one vid, so every tagId of one vertex has one row data at most
    // upsert process one row data, either update or insert
    bool                                                                   insert_{false};

    // std::unordered_map<std::string, std::unique_ptr<RowWriterV2>>          tagUpdateKV_;
    std::string                                                            key_;
    std::unique_ptr<RowWriterV2>                                           rowWriter_;

    TagID                                                                  tagId_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_FILTERNODE_H_
