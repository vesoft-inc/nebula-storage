/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_TAGNODE_H_
#define STORAGE_EXEC_TAGNODE_H_

#include "common/base/Base.h"
#include "storage/exec/RelNode.h"
#include "storage/exec/StorageIterator.h"
#include "storage/exec/FilterContext.h"

namespace nebula {
namespace storage {

// TagNode will return a DataSet of specified props of tagId
class TagNode final : public IterateNode<VertexID> {
public:
    TagNode(PlanContext* planCtx,
            TagContext* ctx,
            TagID tagId,
            const std::vector<PropContext>* props,
            ExpressionContext* expCtx = nullptr,
            Expression* exp = nullptr)
        : planContext_(planCtx)
        , tagContext_(ctx)
        , tagId_(tagId)
        , props_(props)
        , expCtx_(expCtx)
        , exp_(exp) {
        UNUSED(expCtx_); UNUSED(exp_);
        auto schemaIter = tagContext_->schemas_.find(tagId_);
        CHECK(schemaIter != tagContext_->schemas_.end());
        CHECK(!schemaIter->second.empty());
        schemas_ = &(schemaIter->second);
        ttl_ = QueryUtils::getTagTTLInfo(tagContext_, tagId_);
        tagName_ = tagContext_->tagNames_[tagId_];
    }

    kvstore::ResultCode execute(PartitionID partId, const VertexID& vId) override {
        auto ret = RelNode::execute(partId, vId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }
        VLOG(1) << "partId " << partId << ", vId " << vId << ", tagId " << tagId_
                << ", prop size " << props_->size();

        if (FLAGS_enable_vertex_cache && tagContext_->vertexCache_ != nullptr) {
            auto result = tagContext_->vertexCache_->get(std::make_pair(vId, tagId_), partId);
            if (result.ok()) {
                cacheResult_ = std::move(result).value();
                iter_.reset(new SingleTagIterator(cacheResult_, schemas_, &ttl_));
                return kvstore::ResultCode::SUCCEEDED;
            }
        }

        std::unique_ptr<kvstore::KVIterator> iter;
        prefix_ = NebulaKeyUtils::vertexPrefix(planContext_->vIdLen_, partId, vId, tagId_);
        ret = planContext_->env_->kvstore_->prefix(planContext_->spaceId_, partId, prefix_, &iter);
        if (ret == kvstore::ResultCode::SUCCEEDED && iter && iter->valid()) {
            iter_.reset(new SingleTagIterator(std::move(iter), tagId_, planContext_->vIdLen_,
                                              schemas_, &ttl_));
        } else {
            iter_.reset();
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

    kvstore::ResultCode collectTagPropsIfValid(NullHandler nullHandler,
                                               TagPropHandler valueHandler) {
        if (!iter_ || !iter_->valid()) {
            return nullHandler(props_);
        }
        return valueHandler(tagId_, iter_->reader(), props_);
    }

    bool valid() const override {
        return iter_->valid();
    }

    void next() override {
        iter_->next();
    }

    folly::StringPiece key() const override {
        return iter_->key();
    }

    folly::StringPiece val() const override {
        return iter_->val();
    }

    RowReader* reader() const override {
        return iter_->reader();
    }

    const std::string& getTagName() {
        return tagName_;
    }

protected:
    PlanContext* planContext_;
    TagContext* tagContext_;
    TagID tagId_;
    const std::vector<PropContext>* props_;
    ExpressionContext* expCtx_;
    Expression* exp_;
    const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>* schemas_ = nullptr;
    folly::Optional<std::pair<std::string, int64_t>> ttl_;
    std::string tagName_;

    std::unique_ptr<StorageIterator> iter_;
    std::string prefix_;
    std::string cacheResult_;
};


// TagUpdateNode use for update vertex
// TagUpdateNode process a tagId of one vertexID, so update one row or insert one row
class TagUpdateNode final : public TagNode {
public:
    TagUpdateNode(TagContext* ctx,
                  StorageEnv* env,
                  GraphSpaceID spaceId,
                  size_t vIdLen,
                  TagID tagId,
                  const std::vector<PropContext>* props,
                  bool insertable,
                  std::unordered_set<TagID>& updateTagIds,
                  std::vector<storage::cpp2::UpdatedVertexProp>& updatedVertexProps)
        : TagNode(ctx, env, spaceId, vIdLen, tagId, props)
        , insertable_(insertable)
        , updateTagIds_(updateTagIds)
        , updatedVertexProps_(updatedVertexProps) {
            // use newest scheam version
            schema_ = schemas_->back().get();
            CHECK_NOTNULL(schema_);
            filter_ = std::make_unique<FilterContext>();
        }

    // Only process this tag, collect all need attributes of this tag
    folly::Future<kvstore::ResultCode> execute(PartitionID partId, const VertexID& vId) override {
        VLOG(1) << "partId " << partId << ", vId " << vId << ", tagId " << tagId_
                << ", prop size " << props_->size();
        auto ret = processTagProps(partId, vId);
        if (ret == kvstore::ResultCode::ERR_KEY_NOT_FOUND) {
            result_ = NullType::__NULL__;
            ret = kvstore::ResultCode::SUCCEEDED;
        }
        return ret;
    }

    // update newest version
    kvstore::ResultCode processTagProps(PartitionID partId, const VertexID& vId) {
        // use key and value, so do not use vertexCache_
        auto prefix = NebulaKeyUtils::vertexPrefix(vIdLen_, partId, vId, tagId_);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = env_->kvstore_->prefix(spaceId_, partId, prefix, &iter);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            VLOG(1) << "Error! ret = " << static_cast<int32_t>(ret)
                    << ", spaceId " << spaceId_;
            return ret;
        }
        if (iter && iter->valid()) {
            ret = collectTagPropIfValid(iter->key(), iter->val(), partId, vId);
        } else if (insertable_ && updateTagIds_.find(tagId_) != updateTagIds_.end()) {
           // insert one row about the tagId of vId
           ret = insertTagProps(partId, vId);
        } else {
            VLOG(3) << "Missed partId " << partId << ", vId " << vId << ", tagId " << tagId_;
            return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
        }
        return ret;
    }

    kvstore::ResultCode collectTagPropIfValid(folly::StringPiece key,
                                              folly::StringPiece value,
                                              const PartitionID partId,
                                              const VertexID vId) {
        // update newest schema version
        auto reader = RowReader::getRowReader(schema_, value);
        if (!reader) {
            VLOG(1) << "Can't get tag reader of " << tagId_;
            return kvstore::ResultCode::ERR_TAG_NOT_FOUND;
        }
        auto ttl = getTagTTLInfo();
        if (ttl.hasValue()) {
            auto ttlValue = ttl.value();
            if (CommonUtils::checkDataExpiredForTTL(schema_, reader.get(),
                                                     ttlValue.first, ttlValue.second)) {
                // insert one row about the tagId of vId
                if (insertable_ && updateTagIds_.find(tagId_) != updateTagIds_.end()) {
                    return insertTagProps(partId, vId);
                } else {
                    return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
                }
            }
        }
        return collectTagProps(reader.get(), key, value);
    }

    kvstore::ResultCode collectTagProps(RowReader* reader,
                                        folly::StringPiece key,
                                        folly::StringPiece value) {
        for (auto& prop : *props_) {
            VLOG(2) << "Collect prop " << prop.name_ << ", type " << tagId_;

            // read prop value
            auto retVal = readValue(reader, prop);
            if (!retVal.ok()) {
                VLOG(1) << "Bad value for tag: " << tagId_
                        << ", prop " << prop.name_;
                return kvstore::ResultCode::ERR_TAG_PROP_NOT_FOUND;
            }
            auto cloValue = std::move(retVal.value());

            // This is different from others node
            // all prop fields of this tag  new value puts filter_
            filter_->fillTagProp(tagId_, prop.name_, cloValue);
        }

        // Whether to insert
        insert_ = false;

        // update info
        if (updateTagIds_.find(tagId_) != updateTagIds_.end()) {
            rowWriter_ = std::make_unique<RowWriterV2>(schema_, value.str());
            key_ = key.str();
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

    StatusOr<Value> getDefaultProp(const nebula::meta::cpp2::PropertyType& type) {
        switch (type) {
            case nebula::meta::cpp2::PropertyType::BOOL: {
                return Value(false);
            }
            case nebula::meta::cpp2::PropertyType::TIMESTAMP:
            case nebula::meta::cpp2::PropertyType::INT64:
            case nebula::meta::cpp2::PropertyType::INT32:
            case nebula::meta::cpp2::PropertyType::INT16:
            case nebula::meta::cpp2::PropertyType::INT8: {
                return Value(0);
            }
            case nebula::meta::cpp2::PropertyType::FLOAT:
            case nebula::meta::cpp2::PropertyType::DOUBLE: {
                return Value(0.0);
            }
            case nebula::meta::cpp2::PropertyType::STRING: {
                return Value("");
            }
            case nebula::meta::cpp2::PropertyType::DATE: {
                Date date;
                return Value(date);
            }
            // TODO datatime FIXED_STRING
            default: {
                auto msg = folly::sformat("Unknown type: {}", static_cast<int32_t>(type));
                LOG(ERROR) << msg;
                return Status::Error(msg);
            }
        }
    }

    // This tagId needs insert props row
    // Failed when the props neither updated value nor has default value
    kvstore::ResultCode insertTagProps(const PartitionID partId,
                                       const VertexID vId) {
        insert_ = true;

        //  ?    When insert, the filter condition is always true
        //  upsert，first use default value， then use type default

        // the tagId props of props_ need default value or update value
        for (auto& prop : *props_) {
            // first check whether is updated field, then use default value
            if (prop.field_->hasDefault()) {
                // all fields new value puts filter_
                filter_->fillTagProp(tagId_, prop.name_, prop.field_->defaultValue());
            } else {
                bool isUpdateProp = false;
                for (auto& updateProp : updatedVertexProps_) {
                    auto toTagId = updateProp.get_tag_id();
                    auto propName = updateProp.get_name();
                    if (tagId_ == toTagId && !prop.name_.compare(propName)) {
                        isUpdateProp = true;
                        // Insert a type default value temporarily,
                        // it will be updated when update later
                        auto pType = schema_->getFieldType(prop.name_);
                        auto defaultValue = getDefaultProp(pType);
                        if (!defaultValue.ok()) {
                            return kvstore::ResultCode::ERR_INVALID_FIELD_VALUE;
                         }
                        filter_->fillTagProp(tagId_, prop.name_, defaultValue.value());
                        break;
                    }
                }

                // no default value, no nullable, no update prop
                if (!isUpdateProp) {
                    return kvstore::ResultCode::ERR_INVALID_FIELD_VALUE;
                }
            }
        }

        // build key, value is emtpy
        int64_t ms = time::WallClock::fastNowInMicroSec();
        auto now = std::numeric_limits<int64_t>::max() - ms;
        key_ = NebulaKeyUtils::vertexKey(vIdLen_, partId, vId, tagId_, now);
        rowWriter_ = std::make_unique<RowWriterV2>(schema_);
    }

    bool getInsert() {
        return insert_;
    }

    std::pair<std::string, std::unique_ptr<RowWriterV2>> getUpdateKV() {
        return std::make_pair(key_, rowWriter_);
    }

    FilterContext* getFilter() {
        return filter_.get();
    }

    TagID getTagID() {
        return tagId_;
    }

private:
    // Whether to allow insert
    bool                                                            insertable_{false};

    // buildTagContext set this, only update tagId
    std::unordered_set<TagID>                                       updateTagIds_;

    // the newest schema of this tagId
    const meta::NebulaSchemaProvider                               *schema_{nullptr};

    std::vector<storage::cpp2::UpdatedVertexProp>                   updatedVertexProps_;

    // ##############################out result###########################
    // Whether an insert has occurred
    // Because update one vid, so every tagId of one vertex has one row data at most
    // A tagUpdateNode process one row data, either update or insert
    bool                                                            insert_{false};

    // key
    std::string                                                     key_;
    // RowWriterV2(schema, old row values)
    std::unique_ptr<RowWriterV2>                                    rowWriter_;

    // <tagID, prop_name>-> prop_value because only updae one vertex
    // when update, prop value is old prop value
    // when udpate, only update one vertex
    // one tagId of one vertex,  prop value has one at most
    // std::unordered_map<std::pair<TagID, std::string>, nebula::Value> tagFilters_;

    // only collect oneself prop value
    std::unique_ptr<FilterContext>                                  filter_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_TAGNODE_H_
