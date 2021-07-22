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

namespace nebula {
namespace storage {

// TagNode will return a DataSet of specified props of tagId
class TagNode final : public IterateNode<VertexID> {
public:
    using RelNode::execute;

    TagNode(RunTimeContext* context,
            TagContext* ctx,
            TagID tagId,
            const std::vector<PropContext>* props,
            StorageExpressionContext* expCtx = nullptr,
            Expression* exp = nullptr)
        : context_(context)
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

    nebula::cpp2::ErrorCode execute(PartitionID partId, const VertexID& vId) override {
        reader_.reset();
        auto ret = RelNode::execute(partId, vId);
        if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
            return ret;
        }
        VLOG(1) << "partId " << partId << ", vId " << vId << ", tagId " << tagId_
                << ", prop size " << props_->size();

        // doodle
        std::unique_ptr<kvstore::KVIterator> iter;
        auto prefix = NebulaKeyUtils::vertexPrefix(context_->vIdLen(), partId, vId, tagId_);
        ret = context_->env()->kvstore_->prefix(context_->spaceId(), partId, prefix, &iter);
        if (ret == nebula::cpp2::ErrorCode::SUCCEEDED && iter && iter->valid()) {
            key_ = iter->key().str();
            value_ = iter->val().str();
            resetReader();
            return nebula::cpp2::ErrorCode::SUCCEEDED;
        }
        return ret;
    }

    nebula::cpp2::ErrorCode
    collectTagPropsIfValid(NullHandler nullHandler,
                           PropHandler valueHandler) {
        if (!valid()) {
            return nullHandler(props_);
        }
        return valueHandler(key_, reader_.get(), props_);
    }

    bool valid() const override {
        return !stopSearching_ && reader_ != nullptr;
    }

    void next() override {
        // tag only has one valid record, so stop iterate
        stopSearching_ = true;
    }

    folly::StringPiece key() const override {
        return key_;
    }

    folly::StringPiece val() const override {
        return value_;
    }

    RowReader* reader() const override {
        return reader_.get();
    }

    const std::string& getTagName() {
        return tagName_;
    }

    // doodle
    /*
    TagID getTagId() {
        return tagId_;
    }
    */

private:
    bool resetReader() {
        reader_.reset(*schemas_, value_);
        if (!reader_ || (ttl_.hasValue() && CommonUtils::checkDataExpiredForTTL(
            schemas_->back().get(), reader_.get(), ttl_.value().first, ttl_.value().second))) {
            reader_.reset();
            return false;
        }
        return true;
    }

    RunTimeContext                                                       *context_;
    TagContext                                                           *tagContext_;
    TagID                                                                 tagId_;
    const std::vector<PropContext>                                       *props_;
    StorageExpressionContext                                             *expCtx_;
    Expression                                                           *exp_;
    const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>> *schemas_ = nullptr;
    folly::Optional<std::pair<std::string, int64_t>>                      ttl_;
    std::string                                                           tagName_;

    bool                                                                  stopSearching_ = false;
    std::string                                                           key_;
    std::string                                                           value_;
    RowReaderWrapper                                                      reader_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_TAGNODE_H_
