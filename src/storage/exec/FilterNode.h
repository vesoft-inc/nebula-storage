/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_FILTERNODE_H_
#define STORAGE_EXEC_FILTERNODE_H_

#include "base/Base.h"
#include "expression/Expression.h"
#include "context/ExpressionContext.h"
#include "storage/exec/TagNode.h"
#include "storage/exec/EdgeNode.h"

namespace nebula {
namespace storage {

// FilterNode has input of serveral TagNode and EdgeNode, the EdgeNode could be either several
// EdgeTypePrefixScanNode of different edge types, or a single VertexPrefixScanNode which scan
// all edges of a vertex
class FilterNode : public RelNode, public StorageIterator {
public:
    FilterNode(const Expression* exp,
               const std::vector<TagNode*>& tagNodes,
               const std::vector<EdgeNode*>& edgeNodes,
               TagContext* tagContext,
               EdgeContext* edgeContext)
        : exp_(exp)
        , tagNodes_(tagNodes)
        , edgeNodes_(edgeNodes)
        , tagContext_(tagContext)
        , edgeContext_(edgeContext) {}

    folly::Future<kvstore::ResultCode> execute(PartitionID, const VertexID&) override {
        std::vector<StorageIterator*> iters;
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

    EdgeType edgeType() const override {
        return edgeType_;
    }

    size_t idx() {
        return columnIdx_;
    }

    RowReader* reader() {
        return reader_.get();
    }

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
            ttl_ = getEdgeTTLInfo();
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
            exp_->eval();
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

class TagFilterNode : public FilterNode {
public:
    TagFilterNode(const Expression* filterExp,
                  const std::vector<TagUpdateNode*>& tagUpdates,
                  TagContext* tagContext)
        : filterExp_(filterExp)
        , tagUpdates_(tagUpdates)
        , tagContext_(tagContext) {}

    folly::Future<kvstore::ResultCode> execute(PartitionID, const VertexID&) override {
        std::vector<StorageIterator*> iters;
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

    EdgeType edgeType() const override {
        return edgeType_;
    }

    size_t idx() {
        return columnIdx_;
    }

    RowReader* reader() {
        return reader_.get();
    }

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
            ttl_ = getEdgeTTLInfo();
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
            exp_->eval();
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
    const Expression                *filterExp_;
    std::vector<TagUpdateNode*>      tagUpdates_;
    TagContext                      *tagContext_;
    FilterContext                   *filter_;

    EdgeType edgeType_ = 0;
    size_t columnIdx_;
    const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>* schemas_ = nullptr;
    const std::vector<PropContext>* props_ = nullptr;
    folly::Optional<std::pair<std::string, int64_t>> ttl_;

    std::unique_ptr<RowReader> reader_;
    std::unique_ptr<StorageIterator> iter_;
};






}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_FILTERNODE_H_
