/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_EDGENODE_H_
#define STORAGE_EXEC_EDGENODE_H_

#include <ostream>
#include "common/base/Base.h"
#include "storage/exec/RelNode.h"
#include "storage/exec/StorageIterator.h"
#include "storage/transaction/TransactionManager.h"
#include "storage/transaction/TossEdgeIterator.h"

namespace nebula {
namespace storage {

// EdgeNode will return a StorageIterator which iterates over the specified
// edgeType of given vertexId
template<typename T>
class EdgeNode : public IterateNode<T> {
public:
    SingleEdgeIterator* iter() {
        return iter_.get();
    }

    nebula::cpp2::ErrorCode
    collectEdgePropsIfValid(NullHandler nullHandler,
                            PropHandler valueHandler) {
        if (!iter_ || !iter_->valid()) {
            return nullHandler(props_);
        }
        return valueHandler(iter_->key(), iter_->reader(), props_);
    }

    bool valid() const override {
        return iter_ && iter_->valid();
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
        if (iter_) {
            return iter_->reader();
        }
        return nullptr;
    }

    const std::string& getEdgeName() {
        return edgeName_;
    }

protected:
    EdgeNode(RunTimeContext* context,
             EdgeContext* edgeContext,
             EdgeType edgeType,
             const std::vector<PropContext>* props,
             StorageExpressionContext* expCtx,
             Expression* exp)
        : context_(context)
        , edgeContext_(edgeContext)
        , edgeType_(edgeType)
        , props_(props)
        , expCtx_(expCtx)
        , exp_(exp) {
        UNUSED(expCtx_); UNUSED(exp_);
        auto schemaIter = edgeContext_->schemas_.find(std::abs(edgeType_));
        CHECK(schemaIter != edgeContext_->schemas_.end());
        CHECK(!schemaIter->second.empty());
        schemas_ = &(schemaIter->second);
        ttl_ = QueryUtils::getEdgeTTLInfo(edgeContext_, std::abs(edgeType_));
        edgeName_ = edgeContext_->edgeNames_[edgeType_];
    }

    EdgeNode(RunTimeContext* context,
             EdgeContext* ctx)
        : context_(context)
        , edgeContext_(ctx) {}

    RunTimeContext* context_;
    EdgeContext* edgeContext_;
    EdgeType edgeType_;
    const std::vector<PropContext>* props_;
    StorageExpressionContext* expCtx_;
    Expression* exp_;

    const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>* schemas_ = nullptr;
    folly::Optional<std::pair<std::string, int64_t>> ttl_;
    std::string edgeName_;

    std::unique_ptr<SingleEdgeIterator> iter_;
    std::string prefix_;
};

// FetchEdgeNode is used to fetch a single edge
class FetchEdgeNode final : public EdgeNode<cpp2::EdgeKey> {
public:
    using RelNode::execute;

    FetchEdgeNode(RunTimeContext* context,
                  EdgeContext* edgeContext,
                  EdgeType edgeType,
                  const std::vector<PropContext>* props,
                  StorageExpressionContext* expCtx = nullptr,
                  Expression* exp = nullptr)
        : EdgeNode(context, edgeContext, edgeType, props, expCtx, exp) {}

    nebula::cpp2::ErrorCode execute(PartitionID partId, const cpp2::EdgeKey& edgeKey) override {
        auto ret = RelNode::execute(partId, edgeKey);
        if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
            return ret;
        }

        VLOG(1) << "partId " << partId << ", edgeType " << edgeType_
                << ", prop size " << props_->size();
        if (edgeType_ !=  *edgeKey.edge_type_ref()) {
            iter_.reset();
            return nebula::cpp2::ErrorCode::SUCCEEDED;
        }
        prefix_ = NebulaKeyUtils::edgePrefix(context_->vIdLen(),
                                             partId,
                                             (*edgeKey.src_ref()).getStr(),
                                             *edgeKey.edge_type_ref(),
                                             *edgeKey.ranking_ref(),
                                             (*edgeKey.dst_ref()).getStr());
        std::unique_ptr<kvstore::KVIterator> iter;
        ret = context_->env()->kvstore_->prefix(context_->spaceId(), partId, prefix_, &iter);
        if (ret == nebula::cpp2::ErrorCode::SUCCEEDED && iter && iter->valid()) {
            if (context_->env()->txnMan_ &&
                context_->env()->txnMan_->enableToss(context_->spaceId())) {
                bool stopAtFirstEdge = true;
                iter_.reset(new TossEdgeIterator(
                    context_, std::move(iter), edgeType_, schemas_, &ttl_, stopAtFirstEdge));
            } else {
                iter_.reset(new SingleEdgeIterator(
                    context_, std::move(iter), edgeType_, schemas_, &ttl_, false));
            }
        } else {
            iter_.reset();
        }
        return ret;
    }
};

template <typename T>
// SingleEdgeNode is used to scan all edges of a specified edgeType of the same srcId
class SingleEdgeNode final : public EdgeNode<T> {
public:
    using RelNode<T>::execute;
    SingleEdgeNode(RunTimeContext* context,
                   EdgeContext* edgeContext,
                   EdgeType edgeType,
                   const std::vector<PropContext>* props,
                   StorageExpressionContext* expCtx = nullptr,
                   Expression* exp = nullptr)
        : EdgeNode<T>(context, edgeContext, edgeType, props, expCtx, exp) {}

    nebula::cpp2::ErrorCode execute(PartitionID partId, const T& vId) override {
        auto ret = RelNode<T>::execute(partId, vId);
        if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
            return ret;
        }

        std::unique_ptr<kvstore::KVIterator> iter;
        if constexpr (std::is_same_v<T, VertexID>) {
            this->prefix_ = NebulaKeyUtils::edgePrefix(this->context_->vIdLen(),
                                                    partId, vId, this->edgeType_);
        } else if constexpr (std::is_same_v<T, std::pair<VertexID, VertexID>>) {
            this->prefix_ = NebulaKeyUtils::edgePrefix(this->context_->vIdLen(),
                                                    partId, vId.first, this->edgeType_);
        } else {
            LOG(FATAL) << "Invlaid key type for edge scan.";
        }
        ret = this->context_->env()->kvstore_->prefix(this->context_->spaceId(),
                                                         partId, this->prefix_, &iter);
        if (ret == nebula::cpp2::ErrorCode::SUCCEEDED && iter && iter->valid()) {
            if (this->context_->env()->txnMan_ &&
                this->context_->env()->txnMan_->enableToss(this->context_->spaceId())) {
                bool stopAtFirstEdge = false;
                this->iter_.reset(new TossEdgeIterator(
                    this->context_, std::move(iter),
                    this->edgeType_, this->schemas_, &this->ttl_, stopAtFirstEdge));
            } else {
                if constexpr (std::is_same_v<T, VertexID>) {
                    this->iter_.reset(new SingleEdgeIterator(
                        this->context_, std::move(iter), this->edgeType_,
                        this->schemas_, &this->ttl_));
                } else if constexpr (std::is_same_v<T, std::pair<VertexID, VertexID>>) {
                    this->iter_.reset(new SingleEdgeIterator(
                        this->context_, std::move(iter),
                        this->edgeType_, this->schemas_, &this->ttl_, true, vId.second));
                } else {
                    LOG(FATAL) << "Invlaid key type for edge scan.";
                }
            }
        } else {
            this->iter_.reset();
        }
        return ret;
    }
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_EDGENODE_H_
