/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_TRANSACTION_TOSSEDGEITERATOR_H_
#define STORAGE_TRANSACTION_TOSSEDGEITERATOR_H_

#include <folly/String.h>

#include "common/base/Base.h"
#include "kvstore/KVIterator.h"
#include "storage/CommonUtils.h"
#include "storage/exec/StorageIterator.h"
#include "storage/transaction/TransactionManager.h"
#include "storage/StorageFlags.h"

namespace nebula {
namespace storage {

class TossEdgeIterator : public SingleEdgeIterator {
public:
    TossEdgeIterator(PlanContext* planCtx,
                     std::unique_ptr<kvstore::KVIterator> iter,
                     EdgeType edgeType,
                     const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>* schemas,
                     const folly::Optional<std::pair<std::string, int64_t>>* ttl,
                     bool stopAtFirstEdge)
        : SingleEdgeIterator() {
        planContext_ = planCtx;
        iter_ = std::move(iter);
        edgeType_ = edgeType;
        schemas_ = schemas;
        stopAtFirstEdge_ = stopAtFirstEdge;
        recoverEdgesIter_ = recoverEdges_.end();
        if (ttl->hasValue()) {
            hasTtl_ = true;
            ttlCol_ = ttl->value().first;
            ttlDuration_ = ttl->value().second;
        }
        if (isEdge(iter_->key()) && setReader(iter_->val())) {
            lastRank_ = NebulaKeyUtils::getRank(planContext_->vIdLen_, iter_->key());
            lastDstId_ = NebulaKeyUtils::getDstId(planContext_->vIdLen_, iter_->key()).str();
            if (stopAtFirstEdge_) {
                stopSearching_ = true;
            }
            LOG_IF(INFO, FLAGS_trace_toss)
                << "TossEdgeIterator::TossEdgeIterator()"
                << ", key=" << TransactionUtils::hexEdgeId(planContext_->vIdLen_, iter_->key());
        } else {
            next();
        }
    }

    folly::StringPiece key() const override {
        if (iter_->valid()) {
            return iter_->key();
        } else if (recoverEdgesIter_ != recoverEdges_.end()) {
            auto rLockedPtr = (*recoverEdgesIter_)->rlock();
            return rLockedPtr->first;
        }
        return iter_->key();
    }

    bool valid() const override {
        if (!iter_->valid() && recoverEdgesIter_ == recoverEdges_.end()) {
            LOG_IF(INFO, FLAGS_trace_toss) << "TossEdgeIterator::valid() = false";
            return false;
        }
        auto ret = (!stopSearching_) && (reader_ != nullptr);

        if (FLAGS_trace_toss) {
            std::string key;
            std::string val;
            if (iter_->valid()) {
                key = iter_->key().str();
                val = iter_->val().str();
            } else {
                (*recoverEdgesIter_)->withRLock([&](auto&& data){
                    key = data.first;
                    val = data.second;
                });
            }
        }
        return ret;
    }

    /**
     * @brief this iterator has two parts
     * 1st. normal scan iterator from begin to end.
     * 2nd. return the resume locks\edges.
     *
     * iter_ will invalid after step1.
     */
    void next() override {
        if (stopSearching_) {
            return;
        }
        reader_.reset();

        // Step 1: normal scan iterator from begin to end.
        while (iter_->valid()) {
            iter_->next();
            if (!iter_->valid()) {
                break;
            }
            if (isEdge(iter_->key())) {
                LOG_IF(INFO, FLAGS_trace_toss)
                    << "TossEdgeIterator::next(), to an edge, hex="
                    << TransactionUtils::hexEdgeId(planContext_->vIdLen_, iter_->key());
                if (isLatestEdge(iter_->key()) && setReader(iter_->val())) {
                    lastRank_ = NebulaKeyUtils::getRank(planContext_->vIdLen_, iter_->key());
                    lastDstId_ = NebulaKeyUtils::
                                    getDstId(planContext_->vIdLen_, iter_->key()).str();
                    lastIsLock_ = false;
                    if (stopAtFirstEdge_) {
                        stopSearching_ = true;
                    }
                    LOG_IF(INFO, FLAGS_trace_toss)
                        << "TossEdgeIterator::next(), return edge hex="
                        << TransactionUtils::hexEdgeId(planContext_->vIdLen_, iter_->key());
                    return;
                }

                /**
                 * if meet a lock before, will delay this edge return.
                 */
                if (lastIsLock_) {
                    LOG_IF(INFO, FLAGS_trace_toss)
                        << "TossEdgeIterator::next(), prev is a lock, hex="
                        << TransactionUtils::hexEdgeId(planContext_->vIdLen_, iter_->key());
                    auto tryLockData = recoverEdges_.back()->tryWLock();
                    if (tryLockData && tryLockData->first.empty()) {
                        tryLockData->first = iter_->key().str();
                        tryLockData->second = iter_->val().str();
                    }
                    lastIsLock_ = false;
                    continue;
                }
            } else if (NebulaKeyUtils::isLock(planContext_->vIdLen_, iter_->key())) {
                std::string rawKey = NebulaKeyUtils::toEdgeKey(iter_->key());
                auto rank = NebulaKeyUtils::getRank(planContext_->vIdLen_, rawKey);
                auto dstId = NebulaKeyUtils::getDstId(planContext_->vIdLen_, rawKey).str();
                if (!lastIsLock_ && rank == lastRank_ && dstId == lastDstId_) {
                    continue;
                }
                if (rank != lastRank_ || dstId != lastDstId_) {
                    recoverEdges_.emplace_back(
                        std::make_shared<folly::Synchronized<KV>>(std::make_pair("", "")));
                }

                resumeTasks_.emplace_back(
                    planContext_->env_->txnMan_->resumeTransaction(planContext_->vIdLen_,
                                                                   planContext_->spaceId_,
                                                                   iter_->key().str(),
                                                                   iter_->val().str(),
                                                                   recoverEdges_.back()));
                lastRank_ = NebulaKeyUtils::getRank(planContext_->vIdLen_, rawKey);
                lastDstId_ = NebulaKeyUtils::getDstId(planContext_->vIdLen_, rawKey).str();
                lastIsLock_ = true;
                continue;
            } else {
                LOG_IF(INFO, FLAGS_trace_toss)
                    << "next to a weird record: \n"
                    << folly::hexDump(iter_->key().data(), iter_->key().size())
                    << folly::hexDump(iter_->val().data(), iter_->val().size());
            }
        }   // end while(iter_->valid())
        LOG_IF(INFO, FLAGS_trace_toss) << "TossEdgeIterator::next(), no more iter to read";

        // Step 2: return the resumed locks/edges.
        // set recoverEdgesIter_ as begin() at first time. else ++recoverEdgesIter_
        if (needWaitResumeTask_) {
            folly::collectAll(resumeTasks_).wait();
            needWaitResumeTask_ = false;
            recoverEdgesIter_ = recoverEdges_.begin();
        } else {
            recoverEdgesIter_++;
        }

        if (recoverEdgesIter_ != recoverEdges_.end()) {
            auto data = (*recoverEdgesIter_)->copy();
            if (!data.second.empty()) {
                setReader(data.second);
            }
        }
    }

    bool isEdge(const folly::StringPiece& key) {
        return NebulaKeyUtils::isEdge(planContext_->vIdLen_, key);
    }

    bool isLatestEdge(const folly::StringPiece& key) {
        auto rank = NebulaKeyUtils::getRank(planContext_->vIdLen_, key);
        auto dstId = NebulaKeyUtils::getDstId(planContext_->vIdLen_, key);
        return !(rank == lastRank_ && dstId == lastDstId_);
    }

    bool setReader(folly::StringPiece val) {
        reader_.reset();
        if (!reader_) {
            reader_.reset(*schemas_, val);
            if (!reader_) {
                planContext_->resultStat_ = ResultStatus::ILLEGAL_DATA;
                return false;
            }
        } else if (!reader_->reset(*schemas_, val)) {
            planContext_->resultStat_ = ResultStatus::ILLEGAL_DATA;
            return false;
        }

        if (hasTtl_ && CommonUtils::checkDataExpiredForTTL(
                                            schemas_->back().get(), reader_.get(),
                                            ttlCol_, ttlDuration_)) {
            reader_.reset();
            return false;
        }
        return true;
    }

private:
    // using shared_ptr because this Iterator may be deleted if there is a limit in nGQL
    // using folly::Synchronized because the scan thread and the resume thread may write same ptr
    using TResultsItem = std::shared_ptr<folly::Synchronized<KV>>;
    bool                                                 needWaitResumeTask_{true};
    bool                                                 lastIsLock_{false};
    /**
     * getNeighbors need to scan all edges, but update will stop at first edge,
     * use stopAtFirstEdge_ to let caller tell if this need to stop early
     * use stopSearching_ to judge inside.
     */
    bool                                                 stopAtFirstEdge_{false};
    bool                                                 stopSearching_{false};
    std::list<folly::SemiFuture<cpp2::ErrorCode>>        resumeTasks_;
    std::list<TResultsItem>                              recoverEdges_;
    std::list<TResultsItem>::iterator                    recoverEdgesIter_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_TRANSACTION_TOSSEDGEITERATOR_H_
