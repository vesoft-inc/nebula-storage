/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_STORAGEITERATOR_H_
#define STORAGE_EXEC_STORAGEITERATOR_H_

#include "common/base/Base.h"
#include "kvstore/KVIterator.h"
#include "storage/CommonUtils.h"

namespace nebula {
namespace storage {

class StorageIterator {
public:
    virtual ~StorageIterator()  = default;

    virtual bool valid() const = 0;

    // next will skip invalid data, until it points to a valid data or it is invalid
    virtual void next() = 0;

    virtual folly::StringPiece key() const = 0;

    virtual folly::StringPiece val() const = 0;

    virtual RowReader* reader() const = 0;

    virtual bool dataError() const = 0;
};

class SingleTagIterator : public StorageIterator {
public:
    SingleTagIterator(std::unique_ptr<kvstore::KVIterator> iter,
                      TagID tagId,
                      size_t vIdLen,
                      const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>* schemas,
                      const folly::Optional<std::pair<std::string, int64_t>>* ttl)
        : iter_(std::move(iter))
        , tagId_(tagId)
        , vIdLen_(vIdLen)
        , schemas_(schemas)
        , ttl_(ttl) {
        lookupOne_ = true;
        check(iter_->val());
    }

    SingleTagIterator(folly::StringPiece val,
                      const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>* schemas,
                      const folly::Optional<std::pair<std::string, int64_t>>* ttl)
        : schemas_(schemas)
        , ttl_(ttl) {
        lookupOne_ = true;
        check(val);
    }

    bool valid() const override {
        return lookupOne_ && reader_ != nullptr;
    }

    void next() override {
        lookupOne_ = false;
    }

    folly::StringPiece key() const override {
        return iter_->key();
    }

    folly::StringPiece val() const override {
        return iter_->val();
    }

    RowReader* reader() const override {
        return reader_.get();
    }

    // only update use
    bool dataError() const override {
        return dataError_;
    }

protected:
    // return true when the value iter to a valid tag value
    bool check(folly::StringPiece val) {
        dataError_ = false;
        reader_ = RowReader::getRowReader(*schemas_, val);
        if (!reader_) {
            dataError_ = true;
            return false;
        }

        if (ttl_->hasValue()) {
            auto ttlValue = ttl_->value();
            if (CommonUtils::checkDataExpiredForTTL(schemas_->back().get(), reader_.get(),
                                                    ttlValue.first, ttlValue.second)) {
                reader_.reset();
                return false;
            }
        }

        return true;
    }

    std::unique_ptr<kvstore::KVIterator>                                  iter_;
    TagID                                                                 tagId_;
    size_t                                                                vIdLen_;
    const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>> *schemas_ = nullptr;
    const folly::Optional<std::pair<std::string, int64_t>>               *ttl_ = nullptr;
    bool                                                                  lookupOne_ = true;

    std::unique_ptr<RowReader>                                            reader_;
    bool                                                                  dataError_ = false;
};

// Iterator of single specified type
class SingleEdgeIterator : public StorageIterator {
public:
    SingleEdgeIterator(
            std::unique_ptr<kvstore::KVIterator> iter,
            EdgeType edgeType,
            size_t vIdLen,
            const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>* schemas,
            const folly::Optional<std::pair<std::string, int64_t>>* ttl,
            bool moveToValidRecord = true)
        : iter_(std::move(iter))
        , edgeType_(edgeType)
        , vIdLen_(vIdLen)
        , schemas_(schemas)
        , ttl_(ttl)
        , moveToValidRecord_(moveToValidRecord) {
        CHECK(!!iter_);
        lookupOne_ = true;
        // If moveToValidRecord is true, iterator will try to move to first valid record,
        // which is used in GetNeighbors. If it is false, it will only check the latest record,
        // which is used in GetProps and UpdateEdge.
        if (moveToValidRecord_) {
            while (iter_->valid() && !check()) {
                iter_->next();
            }
        } else {
            check();
        }
    }

    bool valid() const override {
        return lookupOne_ && reader_ != nullptr;
    }

    void next() override {
        if (!moveToValidRecord_) {
            lookupOne_ = false;
            return;
        }
        do {
            iter_->next();
            if (!iter_->valid()) {
                reader_.reset();
                break;
            }
        } while (!check());
    }

    folly::StringPiece key() const override {
        return iter_->key();
    }

    folly::StringPiece val() const override {
        return iter_->val();
    }

    RowReader* reader() const override {
        return reader_.get();
    }

    EdgeType edgeType() const {
        return edgeType_;
    }

    // only update use
    bool dataError() const override {
        return dataError_;
    }

protected:
    // return true when the value iter to a valid edge value
    bool check() {
        reader_.reset();
        dataError_ = false;
        auto key = iter_->key();
        auto rank = NebulaKeyUtils::getRank(vIdLen_, key);
        auto dstId = NebulaKeyUtils::getDstId(vIdLen_, key);
        if (!firstLoop_ && rank == lastRank_ && lastDstId_ == dstId) {
            // pass old version data of same edge
            return false;
        }

        auto val = iter_->val();
        if (!reader_) {
            reader_ = RowReader::getRowReader(*schemas_, val);
            if (!reader_) {
                dataError_ = true;
                return false;
            }
        } else if (!reader_->reset(*schemas_, val)) {
            dataError_ = true;
            return false;
        }

        firstLoop_ = false;
        lastRank_ = rank;
        lastDstId_ = dstId.str();

        if (ttl_->hasValue()) {
            auto ttlValue = ttl_->value();
            if (CommonUtils::checkDataExpiredForTTL(schemas_->back().get(), reader_.get(),
                                                    ttlValue.first, ttlValue.second)) {
                reader_.reset();
                return false;
            }
        }

        return true;
    }

    std::unique_ptr<kvstore::KVIterator>                                  iter_;
    EdgeType                                                              edgeType_;
    size_t                                                                vIdLen_;
    const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>> *schemas_ = nullptr;
    const folly::Optional<std::pair<std::string, int64_t>>               *ttl_ = nullptr;
    bool                                                                  moveToValidRecord_{true};
    bool                                                                  lookupOne_ = true;

    std::unique_ptr<RowReader>                                            reader_;
    EdgeRanking                                                           lastRank_ = 0;
    VertexID                                                              lastDstId_ = "";
    bool                                                                  firstLoop_ = true;
    bool                                                                  dataError_ = false;
};

// Iterator of multiple SingleEdgeIterator, it will iterate over edges of different types
class MultiEdgeIterator : public StorageIterator {
public:
    // will move to a valid SingleEdgeIterator if there is one
    explicit MultiEdgeIterator(std::vector<SingleEdgeIterator*> iters)
        : iters_(std::move(iters)) {
        moveToNextValidIterator();
    }

    bool valid() const override {
        return curIter_ < iters_.size();
    }

    void next() override {
        iters_[curIter_]->next();
        if (!iters_[curIter_]->valid()) {
            moveToNextValidIterator();
        }
    }

    folly::StringPiece key() const override {
        return iters_[curIter_]->key();
    }

    folly::StringPiece val() const override {
        return iters_[curIter_]->val();
    }

    RowReader* reader() const override {
        return iters_[curIter_]->reader();
    }

    EdgeType edgeType() const {
        return iters_[curIter_]->edgeType();
    }

    // return the index of multiple iterators
    size_t getIdx() const {
        return curIter_;
    }

    bool dataError() const override {
        if (iters_.empty() || !iters_[curIter_]) {
            return false;
        }
        if (curIter_ < iters_.size()) {
            return iters_[curIter_]->dataError();
        }
        return false;
    }

private:
    void moveToNextValidIterator() {
        while (curIter_ < iters_.size()) {
            if (iters_[curIter_] && iters_[curIter_]->valid()) {
                return;
            }
            ++curIter_;
        }
    }

private:
    std::vector<SingleEdgeIterator*> iters_;
    size_t curIter_ = 0;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_STORAGEITERATOR_H_
