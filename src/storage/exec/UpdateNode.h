/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_UPDATENODE_H_
#define STORAGE_EXEC_UPDATENODE_H_

#include "common/base/Base.h"
#include "common/expression/Expression.h"
#include "storage/context/UpdateExpressionContext.h"
#include "storage/exec/TagNode.h"
#include "storage/exec/FilterNode.h"
#include "kvstore/LogEncoder.h"

namespace nebula {
namespace storage {

// Only use for update vertex
// Update records, write to kvstore
class UpdateTagNode : public RelNode<VertexID> {
public:
    UpdateTagNode(StorageEnv* env,
                  GraphSpaceID spaceId,
                  size_t vIdLen,
                  std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>> indexes,
                  std::vector<storage::cpp2::UpdatedProp>& updatedProps,
                  TagFilterNode* filterNode)
        : env_(env)
        , spaceId_(spaceId)
        , vIdLen_(vIdLen)
        , indexes_(indexes)
        , updatedProps_(updatedProps)
        , filterNode_(filterNode) {
        }

    kvstore::ResultCode execute(PartitionID partId, const VertexID& vId) override {
        CHECK_NOTNULL(env_->kvstore_);

        folly::Baton<true, std::atomic> baton;
        auto ret = kvstore::ResultCode::SUCCEEDED;
        env_->kvstore_->asyncAtomicOp(spaceId_, partId,
            [&partId, &vId, this] ()
            -> folly::Optional<std::string> {
                this->exeResult_ = RelNode::execute(partId, vId);
                if (this->exeResult_ == kvstore::ResultCode::SUCCEEDED) {
                    this->tagId_ = filterNode_->getTagId();
                    this->key_ = filterNode_->getKey();
                    this->val_ = filterNode_->getValue();
                    this->rowWriter_ = filterNode_->getRowWriter();
                    this->filter_ = filterNode_->getFilterCont();
                    this->insert_ = filterNode_->getInsert();
                    this->expCtx_ = filterNode_->getExpressionContext();
                    return this->updateAndWriteBack(partId, vId);
                } else {
                    if (this->exeResult_ == kvstore::ResultCode::ERR_RESULT_FILTERED) {
                        this->tagId_ = filterNode_->getTagId();
                        this->filter_ = filterNode_->getFilterCont();
                        this->insert_ = filterNode_->getInsert();
                        this->expCtx_ = filterNode_->getExpressionContext();
                    }
                    return folly::none;
                }
            },
            [&ret, &baton, this] (kvstore::ResultCode code) {
                if (code == kvstore::ResultCode::ERR_ATOMIC_OP_FAILED &&
                    this->exeResult_ != kvstore::ResultCode::SUCCEEDED) {
                    ret = this->exeResult_;
                } else {
                    ret = code;
                }
                baton.post();
            });
        baton.wait();

        return ret;
    }

    folly::Optional<std::string>
    updateAndWriteBack(const PartitionID partId, const VertexID vId) {
        for (auto& updateProp : updatedProps_) {
            auto propName = updateProp.get_name();
            auto updateExp = Expression::decode(updateProp.get_value());
            if (!updateExp) {
                LOG(ERROR) << "Update expression decode failed " << updateProp.get_value();
                return folly::none;
            }
            auto updateVal = updateExp->eval(*expCtx_);
            // update prop value to filter_
            filter_->fillTagProp(tagId_, propName, updateVal);

            // update expression context
            auto tagName = env_->schemaMan_->toTagName(spaceId_, tagId_);
            if (!tagName.ok()) {
                LOG(ERROR) << "Can't find spaceId " << spaceId_ << " tagId " << tagId_;
                return folly::none;
            }
            expCtx_->setSrcProp(tagName.value(), propName, updateVal);
        }

        auto tagPropMap = filter_->getTagFilter();
        for (auto& e : tagPropMap) {
            if (e.first.first == tagId_) {
                auto wRet = rowWriter_->setValue(e.first.second, e.second);
                if (wRet != WriteResult::SUCCEEDED) {
                    LOG(ERROR) << "Add field faild ";
                    return folly::none;
                }
            }
        }

        std::unique_ptr<kvstore::BatchHolder> batchHolder
            = std::make_unique<kvstore::BatchHolder>();

        auto wRet = rowWriter_->finish();
        if (wRet != WriteResult::SUCCEEDED) {
            LOG(ERROR) << "Add field faild ";
            return folly::none;
        }

        auto nVal = rowWriter_->moveEncodedStr();

        // update index if exists
        // Note: when insert_ is true, either there is no origin data or TTL expired
        // when there is no origin data, there is no the old index.
        // when TTL exists, there is no index.
        // when insert_ is true, not old index, val_ is empty.
        if (!indexes_.empty()) {
            std::unique_ptr<RowReader> nReader, oReader;
            for (auto& index : indexes_) {
                if (tagId_ == index->get_schema_id().get_tag_id()) {
                    // step 1, delete old version index if exists.
                    if (!val_.empty()) {
                        if (!oReader) {
                            oReader = RowReader::getTagPropReader(env_->schemaMan_,
                                                                  spaceId_,
                                                                  tagId_,
                                                                  val_);
                        }
                        if (!oReader) {
                            LOG(ERROR) << "Bad format row";
                            return folly::none;
                        }
                        auto oi = indexKey(partId, vId, oReader.get(), index);
                        if (!oi.empty()) {
                            batchHolder->remove(std::move(oi));
                        }
                    }

                    // step 2, insert new vertex index
                    if (!nReader) {
                        nReader = RowReader::getTagPropReader(env_->schemaMan_,
                                                              spaceId_,
                                                              tagId_,
                                                              nVal);
                    }
                    if (!nReader) {
                        LOG(ERROR) << "Bad format row";
                        return folly::none;
                    }
                    auto ni = indexKey(partId, vId, nReader.get(), index);
                    if (!ni.empty()) {
                        batchHolder->put(std::move(ni), "");
                    }
                }
            }
        }
        // step 3, insert new vertex data
        batchHolder->put(std::move(key_), std::move(nVal));
        return encodeBatchValue(batchHolder->getBatch());
    }

    std::string indexKey(PartitionID partId,
                         VertexID vId,
                         RowReader* reader,
                         std::shared_ptr<nebula::meta::cpp2::IndexItem> index) {
        std::vector<Value::Type> colsType;
        auto values = IndexKeyUtils::collectIndexValues(reader, index->get_fields(), colsType);
        if (!values.ok()) {
            return "";
        }
        return IndexKeyUtils::vertexIndexKey(vIdLen_, partId, index->get_index_id(),
                                             vId, values.value(), colsType);
    }

    FilterContext* getFilterCont() {
        return filter_;
    }

    bool getInsert() {
        return insert_;
    }

    UpdateExpressionContext* getExpressionContext() {
        return expCtx_;
    }


private:
    // ============================ input =====================================================
    StorageEnv                                                             *env_;
    GraphSpaceID                                                            spaceId_;
    size_t                                                                  vIdLen_;
    std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>>             indexes_;
    // update <tagID, prop name, new value expression>
    std::vector<storage::cpp2::UpdatedProp>                                 updatedProps_;
    TagFilterNode                                                          *filterNode_;

    TagID                                                                   tagId_;
    std::string                                                             key_;
    // use to save old row value
    std::string                                                             val_;
    RowWriterV2*                                                            rowWriter_;

    // ============================ output ====================================================
    // input and update, then output
    FilterContext                                                          *filter_;
    bool                                                                    insert_{false};
    UpdateExpressionContext                                                *expCtx_;
    std::atomic<kvstore::ResultCode>                                        exeResult_;
};

// Only use for update edge
// Update records, write to kvstore
class UpdateEdgeNode : public RelNode<cpp2::EdgeKey> {
public:
    UpdateEdgeNode(StorageEnv* env,
                   GraphSpaceID spaceId,
                   size_t vIdLen,
                   std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>> indexes,
                   std::vector<storage::cpp2::UpdatedProp>& updatedProps,
                   EdgeFilterNode* filterNode)
        : env_(env)
        , spaceId_(spaceId)
        , vIdLen_(vIdLen)
        , indexes_(indexes)
        , updatedProps_(updatedProps)
        , filterNode_(filterNode) {
        }

    kvstore::ResultCode execute(PartitionID partId, const cpp2::EdgeKey& edgeKey) override {
        CHECK_NOTNULL(env_->kvstore_);

        folly::Baton<true, std::atomic> baton;
        auto ret = kvstore::ResultCode::SUCCEEDED;
        env_->kvstore_->asyncAtomicOp(spaceId_, partId,
            [&partId, &edgeKey, this] ()
            -> folly::Optional<std::string> {
                this->exeResult_ = RelNode::execute(partId, edgeKey);
                if (this->exeResult_ == kvstore::ResultCode::SUCCEEDED) {
                    this->edgeType_ = filterNode_->getEdgeType();
                    this->key_ = filterNode_->getKey();
                    this->val_ = filterNode_->getValue();
                    this->rowWriter_ = filterNode_->getRowWriter();
                    this->filter_ = filterNode_->getFilterCont();
                    this->insert_ = filterNode_->getInsert();
                    this->expCtx_ = filterNode_->getExpressionContext();
                    return this->updateAndWriteBack(partId, edgeKey);
                } else {
                    if (this->exeResult_ == kvstore::ResultCode::ERR_RESULT_FILTERED) {
                        this->edgeType_ = filterNode_->getEdgeType();
                        this->filter_ = filterNode_->getFilterCont();
                        this->insert_ = filterNode_->getInsert();
                        this->expCtx_ = filterNode_->getExpressionContext();
                    }
                    return folly::none;
                }
            },
            [&ret, &baton, this] (kvstore::ResultCode code) {
                if (code == kvstore::ResultCode::ERR_ATOMIC_OP_FAILED &&
                    this->exeResult_ != kvstore::ResultCode::SUCCEEDED) {
                    ret = this->exeResult_;
                } else {
                    ret = code;
                }
                baton.post();
            });
        baton.wait();

        return ret;
    }

    folly::Optional<std::string>
    updateAndWriteBack(const PartitionID partId, const cpp2::EdgeKey& edgeKey) {
        if (edgeType_ != edgeKey.edge_type) {
            VLOG(1) << "Update edge faild ";
            return folly::none;
        }

        // update expression context
        auto edgeRet = env_->schemaMan_->toEdgeName(spaceId_, edgeType_);
        if (!edgeRet.ok()) {
            VLOG(1) << "Can't find spaceId " << spaceId_
                    << " edgeType " << edgeType_;
            return folly::none;
        }
        auto edgeName = edgeRet.value();

        for (auto& updateProp : updatedProps_) {
            auto propName = updateProp.get_name();
            auto updateExp = Expression::decode(updateProp.get_value());
            if (!updateExp) {
                return folly::none;
            }
            auto updateVal = updateExp->eval(*expCtx_);
            // update prop value to filter_
            filter_->fillEdgeProp(edgeType_, propName, updateVal);
            expCtx_->setEdgeProp(edgeName, propName, updateVal);
        }


        auto edgePropMap = filter_->getEdgeFilter();
        for (auto& e : edgePropMap) {
            if (e.first.first == edgeType_) {
                auto wRet = rowWriter_->setValue(e.first.second, e.second);
                if (wRet != WriteResult::SUCCEEDED) {
                    VLOG(1) << "Add field faild ";
                    return folly::none;
                }
            }
        }

        std::unique_ptr<kvstore::BatchHolder> batchHolder
            = std::make_unique<kvstore::BatchHolder>();

        auto wRet = rowWriter_->finish();
        if (wRet != WriteResult::SUCCEEDED) {
            VLOG(1) << "Add field faild ";
            return folly::none;
        }

        auto nVal = rowWriter_->moveEncodedStr();
        // update index if exists
        // Note: when insert_ is true, either there is no origin data or TTL expired
        // when there is no origin data, there is no the old index.
        // when TTL exists, there is no index.
        // when insert_ is true, not old index, val_ is empty.
        if (!indexes_.empty()) {
            std::unique_ptr<RowReader> nReader, oReader;
            for (auto& index : indexes_) {
                if (edgeType_ == index->get_schema_id().get_edge_type()) {
                    // step 1, delete old version index if exists.
                    if (!val_.empty()) {
                        if (!oReader) {
                            oReader = RowReader::getEdgePropReader(env_->schemaMan_,
                                                                   spaceId_,
                                                                   edgeType_,
                                                                   val_);
                        }
                        if (!oReader) {
                            LOG(ERROR) << "Bad format row";
                            return folly::none;
                        }
                        auto oi = indexKey(partId, oReader.get(), edgeKey, index);
                        if (!oi.empty()) {
                            batchHolder->remove(std::move(oi));
                        }
                    }

                    // step 2, insert new edge index
                    if (!nReader) {
                        nReader = RowReader::getEdgePropReader(env_->schemaMan_,
                                                               spaceId_,
                                                               edgeType_,
                                                               nVal);
                    }
                    if (!nReader) {
                        LOG(ERROR) << "Bad format row";
                        return folly::none;
                    }
                    auto ni = indexKey(partId, nReader.get(), edgeKey, index);
                    if (!ni.empty()) {
                        batchHolder->put(std::move(ni), "");
                    }
                }
            }
        }
        // step 3, insert new edge data
        batchHolder->put(std::move(key_), std::move(nVal));
        return encodeBatchValue(batchHolder->getBatch());
    }

    std::string indexKey(PartitionID partId,
                         RowReader* reader,
                         const cpp2::EdgeKey& edgeKey,
                         std::shared_ptr<nebula::meta::cpp2::IndexItem> index) {
        std::vector<Value::Type> colsType;
        auto values = IndexKeyUtils::collectIndexValues(reader, index->get_fields(), colsType);
        if (!values.ok()) {
            return "";
        }
        return IndexKeyUtils::edgeIndexKey(vIdLen_,
                                           partId,
                                           index->get_index_id(),
                                           edgeKey.get_src(),
                                           edgeKey.get_ranking(),
                                           edgeKey.get_dst(),
                                           values.value(),
                                           colsType);
    }

    FilterContext* getFilterCont() {
        return filter_;
    }

    bool getInsert() {
        return insert_;
    }

    UpdateExpressionContext* getExpressionContext() {
        return expCtx_;
    }


private:
    // ============================ input =====================================================
    StorageEnv                                                             *env_;
    GraphSpaceID                                                            spaceId_;
    size_t                                                                  vIdLen_;
    std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>>             indexes_;
    // update <prop name, new value expression>
    std::vector<storage::cpp2::UpdatedProp>                                 updatedProps_;
    EdgeFilterNode                                                         *filterNode_;

    EdgeType                                                                edgeType_;
    std::string                                                             key_;
    // use to save old row value
    std::string                                                             val_;
    RowWriterV2*                                                            rowWriter_;

    // ============================ output ====================================================
    // input and update, then output
    FilterContext                                                          *filter_;
    bool                                                                    insert_{false};
    UpdateExpressionContext                                                *expCtx_;
    std::atomic<kvstore::ResultCode>                                        exeResult_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_FILTERNODE_H_
