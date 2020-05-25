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
                  std::vector<storage::cpp2::UpdatedVertexProp>& updatedVertexProps,
                  UpdateFilterNode* filterNode)
        : env_(env)
        , spaceId_(spaceId)
        , updatedVertexProps_(updatedVertexProps)
        , filterNode_(filterNode) {
        }

    kvstore::ResultCode execute(PartitionID partId, const VertexID& vId) override {
        CHECK_NOTNULL(env_->kvstore_);

        folly::Baton<true, std::atomic> baton;
        auto ret = kvstore::ResultCode::SUCCEEDED;

        env_->kvstore_->asyncAtomicOp(spaceId_, partId,
            [partId, vId, this] ()
            -> folly::Optional<std::string> {
                auto exeRet = RelNode::execute(partId, vId);
                if (exeRet == kvstore::ResultCode::SUCCEEDED) {
                    this->tagId_ = filterNode_->getTagId();
                    this->key_ = filterNode_->getKey();
                    this->rowWriter_ = filterNode_->getRowWriter();
                    this->filter_ = filterNode_->getFilterCont();
                    this->insert_ = filterNode_->getInsert();
                    this->expCtx_ = filterNode_->getExpressionContext();
                    return this->updateAndWriteBack(partId, vId);
                } else {
                    return folly::none;
                }
            },
            [this, partId, vId, &ret, &baton] (kvstore::ResultCode code) {
                ret = code;
                baton.post();
            });
        baton.wait();

        return ret;
    }

    std::string updateAndWriteBack(const PartitionID partId, const VertexID vId) {
        for (auto& updateProp : this->updatedVertexProps_) {
            auto tagId = updateProp.get_tag_id();
            auto propName = updateProp.get_name();
            auto updateExp = Expression::decode(updateProp.get_value());
            if (!updateExp) {
                return std::string("");
            }
            auto updateVal = updateExp->eval(*(this->expCtx_));
            // update prop value to filter_
            this->filter_->fillTagProp(tagId, propName, updateVal);

            // update expression context
            auto tagName = this->env_->schemaMan_->toTagName(this->spaceId_, tagId);
            if (!tagName.ok()) {
                VLOG(1) << "Can't find spaceId " << this->spaceId_ << " tagId " << tagId;
                return std::string("");
            }
            this->expCtx_->setSrcProp(tagName.value(), propName, updateVal);


            // update RowWriterV2 old value -> new value
            if (tagId_ != tagId) {
                VLOG(1) << "Update field faild ";
                return std::string("");
            }
            auto wRet = rowWriter_->setValue(propName, updateVal);
            if (wRet != WriteResult::SUCCEEDED) {
                VLOG(1) << "Add field faild ";
                return std::string("");
            }
        }

        std::unique_ptr<kvstore::BatchHolder> batchHolder
            = std::make_unique<kvstore::BatchHolder>();

        auto wRet = rowWriter_->finish();
        if (wRet != WriteResult::SUCCEEDED) {
            VLOG(1) << "Add field faild ";
            return std::string("");
        }

        auto nVal = std::move(rowWriter_->moveEncodedStr());

        UNUSED(partId);
        UNUSED(vId);
            /*
            if (!indexes_.empty()) {
                std::unique_ptr<RowReader> reader, oReader;
                for (auto &index : indexes_) {
                    if (index->get_schema_id().get_tag_id() == u.first) {
                        if (!(u.second->kv.second.empty())) {
                            if (oReader == nullptr) {
                                oReader = RowReader::getTagPropReader(this->schemaMan_,
                                                                      u.second->kv.second,
                                                                      spaceId_,
                                                                      u.first);
                            }
                            const auto &oCols = index->get_fields();
                            auto oValues = collectIndexValues(oReader.get(), oCols);
                            auto oIndexKey = NebulaKeyUtils::vertexIndexKey(partId,
                                                                            index->index_id,
                                                                            vId,
                                                                            oValues);
                            batchHolder->remove(std::move(oIndexKey));
                        }
                        if (reader == nullptr) {
                            reader = RowReader::getTagPropReader(this->schemaMan_,
                                                                 nVal,
                                                                 spaceId_,
                                                                 u.first);
                        }
                        const auto &cols = index->get_fields();
                        auto values = collectIndexValues(reader.get(), cols);
                        auto indexKey = NebulaKeyUtils::vertexIndexKey(partId,
                                                                       index->get_index_id(),
                                                                       vId,
                                                                       values);
                        batchHolder->put(std::move(indexKey), "");
                    }
                }
            }
            */

        batchHolder->put(std::move(key_), std::move(nVal));
        return encodeBatchValue(batchHolder->getBatch());
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
    StorageEnv                                                                     *env_;
    GraphSpaceID                                                                    spaceId_;
    // update <tagID, prop name, new value expression>
    std::vector<storage::cpp2::UpdatedVertexProp>                           updatedVertexProps_;
    UpdateFilterNode                                                               *filterNode_;

    // To update tagid，key, old row value
    // std::unordered_map<std::string, std::unique_ptr<RowWriterV2>>                tagUpdateKVs_;
    TagID                                                                           tagId_;
    std::string                                                                     key_;

    RowWriterV2*                                                                    rowWriter_;
    // ============================ output ====================================================
    // input and update, then output
    FilterContext                                                                  *filter_;
    bool                                                                            insert_{false};
    UpdateExpressionContext                                                        *expCtx_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_FILTERNODE_H_
