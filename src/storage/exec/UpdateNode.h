/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_UPDATENODE_H_
#define STORAGE_EXEC_UPDATENODE_H_

#include "base/Base.h"
#include "expression/Expression.h"
#include "context/ExpressionContext.h"
#include "storage/exec/TagNode.h"
#include "storage/exec/FilterNode.h"
#include "kvstore/LogEncoder.h"

namespace nebula {
namespace storage {

class UpdateNode : public RelNode {
public:
    UpdateNode(StorageEnv* env,
               GraphSpaceID spaceId,
               std::vector<storage::cpp2::UpdatedVertexProp>& updatedVertexProps,
               TagFilterNode* filterNode,
               ExpressionContext* expCtx)
        : env_(env)
        , spaceId_(spaceId)
        , updatedVertexProps_(updatedVertexProps)
        , filterNode_(filterNode) {
            expCtx_.reset(expCtx);
            filter_ = filterNode_->getFilterCont();
            insert_ = filterNode_->getInsert();
            tagUpdates_ = filterNode_->getUpdateKV();
        }

    folly::Future<kvstore::ResultCode> execute(PartitionID partId, const VertexID& vId) override {
        CHECK_NOTNULL(env_->kvstore_);
        auto atomic = [partId, vId, this] () -> folly::Optional<std::string> {
            return updateAndWriteBack(partId, vId);
        };

        auto callback = [this, partId, vId] (kvstore::ResultCode code) {
            return code;
        };

        env_->kvstore_->asyncAtomicOp(spaceId_, partId, atomic, callback);

        // TODO need to return
    }

    std::string updateAndWriteBack(const PartitionID partId, const VertexID vId) {
        UNUSED(partId);
        UNUSED(vId);
        Getters getters;
        getters.getSrcTagProp = [this] (const std::string& tagName,
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

        for (auto& updateProp : updatedVertexProps_) {
            auto tagId = updateProp.get_tag_id();
            auto propName = updateProp.get_name();
            auto exp = Expression::decode(updateProp.get_value());
            if (!exp.ok()) {
                return std::string("");
            }
            auto vexp = std::move(exp).value();
            vexp->setContext(this->expCtx_.get());
            auto value = vexp->eval(getters);
            if (!value.ok()) {
                return std::string("");
            }
            auto expValue = value.value();
            // update prop value
            filter_->fillTagProp(tagId, propName, expValue);

            // update RowWriterV2 old value -> new value
            auto wRet = tagUpdates_[tagId].second->setValue(propName, expValue);
            if (wRet != WriteResult::SUCCEEDED) {
                VLOG(1) << "Add field faild ";
                return std::string("");
            }
        }

        std::unique_ptr<kvstore::BatchHolder> batchHolder
            = std::make_unique<kvstore::BatchHolder>();
        for (const auto& u : tagUpdates_) {
            auto nKey = u.second.first;
            auto wRet = u.second.second->finish();
            if (wRet != WriteResult::SUCCEEDED) {
                VLOG(1) << "Add field faild ";
                return std::string("");
            }
            auto nVal = std::move(u.second.second->moveEncodedStr());

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

            batchHolder->put(std::move(nKey), std::move(nVal));
        }
        return encodeBatchValue(batchHolder->getBatch());
    }

     FilterContext* getFilterCont() {
         return filter_;
     }

     bool getInsert() {
         return insert_;
     }

private:
    // ================= input ========================================================
    StorageEnv                                                                     *env_;
    GraphSpaceID                                                                    spaceId_;
    // update <tagID, prop name, new value expression>
    std::vector<storage::cpp2::UpdatedVertexProp>                           updatedVertexProps_;
    TagFilterNode                                                                  *filterNode_;
    std::unique_ptr<ExpressionContext>                                              expCtx_;

    // ===============out==========================
    // input and update, then output
    FilterContext                                                                  *filter_;
    bool                                                                            insert_{false};
    std::unordered_map<TagID, std::pair<std::string, std::unique_ptr<RowWriterV2>>> tagUpdates_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_FILTERNODE_H_
