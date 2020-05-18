/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_TAGNODE_H_
#define STORAGE_EXEC_TAGNODE_H_

#include "base/Base.h"
#include "storage/exec/RelNode.h"
#include "storage/exec/FilterContext.h"

namespace nebula {
namespace storage {

// TagNode will return a DataSet of specified props of tagId
// todo(doodle): return a iterator
class TagNode final : public RelNode {
public:
    TagNode(TagContext* ctx,
            StorageEnv* env,
            GraphSpaceID spaceId,
            size_t vIdLen,
            TagID tagId,
            const std::vector<PropContext>* props)
        : tagContext_(ctx)
        , env_(env)
        , spaceId_(spaceId)
        , vIdLen_(vIdLen)
        , tagId_(tagId)
        , props_(props) {
        auto schemaIter = tagContext_->schemas_.find(tagId_);
        CHECK(schemaIter != tagContext_->schemas_.end());
        CHECK(!schemaIter->second.empty());
        schemas_ = &(schemaIter->second);
    }

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

    const Value& result() {
        return result_;
    }

private:
    kvstore::ResultCode processTagProps(PartitionID partId, const VertexID& vId) {
        if (FLAGS_enable_vertex_cache && tagContext_->vertexCache_ != nullptr) {
            auto result = tagContext_->vertexCache_->get(std::make_pair(vId, tagId_), partId);
            if (result.ok()) {
                auto value = std::move(result).value();
                auto ret = collectTagPropIfValid(value);
                if (ret != kvstore::ResultCode::SUCCEEDED) {
                    VLOG(1) << "Evict from cache vId " << vId << ", tagId " << tagId_;
                    tagContext_->vertexCache_->evict(std::make_pair(vId, tagId_), partId);
                }
                return ret;
            } else {
                VLOG(1) << "Miss cache for vId " << vId << ", tagId " << tagId_;
            }
        }

        auto prefix = NebulaKeyUtils::vertexPrefix(vIdLen_, partId, vId, tagId_);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = env_->kvstore_->prefix(spaceId_, partId, prefix, &iter);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            VLOG(1) << "Error! ret = " << static_cast<int32_t>(ret) << ", spaceId " << spaceId_;
            return ret;
        }
        if (iter && iter->valid()) {
            // Will decode the properties according to the schema version
            // stored along with the properties
            ret = collectTagPropIfValid(iter->val());
            if (ret == kvstore::ResultCode::SUCCEEDED &&
                FLAGS_enable_vertex_cache &&
                tagContext_->vertexCache_ != nullptr) {
                tagContext_->vertexCache_->insert(std::make_pair(vId, tagId_),
                                                  iter->val().str(), partId);
                VLOG(1) << "Insert cache for vId " << vId << ", tagId " << tagId_;
            }
            return ret;
        } else {
            VLOG(1) << "Missed partId " << partId << ", vId " << vId << ", tagId " << tagId_;
            return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
        }
    }

    kvstore::ResultCode collectTagPropIfValid(folly::StringPiece value) {
        auto reader = RowReader::getRowReader(*schemas_, value);
        if (!reader) {
            VLOG(1) << "Can't get tag reader of " << tagId_;
            return kvstore::ResultCode::ERR_TAG_NOT_FOUND;
        }
        auto ttl = getTagTTLInfo();
        if (ttl.hasValue()) {
            auto ttlValue = ttl.value();
            if (CommonUtils::checkDataExpiredForTTL(schemas_->back().get(), reader.get(),
                                                    ttlValue.first, ttlValue.second)) {
                return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
            }
        }
        return collectTagProps(reader.get());
    }

    kvstore::ResultCode collectTagProps(RowReader* reader) {
        nebula::Row row;
        for (auto& prop : *props_) {
            VLOG(2) << "Collect prop " << prop.name_ << ", type " << tagId_;
            if (reader != nullptr) {
                auto status = readValue(reader, prop);
                if (!status.ok()) {
                    return kvstore::ResultCode::ERR_TAG_PROP_NOT_FOUND;
                }
                auto value = std::move(status.value());
                if (prop.tagFiltered_) {
                    filter_->fillTagProp(tagId_, prop.name_, value);
                }
                if (prop.returned_) {
                    row.columns.emplace_back(std::move(value));
                }
            }
        }
        nebula::DataSet dataSet;
        dataSet.rows.emplace_back(std::move(row));
        result_ = std::move(dataSet);
        return kvstore::ResultCode::SUCCEEDED;
    }

    folly::Optional<std::pair<std::string, int64_t>> getTagTTLInfo() {
        folly::Optional<std::pair<std::string, int64_t>> ret;
        auto tagFound = tagContext_->ttlInfo_.find(tagId_);
        if (tagFound != tagContext_->ttlInfo_.end()) {
            ret.emplace(tagFound->second.first, tagFound->second.second);
        }
        return ret;
    }

private:
    TagContext* tagContext_;
    StorageEnv* env_;
    GraphSpaceID spaceId_;
    size_t vIdLen_;
    TagID tagId_;
    const std::vector<PropContext>* props_;
    FilterContext* filter_;
    const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>* schemas_ = nullptr;

    Value result_ = NullType::__NULL__;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_TAGNODE_H_
