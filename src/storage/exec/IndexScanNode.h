/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_INDEXSCANNODE_H_
#define STORAGE_EXEC_INDEXSCANNODE_H_

#include "common/base/Base.h"
#include "storage/exec/RelNode.h"
#include "storage/exec/StorageIterator.h"

namespace nebula {
namespace storage {
template<typename T>
class IndexScanNode : public RelNode<T> {
public:
    using RelNode<T>::execute;

    IndexScanNode(PlanContext* planCtx,
                  IndexID indexId,
                  std::vector<cpp2::IndexColumnHint> columnHints)
    : planContext_(planCtx)
    , indexId_(indexId)
    , columnHints_(std::move(columnHints)) {
        /**
         * columnHints's elements are {scanType = PREFIX|RANGE; beginStr; endStr},
         *                            {scanType = PREFIX|RANGE; beginStr; endStr},...
         * if the scanType is RANGE, means the index scan is range scan.
         * if all scanType are PREFIX, means the index scan is prefix scan.
         * there should be only one RANGE hnit, and it must be the last one.
         */
        for (size_t i = 0; i < columnHints_.size(); i++) {
            if (columnHints_[i].get_scan_type() == cpp2::ScanType::RANGE) {
                isRangeScan_ = true;
                CHECK_EQ(columnHints_.size() - 1, i);
                break;
            }
        }
    }

    kvstore::ResultCode execute(PartitionID partId) override {
        auto ret = RelNode<T>::execute(partId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }
        auto scanRet = scanStr(partId);
        if (!scanRet.ok()) {
            return kvstore::ResultCode::ERR_INVALID_FIELD_VALUE;
        }
        scanPair_ = scanRet.value();
        std::unique_ptr<kvstore::KVIterator> iter;
        ret = isRangeScan_
              ? planContext_->env_->kvstore_->range(planContext_->spaceId_, partId,
                  scanPair_.first, scanPair_.second, &iter)
              : planContext_->env_->kvstore_->prefix(planContext_->spaceId_, partId,
                  scanPair_.first, &iter);
        if (ret == kvstore::ResultCode::SUCCEEDED && iter && iter->valid()) {
            planContext_->isEdge_
            ? iter_.reset(new EdgeIndexIterator(std::move(iter), planContext_->vIdLen_))
            : iter_.reset(new VertexIndexIterator(std::move(iter), planContext_->vIdLen_));
        } else {
            iter_.reset();
            return ret;
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

    IndexIterator* iterator() {
        return iter_.get();
    }

    std::vector<kvstore::KV> moveData() {
        data_.clear();
        while (!!iter_ && iter_->valid()) {
            data_.emplace_back(iter_->key(), "");
            iter_->next();
        }
        return std::move(data_);
    }

private:
    StatusOr<std::pair<std::string, std::string>> scanStr(PartitionID partId) {
        auto iRet = planContext_->isEdge_
                    ? planContext_->env_->indexMan_->getEdgeIndex(planContext_->spaceId_, indexId_)
                    : planContext_->env_->indexMan_->getTagIndex(planContext_->spaceId_, indexId_);
        if (!iRet.ok()) {
            return Status::IndexNotFound();
        }
        if (isRangeScan_) {
            return getRangeStr(partId, iRet.value()->get_fields());
        } else {
            return getPrefixStr(partId, iRet.value()->get_fields());
        }
    }

    StatusOr<std::pair<std::string, std::string>> getPrefixStr(
        PartitionID partId, const std::vector< ::nebula::meta::cpp2::ColumnDef>& fields) {
        std::string prefix;
        prefix.append(IndexKeyUtils::indexPrefix(partId, indexId_));
        for (auto& col : columnHints_) {
            auto iter = std::find_if(fields.begin(), fields.end(), [col](const auto& field) {
                return col.get_column_name() == field.get_name();
            });
            if (iter == fields.end()) {
                VLOG(3) << "Field " << col.get_column_name() << " not found ";
                return Status::Error("Field not found");
            }
            auto type = IndexKeyUtils::toValueType(iter->type.type);
            if (type == Value::Type::STRING && !iter->type.type_length_ref().has_value()) {
                return Status::Error("String property index has not set prefix length.");
            }
            prefix.append(encodeValue(*col.begin_value_ref(), type, iter->type.get_type_length()));
        }
        return std::make_pair(prefix, "");
    }

    StatusOr<std::pair<std::string, std::string>> getRangeStr(
        PartitionID partId, const std::vector< ::nebula::meta::cpp2::ColumnDef>& fields) {
        std::string start, end;
        start.append(IndexKeyUtils::indexPrefix(partId, indexId_));
        end.append(IndexKeyUtils::indexPrefix(partId, indexId_));
        for (auto& col : columnHints_) {
            auto iter = std::find_if(fields.begin(), fields.end(), [col](const auto& field) {
                return col.get_column_name() == field.get_name();
            });
            if (iter == fields.end()) {
                VLOG(3) << "Field " << col.get_column_name() << " not found ";
                return Status::Error("Field not found");
            }
            auto type = IndexKeyUtils::toValueType(iter->get_type().get_type());
            if (type == Value::Type::STRING && !iter->get_type().type_length_ref().has_value()) {
                return Status::Error("String property index has not set prefix length.");
            }
            if (col.get_scan_type() == cpp2::ScanType::PREFIX) {
                start.append(encodeValue(*col.begin_value_ref(),
                            type, iter->type.get_type_length()));
                end.append(encodeValue(*col.begin_value_ref(),
                            type, iter->type.get_type_length()));
            } else {
                start.append(encodeValue(*col.begin_value_ref(),
                            type, iter->type.get_type_length()));
                end.append(encodeValue(*col.end_value_ref(),
                            type, iter->type.get_type_length()));
            }
        }
        return std::make_pair(start, end);
    }

    // precondition: if type is STRING, strLen must be valid
    std::string encodeValue(const Value& val, Value::Type type, const int16_t* strLen) {
        if (val.isNull()) {
            return IndexKeyUtils::encodeNullValue(type, strLen);
        }
        if (type == Value::Type::STRING) {
            return IndexKeyUtils::encodeValue(val, *strLen);
        } else {
            return IndexKeyUtils::encodeValue(val);
        }
    }

private:
    PlanContext*                        planContext_;
    IndexID                             indexId_;
    bool                                isRangeScan_{false};
    std::unique_ptr<IndexIterator>      iter_;
    std::pair<std::string, std::string> scanPair_;
    std::vector<cpp2::IndexColumnHint>  columnHints_;
    std::vector<kvstore::KV>            data_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_INDEXSCANNODE_H_
