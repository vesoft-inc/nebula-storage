/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/index/ExecutionPlan.h"

namespace nebula {
namespace storage {

const std::string& ExecutionPlan::getFilter() const {
    return filter_;
}

void ExecutionPlan::setFilter(const folly::StringPiece& filter) {
    filter_ = filter.data();
}

const std::shared_ptr<IndexItem>& ExecutionPlan::getIndex() const {
    return index_;
}

void ExecutionPlan::setIndex(const std::shared_ptr<IndexItem>& index) {
    index_ = index;
}

const std::map<std::string, Value::Type>& 
ExecutionPlan::getIndexCols() const {
    return indexCols_;
}

Value::Type ExecutionPlan::getIndexColType(const folly::StringPiece& prop) const {
    auto it = indexCols_.find(prop.str());
    if (it != indexCols_.end()) {
        return it->second;
    }
    return Value::Type::__EMPTY__;
}

void ExecutionPlan::setIndexCols(
    const std::map<std::string, Value::Type>& indexCols) {
    indexCols_ = indexCols;
}

const std::vector<cpp2::IndexColumnHint>& ExecutionPlan::getColumnHints() const {
    return columnHints_;
}

void ExecutionPlan::setColumnHints(const std::vector<cpp2::IndexColumnHint>& columnHints) {
    columnHints_ = columnHints;
}

IndexID ExecutionPlan::getIndexId() const {
    return index_->get_index_id();
}

bool ExecutionPlan::isRangeScan() const {
    if (!columnHints_.empty()) {
        return columnHints_[0].get_scan_type() == cpp2::ScanType::RANGE;
    }
    return false;
}

StatusOr<std::string> ExecutionPlan::getPrefixStr(PartitionID partId) const {
    std::string prefix = IndexKeyUtils::indexPrefix(partId, index_->get_index_id());
    for (auto& col : columnHints_) {
        if (col.__isset.scan_type &&
            col.get_scan_type() == cpp2::ScanType::PREFIX) {
            prefix.append(IndexKeyUtils::encodeValue(col.get_begin_value()));
        } else {
            return Status::Error("invalid scan type , colums : %s", col.get_column_name().c_str());
        }
    }
    return prefix;
}

StatusOr<std::pair<std::string, std::string>>
ExecutionPlan::getRangeStartStr(PartitionID partId) const {
    std::string start , end;
    start = end = IndexKeyUtils::indexPrefix(partId, index_->get_index_id());
    for (auto& col : columnHints_) {
        if (!col.__isset.scan_type) {
            return Status::Error("missing scan type by column : %s", col.get_column_name().c_str());
        }
        if (col.get_scan_type() == cpp2::ScanType::PREFIX) {
            start.append(IndexKeyUtils::encodeValue(col.get_begin_value()));
            end.append(IndexKeyUtils::encodeValue(col.get_begin_value()));
        } else {
            start.append(IndexKeyUtils::encodeValue(col.get_begin_value()));
            end.append(IndexKeyUtils::encodeValue(col.get_end_value()));
        }
    }
    return std::make_pair<std::string, std::string>(std::move(start), std::move(end));
}

}  // namespace storage
}  // namespace nebula
