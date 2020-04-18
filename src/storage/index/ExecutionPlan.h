/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef NEBULA_STORAGE_EXECUTIONPLAN_H
#define NEBULA_STORAGE_EXECUTIONPLAN_H

#include <utility>

#include "base/Base.h"
#include "storage/BaseProcessor.h"
#include "common/IndexKeyUtils.h"
#include "interface/gen-cpp2/meta_types.h"

namespace nebula {
namespace storage {

using IndexItem = nebula::meta::cpp2::IndexItem;

class ExecutionPlan final {

public:
    ExecutionPlan() {};

    ExecutionPlan(std::shared_ptr<IndexItem>  index,
                  std::map<std::string, Value::Type>  indexCols,
                  std::vector<cpp2::IndexColumnHint>  columnHints)
        : index_(std::move(index)),
          indexCols_(std::move(indexCols)),
          columnHints_(std::move(columnHints)) {};

    ~ExecutionPlan() = default;

public:

    const std::string& getFilter() const;

    void setFilter(const folly::StringPiece& filter);

    const std::shared_ptr<IndexItem>& getIndex() const;

    void setIndex(const std::shared_ptr<IndexItem>& index);

    const std::map<std::string, Value::Type>& getIndexCols() const;

    Value::Type getIndexColType(const folly::StringPiece& prop) const;

    void setIndexCols(const std::map<std::string, Value::Type>& indexCols);

    const std::vector<cpp2::IndexColumnHint>& getColumnHints() const;

    void setColumnHints(const std::vector<cpp2::IndexColumnHint>& columnHints);

    IndexID getIndexId() const;

    bool isRangeScan() const;

    StatusOr<std::string> getPrefixStr(PartitionID partId) const;

    StatusOr<std::pair<std::string, std::string>> getRangeStartStr(PartitionID partId) const;

private:
    std::shared_ptr<IndexItem>                            index_{nullptr};
    std::map<std::string, Value::Type>                    indexCols_;
    std::vector<cpp2::IndexColumnHint>                    columnHints_;
    std::string                                           filter_;
};

}  // namespace storage
}  // namespace nebula

#endif  // NEBULA_STORAGE_EXECUTIONPLAN_H
