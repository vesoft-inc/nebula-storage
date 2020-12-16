
/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_DEDUPNODE_H_
#define STORAGE_EXEC_DEDUPNODE_H_

#include "common/base/Base.h"
#include "storage/exec/FilterNode.h"

namespace nebula {
namespace storage {

// DedupNode will used dedup the result set based on the given fields
template<typename T>
class DeDupNode : public IterateNode<T> {
public:
    using RelNode<T>::execute;

    explicit DeDupNode(nebula::DataSet* resultSet, DeDupDataSet* deDupResultSet)
        : resultSet_(resultSet)
        , deDupResultSet_(deDupResultSet) {}

    kvstore::ResultCode execute(PartitionID partId) override {
        auto ret = RelNode<T>::execute(partId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }
        std::transform(deDupResultSet_->rows.begin(),
                       deDupResultSet_->rows.end(),
                       std::back_inserter(resultSet_->rows),
                       [] (const auto& row) {
                           return row.second;
                       });
        deDupResultSet_->rows.clear();
        return kvstore::ResultCode::SUCCEEDED;
    }

private:
    nebula::DataSet* resultSet_;
    DeDupDataSet*    deDupResultSet_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_DEDUPNODE_H_
