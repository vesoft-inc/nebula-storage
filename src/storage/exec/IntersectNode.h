
/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_INTERSECTNODE_H_
#define STORAGE_EXEC_INTERSECTNODE_H_

#include "common/base/Base.h"

namespace nebula {
namespace storage {

template<typename T>
class IntersectNode : public IterateNode<T> {
public:
    using RelNode<T>::execute;

    explicit IntersectNode(nebula::DataSet* resultSet,
                           std::vector<nebula::DataSet>*  resultItems,
                           const std::vector<size_t>& pos)
        : resultSet_(resultSet)
        , resultItems_(resultItems)
        , pos_(pos) {}

    kvstore::ResultCode execute(PartitionID partId) override {
        auto ret = RelNode<T>::execute(partId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }
        intersect();
        return kvstore::ResultCode::SUCCEEDED;
    }

private:
    void sortRows() noexcept {
        for (auto it = resultItems_->begin(); it != resultItems_->end(); it++) {
            std::sort(it->rows.begin(), it->rows.end(), [this](auto& l, auto& r) {
                for (auto p : pos_) {
                    if (l.values[p] != r.values[p]) {
                        return l.values[p] < r.values[p];
                    }
                }
                return false;
            });
        }
    }

    void clearRows() noexcept {
        for (auto it = resultItems_->begin(); it != resultItems_->end(); it++) {
            it->rows.clear();
        }
    }

    std::vector<Row> setIntersection(std::vector<Row> row1, std::vector<Row> row2) {
        std::vector<Row> target;
        std::set_intersection(row1.begin(), row1.end(), row2.begin(), row2.end(), target.begin());
        return target;
    }

    void intersect() noexcept {
        // TODO (sky) : Performance test, whether sorting is required
        sortRows();
        std::vector<Row> target = (*resultItems_)[0].rows;
        for (size_t i = 1; i < resultItems_->size(); i++) {
            if (target.empty()) {
                return;
            }
            target = setIntersection(target, (*resultItems_)[i].rows);
        }
        if (!target.empty()) {
            resultSet_->rows.insert(resultSet_->rows.end(), target.begin(), target.end());
        }
        target.clear();
        clearRows();
    }

private:
    nebula::DataSet*                         resultSet_;
    std::vector<nebula::DataSet>*            resultItems_;
    std::vector<size_t>                      pos_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_INTERSECTNODE_H_
