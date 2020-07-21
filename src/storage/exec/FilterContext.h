/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_FILTERCONTEXT_H_
#define STORAGE_EXEC_FILTERCONTEXT_H_

#include "common/base/Base.h"

namespace nebula {
namespace storage {

class FilterContext {
public:
    void fillTagProp(TagID tagId, const std::string& prop, const nebula::Value& value) {
        tagFilters_[std::make_pair(tagId, prop)] = value;
    }

    void fillEdgeProp(EdgeType edgeType, const std::string& prop, const nebula::Value& value) {
        edgeFilters_[std::make_pair(edgeType, prop)] = value;
    }

    std::unordered_map<std::pair<TagID, std::string>, nebula::Value> getTagFilter() {
        return tagFilters_;
    }

    std::unordered_map<std::pair<EdgeType, std::string>, nebula::Value> getEdgeFilter() {
        return edgeFilters_;
    }

private:
    // key: <tagId, propName> -> propValue
    std::unordered_map<std::pair<TagID, std::string>, nebula::Value> tagFilters_;

    // key: <EdgeType, propName> -> propValue
    std::unordered_map<std::pair<EdgeType, std::string>, nebula::Value> edgeFilters_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_FILTERCONTEXT_H_
