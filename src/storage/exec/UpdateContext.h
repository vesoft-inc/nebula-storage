/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_UPDATECONTEXT_H_
#define STORAGE_EXEC_UPDATECONTEXT_H_

#include "common/base/Base.h"

namespace nebula {
namespace storage {

class UpdateContext {
public:
    void fillTagProp(TagID tagId, const std::string& prop, const nebula::Value& value) {
        tagUpdates_[std::make_pair(tagId, prop)] = value;
    }

    void fillEdgeProp(EdgeType edgeType, const std::string& prop, const nebula::Value& value) {
        edgeUpdates_[std::make_pair(edgeType, prop)] = value;
    }

    std::unordered_map<std::pair<TagID, std::string>, nebula::Value> getTagUpdateCon() {
        return tagUpdates_;
    }

    std::unordered_map<std::pair<EdgeType, std::string>, nebula::Value> getEdgeUpdateCon() {
        return edgeUpdates_;
    }

private:
    // key: <tagId, propName> -> propValue
    std::unordered_map<std::pair<TagID, std::string>, nebula::Value> tagUpdates_;

    // key: <EdgeType, propName> -> propValue
    std::unordered_map<std::pair<EdgeType, std::string>, nebula::Value> edgeUpdates_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_UPDAtECONTEXT_H_
