/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_QUERYUTILS_H_
#define STORAGE_EXEC_QUERYUTILS_H_

namespace nebula {
namespace storage {

#include "common/base/Base.h"
#include "storage/CommonUtils.h"
#include "storage/query/QueryBaseProcessor.h"

class QueryUtils final {
public:
    static nebula::Value readEdgeProp(VertexIDSlice srcId,
                                      EdgeType edgeType,
                                      EdgeRanking edgeRank,
                                      VertexIDSlice dstId,
                                      RowReader* reader,
                                      const PropContext& prop) {
        nebula::Value value;
        switch (prop.propInKeyType_) {
            // prop in value
            case PropContext::PropInKeyType::NONE: {
                value = reader->getValueByName(prop.name_);
                break;
            }
            case PropContext::PropInKeyType::SRC: {
                value = srcId.str();
                break;
            }
            case PropContext::PropInKeyType::TYPE: {
                value = edgeType;
                break;
            }
            case PropContext::PropInKeyType::RANK: {
                value = edgeRank;
                break;
            }
            case PropContext::PropInKeyType::DST: {
                value = dstId.str();
                break;
            }
        }
        return value;
    }

    // return none if no valid ttl, else return the ttl property name and time
    static folly::Optional<std::pair<std::string, int64_t>>
    getEdgeTTLInfo(EdgeContext* edgeContext, EdgeType edgeType) {
        folly::Optional<std::pair<std::string, int64_t>> ret;
        auto edgeFound = edgeContext->ttlInfo_.find(std::abs(edgeType));
        if (edgeFound != edgeContext->ttlInfo_.end()) {
            ret.emplace(edgeFound->second.first, edgeFound->second.second);
        }
        return ret;
    }

    // return none if no valid ttl, else return the ttl property name and time
    static folly::Optional<std::pair<std::string, int64_t>>
    getTagTTLInfo(TagContext* tagContext, TagID tagId) {
        folly::Optional<std::pair<std::string, int64_t>> ret;
        auto tagFound = tagContext->ttlInfo_.find(tagId);
        if (tagFound != tagContext->ttlInfo_.end()) {
            ret.emplace(tagFound->second.first, tagFound->second.second);
        }
        return ret;
    }
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_EXEC_QUERYUTILS_H_
