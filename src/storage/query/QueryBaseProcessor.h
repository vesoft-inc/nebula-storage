/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_QUERY_QUERYBASEPROCESSOR_H_
#define STORAGE_QUERY_QUERYBASEPROCESSOR_H_

#include "base/Base.h"
#include "common/NebulaKeyUtils.h"
#include "meta/NebulaSchemaProvider.h"
#include "storage/BaseProcessor.h"
#include "storage/CommonUtils.h"
#include "stats/Stats.h"

namespace nebula {
namespace storage {

struct ReturnProp {
    int32_t entryId_;                       // tagId or edgeType
    std::vector<std::string> names_;        // property names
};

// The PropContext stores the info about property to be returned or filtered
class PropContext {
public:
    enum class PropInKeyType {
        NONE = 0x00,
        SRC = 0x01,
        DST = 0x02,
        TYPE = 0x03,
        RANK = 0x04,
    };

    explicit PropContext(const char* name)
        : name_(name) {}

    // prop name
    std::string name_;
    // field info, e.g. nullable, default value
    const meta::SchemaProviderIf::Field* field_;
    bool tagFiltered_ = false;
    bool returned_ = false;
    // prop type in edge key, for srcId/dstId/type/rank
    PropInKeyType propInKeyType_ = PropInKeyType::NONE;

    // for edge prop stat, such as count/avg/sum
    bool hasStat_ = false;
    // stat prop index from request
    size_t statIndex_;
    cpp2::StatType statType_;
};

// used to save stat value of each vertex
struct PropStat {
    PropStat() = default;

    explicit PropStat(const cpp2::StatType& statType) : statType_(statType) {}

    cpp2::StatType statType_;
    mutable Value sum_ = 0L;
    mutable int32_t count_ = 0;
};

const std::vector<std::pair<std::string, PropContext::PropInKeyType>> kPropsInKey_ = {
    {"_src", PropContext::PropInKeyType::SRC},
    {"_type", PropContext::PropInKeyType::TYPE},
    {"_rank", PropContext::PropInKeyType::RANK},
    {"_dst", PropContext::PropInKeyType::DST},
};

struct FilterContext {
    // key: <tagId, propName> -> propValue
    std::unordered_map<std::pair<TagID, std::string>, nebula::Value> tagFilters_;
};

template<typename REQ, typename RESP>
class QueryBaseProcessor : public BaseProcessor<RESP> {
public:
    virtual ~QueryBaseProcessor() = default;

    virtual void process(const REQ& req) = 0;

protected:
    explicit QueryBaseProcessor(StorageEnv* env,
                                stats::Stats* stats = nullptr,
                                VertexCache* cache = nullptr)
        : BaseProcessor<RESP>(env, stats)
        , vertexCache_(cache) {}

    virtual cpp2::ErrorCode checkAndBuildContexts(const REQ& req) = 0;
    virtual void onProcessFinished() = 0;

    // build tagContexts_ according to return props
    cpp2::ErrorCode handleVertexProps(const std::vector<ReturnProp>& vertexProps);
    // build edgeContexts_ according to return props
    cpp2::ErrorCode handleEdgeProps(const std::vector<ReturnProp>& edgeProps);

    // todo(doodle)
    cpp2::ErrorCode buildFilter(const REQ& req);

    // build ttl info map
    void buildTagTTLInfo();
    void buildEdgeTTLInfo();

    kvstore::ResultCode processTagProps(PartitionID partId,
                                        const VertexID& vId,
                                        TagID tagId,
                                        const std::vector<PropContext>& returnProps,
                                        FilterContext& fcontext,
                                        nebula::DataSet& dataSet);

    kvstore::ResultCode collectTagPropIfValid(const meta::NebulaSchemaProvider* schema,
                                              folly::StringPiece value,
                                              TagID tagId,
                                              const std::vector<PropContext>& returnProps,
                                              FilterContext& fcontext,
                                              nebula::DataSet& dataSet);

    // collectTagProps and collectEdgeProps is use to collect prop in props,
    // and put them into dataSet
    kvstore::ResultCode collectTagProps(TagID tagId,
                                        RowReader* reader,
                                        const std::vector<PropContext>& props,
                                        FilterContext& fcontext,
                                        nebula::DataSet& dataSet);

    kvstore::ResultCode collectEdgeProps(EdgeType edgeType,
                                         RowReader* reader,
                                         folly::StringPiece key,
                                         const std::vector<PropContext>& props,
                                         nebula::DataSet& dataSet,
                                         std::vector<PropStat>* stats);

    StatusOr<nebula::Value> readValue(RowReader* reader, const PropContext& ctx);

    void addStatValue(const Value& value, PropStat& stat);

    folly::Optional<std::pair<std::string, int64_t>> getTagTTLInfo(TagID tagId);
    folly::Optional<std::pair<std::string, int64_t>> getEdgeTTLInfo(EdgeType edgeType);

    cpp2::ErrorCode getSpaceVidLen(GraphSpaceID spaceId);

    // only used in getVertexProps, get all prop of all tagId
    std::vector<ReturnProp> buildAllTagProps(GraphSpaceID spaceId);

    // only used in getEdgeProps, get all prop of all edgeType
    std::vector<ReturnProp> buildAllEdgeProps(GraphSpaceID spaceId,
                                              const cpp2::EdgeDirection& direction);

protected:
    GraphSpaceID spaceId_;
    size_t vIdLen_;
    VertexCache* vertexCache_ = nullptr;

    std::vector<std::pair<TagID, std::vector<PropContext>>> tagContexts_;
    std::vector<std::pair<EdgeType, std::vector<PropContext>>> edgeContexts_;

    // the value of index map is the index in tagContexts/edgeContexts
    std::unordered_map<TagID, size_t> tagIndexMap_;
    std::unordered_map<EdgeType, size_t> edgeIndexMap_;

    std::unordered_map<TagID,
                       std::shared_ptr<const nebula::meta::NebulaSchemaProvider>> tagSchemas_;
    std::unordered_map<EdgeType,
                       std::shared_ptr<const nebula::meta::NebulaSchemaProvider>> edgeSchemas_;

    std::unordered_map<TagID, std::pair<std::string, int64_t>> tagTTLInfo_;
    std::unordered_map<EdgeType, std::pair<std::string, int64_t>> edgeTTLInfo_;

    nebula::DataSet resultDataSet_;
};


}  // namespace storage
}  // namespace nebula

#include "storage/query/QueryBaseProcessor.inl"

#endif  // STORAGE_QUERY_QUERYBASEPROCESSOR_H_
