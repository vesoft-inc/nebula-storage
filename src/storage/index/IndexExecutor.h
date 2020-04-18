/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_INDEXEXECUTOR_H
#define STORAGE_INDEXEXECUTOR_H

#include "stats/Stats.h"
#include "storage/BaseProcessor.h"
#include "storage/index/ExecutionPlan.h"
#include "storage/CommonUtils.h"
#include "common/NebulaKeyUtils.h"

namespace nebula {
namespace storage {

template<typename RESP>
class IndexExecutor : public BaseProcessor<RESP> {
public:
    virtual ~IndexExecutor() = default;

protected:
    explicit IndexExecutor(StorageEnv* env,
                           stats::Stats* stats,
                           VertexCache* cache)
        : BaseProcessor<RESP>(env, stats)
        , vertexCache_(cache) {}

    void putResultCodes(cpp2::ErrorCode code, const std::vector<PartitionID>& parts) {
        for (auto& p : parts) {
            this->pushResultCode(code, p);
        }
        this->onFinished();
    }

    cpp2::ErrorCode prepareRequest(const cpp2::LookupIndexRequest &req);

    cpp2::ErrorCode buildExecutionPlan(int32_t hintId, const cpp2::IndexQueryContext& queryContext);

    cpp2::ErrorCode setupIndexColumns(std::map<std::string, Value::Type> &cols,
                                      std::shared_ptr<IndexItem>  index);

    Value::Type toValueType(nebula::meta::cpp2::PropertyType type) noexcept;

    /**
     * Details Scan index or data part as one by one.
     **/
    kvstore::ResultCode executeExecutionPlan(PartitionID part);

private:
    kvstore::ResultCode executeIndexScan(int32_t hintId, PartitionID part);

    kvstore::ResultCode executeIndexRangeScan(int32_t hintId, PartitionID part);

    kvstore::ResultCode executeIndexPrefixScan(int32_t hintId, PartitionID part);

    kvstore::ResultCode getDataRow(PartitionID partId,
                                   const folly::StringPiece& key);

    kvstore::ResultCode getVertexRow(PartitionID partId,
                                     const folly::StringPiece& key,
                                     cpp2::VertexIndexData* data);

    kvstore::ResultCode getEdgeRow(PartitionID partId,
                                   const folly::StringPiece& key,
                                   cpp2::EdgeIndexData* data);

    bool conditionsCheck(int32_t hintId, const folly::StringPiece& key);

    StatusOr<Value> decodeValue(int32_t hintId,
                                const folly::StringPiece& key,
                                const folly::StringPiece& prop);

    folly::StringPiece getIndexVal(int32_t hintId,
                                   const folly::StringPiece& key,
                                   const folly::StringPiece& prop);

protected:
    GraphSpaceID                           spaceId_;
    VertexCache*                           vertexCache_{nullptr};
    size_t                                 vIdLen_{0};
    std::vector<cpp2::VertexIndexData>     vertexRows_;
    std::vector<cpp2::EdgeIndexData>       edgeRows_;
    bool                                   isEdgeIndex_{false};

private:
    int                                    rowNum_{0};
    int32_t                                tagOrEdgeId_{-1};
    int32_t                                vColNum_{0};
    std::vector<std::string>               returnColumns_;
    std::map<int32_t, std::unique_ptr<ExecutionPlan>>       executionPlans_;
};

}  // namespace storage
}  // namespace nebula
#include "storage/index/IndexExecutor.inl"

#endif  // STORAGE_INDEXEXECUTOR_H
