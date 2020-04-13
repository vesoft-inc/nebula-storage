/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_MUTATE_UPDATEEDGEROCESSOR_H_
#define STORAGE_MUTATE_UPDATEEDGEROCESSOR_H_

#include "storage/query/QueryBaseProcessor.h"

namespace nebula {
namespace storage {

class UpdateEdgeProcessor
    : public QueryBaseProcessor<cpp2::UpdateEdgeRequest, cpp2::UpdateResponse> {
public:
    static UpdateEdgeProcessor* instance(StorageEnv* env,
                                         stats::Stats* stats) {
        return new UpdateEdgeProcessor(env, stats);
    }

    void process(const cpp2::UpdateEdgeRequest& req) override;

private:
    UpdateEdgeProcessor(StorageEnv* env, stats::Stats* stats)
        : QueryBaseProcessor<cpp2::UpdateEdgeRequest,
                             cpp2::UpdateResponse>(env, stats) {}

    void onProcessFinished() override;

    cpp2::ErrorCode checkAndBuildContexts(const cpp2::UpdateEdgeRequest& req) override;

    kvstore::ResultCode processTagProps(const PartitionID partId,
                                        const VertexID vId,
                                        const TagID tagId,
                                        const std::vector<PropContext>& props);

    kvstore::ResultCode collectTagPropIfValid(folly::StringPiece value,
                                              const PartitionID partId,
                                              const VertexID vId,
                                              const TagID tagId,
                                              const std::vector<PropContext>& props);

    kvstore::ResultCode collectTagProps(folly::StringPiece value,
                                        const TagID tagId,
                                        const std::vector<PropContext>& props);

    kvstore::ResultCode processEdgeProps(const PartitionID partId,
                                         const cpp2::EdgeKey& edgeKey);

    kvstore::ResultCode collectEdgePropIfValid(const PartitionID partId,
                                               const cpp2::EdgeKey& edgeKey);

    kvstore::ResultCode collectEdgeProps(const PartitionID partId,
                                         const cpp2::EdgeKey& edgeKey,
                                         RowReader* rowReader);

    kvstore::ResultCode insertEdgeProps(const PartitionID partId,
                                        const cpp2::EdgeKey& edgeKey,
                                        RowReader* rowReader);

    cpp2::ErrorCode checkFilter(const PartitionID partId, const cpp2::EdgeKey& edgeKey);

    std::string updateAndWriteBack(PartitionID partId, const cpp2::EdgeKey& edgeKey);

    cpp2::ErrorCode buildTagSchema();

    cpp2::ErrorCode buildEdgeSchema();

private:
    bool                                                            insertable_{false};
    std::vector<storage::cpp2::UpdatedEdgeProp>                     updatedEdgeProps_;

    // TODO return props expression
    std::vector<std::unique_ptr<Expression>>                        returnPropsExp_;

    // <tagID, prop_name>-> prop_value
    std::unordered_map<std::pair<TagID, std::string>, Value>        tagFilters_;

    // only update one edgekey, prop -> value
    std::unordered_map<std::string, Value>                          edgeFilters_;
    std::string                                                     key_;
    std::string                                                     val_;

    // std::unique_ptr<RowUpdater>                                     updater_;
    std::unique_ptr<RowWriterV2>                                    rowWriter_;

    std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>>     indexes_;
    std::atomic<cpp2::ErrorCode>                        filterResult_{cpp2::ErrorCode::SUCCEEDED};

    // TODO  add
    std::unique_ptr<ExpressionContext>                              expCtx_;
    // TODO Condition expression
    std::unique_ptr<Expression>                                     exp_;

    bool                                                            insert_{false};
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_MUTATE_UPDATEEDGEROCESSOR_H_
