/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_MUTATE_ADDEDGESPROCESSOR_H_
#define STORAGE_MUTATE_ADDEDGESPROCESSOR_H_

#include "common/base/Base.h"
#include "storage/BaseProcessor.h"
#include "storage/StorageFlags.h"
#include "kvstore/LogEncoder.h"

namespace nebula {
namespace storage {

class AddEdgesProcessor : public BaseProcessor<cpp2::ExecResponse> {
public:
    static AddEdgesProcessor* instance(StorageEnv* env,
                                       stats::Stats* stats) {
        return new AddEdgesProcessor(env, stats);
    }

    void process(const cpp2::AddEdgesRequest& req);

private:
    AddEdgesProcessor(StorageEnv* env, stats::Stats* stats)
        : BaseProcessor<cpp2::ExecResponse>(env, stats) {}

    folly::Optional<std::string> addEdges(PartitionID partId,
                                          const std::vector<kvstore::KV>& edges);

    folly::Optional<std::string> findOldValue(PartitionID partId,
                                              VertexID srcId,
                                              EdgeType eType,
                                              EdgeRanking rank,
                                              VertexID dstId);

    std::string normalIndexKey(PartitionID partId,
                               RowReader* reader,
                               VertexID srcId,
                               EdgeRanking rank,
                               VertexID dstId,
                               std::shared_ptr<nebula::meta::cpp2::IndexItem> index);

private:
    GraphSpaceID                                                spaceId_;

    // In a part, when processing the normal index, whether the record has been read
    // In one part, same <VertexID, EdgeType, EdgeRanking, VertexID> has
    // only one data in addEdges
    std::unordered_map<std::tuple<VertexID, EdgeType, EdgeRanking, VertexID>, bool>  keyExist_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_MUTATE_ADDEDGESPROCESSOR_H_
