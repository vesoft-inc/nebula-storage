/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_GRAPHSTORAGESERVICEHANDLER_H_
#define STORAGE_GRAPHSTORAGESERVICEHANDLER_H_

#include "base/Base.h"
#include "interface/gen-cpp2/GraphStorageService.h"
#include "stats/Stats.h"

namespace nebula {
namespace storage {

class StorageEnv;

class GraphStorageServiceHandler final : public cpp2::GraphStorageServiceSvIf {
public:
    GraphStorageServiceHandler(StorageEnv* env) : env_(env) {
    }

    folly::Future<cpp2::ExecResponse>
    future_addVertices(const cpp2::AddVerticesRequest& req) override;

//    folly::Future<cpp2::ExecResponse>
//    future_addEdges(const cpp2::AddEdgesRequest& req) override;

private:
    StorageEnv*             env_{nullptr};
    // VertexCache             vertexCache_;
    stats::Stats            addVertexQpsStat_;
    stats::Stats            addEdgeQpsStat_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_GRAPHSTORAGESERVICEHANDLER_H_
