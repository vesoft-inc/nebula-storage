/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/GraphStorageServiceHandler.h"
#include "storage/mutate/AddVerticesProcessor.h"
// #include "storage/mutate/AddEdgesProcessor.h"

#define RETURN_FUTURE(processor) \
    auto f = processor->getFuture(); \
    processor->process(req); \
    return f;

namespace nebula {
namespace storage {

folly::Future<cpp2::ExecResponse>
GraphStorageServiceHandler::future_addVertices(const cpp2::AddVerticesRequest& req) {
    // auto* processor = AddVerticesProcessor::instance(env_, &addVertexQpsStat_, &vertexCache_);
    auto* processor = AddVerticesProcessor::instance(env_, &addVertexQpsStat_);
    RETURN_FUTURE(processor);
}

// folly::Future<cpp2::ExecResponse>
// GraphStorageServiceHandler::future_addEdges(const cpp2::AddEdgesRequest& req) {
//     auto* processor = AddEdgesProcessor::instance(env_, &addEdgeQpsStat_);
//     RETURN_FUTURE(processor);
//}

}  // namespace storage
}  // namespace nebula
