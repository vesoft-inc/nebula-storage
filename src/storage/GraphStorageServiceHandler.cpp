/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/GraphStorageServiceHandler.h"
#include "storage/mutate/AddVerticesProcessor.h"
#include "storage/mutate/AddEdgesProcessor.h"
#include "storage/mutate/DeleteVerticesProcessor.h"
#include "storage/mutate/DeleteEdgesProcessor.h"
#include "storage/query/GetNeighborsProcessor.h"

#define RETURN_FUTURE(processor) \
    auto f = processor->getFuture(); \
    processor->process(req); \
    return f;

DEFINE_int32(vertex_cache_num, 16 * 1000 * 1000, "Total keys inside the cache");
DEFINE_int32(vertex_cache_bucket_exp, 4, "Total buckets number is 1 << cache_bucket_exp");
DEFINE_int32(reader_handlers, 32, "Total reader handlers");

namespace nebula {
namespace storage {

// Vertice section
folly::Future<cpp2::ExecResponse>
GraphStorageServiceHandler::future_addVertices(const cpp2::AddVerticesRequest& req) {
    auto* processor = AddVerticesProcessor::instance(env_, &addVerticesQpsStat_, &vertexCache_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResponse>
GraphStorageServiceHandler::future_deleteVertices(const cpp2::DeleteVerticesRequest& req) {
    auto* processor = DeleteVerticesProcessor::instance(env_, &delVerticesQpsStat_, &vertexCache_);
    RETURN_FUTURE(processor);
}

// Edge section
folly::Future<cpp2::ExecResponse>
GraphStorageServiceHandler::future_addEdges(const cpp2::AddEdgesRequest& req) {
    auto* processor = AddEdgesProcessor::instance(env_, &addEdgesQpsStat_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResponse>
GraphStorageServiceHandler::future_deleteEdges(const cpp2::DeleteEdgesRequest& req) {
    auto* processor = DeleteEdgesProcessor::instance(env_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::GetNeighborsResponse>
GraphStorageServiceHandler::future_getNeighbors(const cpp2::GetNeighborsRequest& req) {
    auto* processor = GetNeighborsProcessor::instance(env_,
                                                      nullptr,
                                                      &vertexCache_,
                                                      readerPool_.get());
    RETURN_FUTURE(processor);
}

}  // namespace storage
}  // namespace nebula
