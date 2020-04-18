/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/GraphStorageServiceHandler.h"
#include "storage/index/LookupIndexProcessor.h"

#define RETURN_FUTURE(processor) \
    auto f = processor->getFuture(); \
    processor->process(req); \
    return f;

namespace nebula {
namespace storage {
folly::Future<cpp2::LookupIndexResp>
GraphStorageServiceHandler::future_lookupIndex(const cpp2::LookupIndexRequest& req) {
    auto* processor = LookupIndexProcessor::instance(env_, &lookupQpsStat_, nullptr);
    RETURN_FUTURE(processor);
}
}  // namespace storage
}  // namespace nebula
