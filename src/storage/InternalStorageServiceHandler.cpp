/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/InternalStorageServiceHandler.h"
#include "storage/transaction/TransactionProcessor.h"

#define RETURN_FUTURE(processor) \
    auto f = processor->getFuture(); \
    processor->process(req); \
    return f;

namespace nebula {
namespace storage {

// Transaction section
folly::Future<cpp2::ExecResponse>
InternalStorageServiceHandler::future_processTransaction(const cpp2::TransactionReq& req) {
    auto* processor = TransactionProcessor::instance(env_, &addEdgesQpsStat_);
    RETURN_FUTURE(processor);
}

// folly::Future<cpp2::GetPropResponse>
// GraphStorageServiceHandler::future_getProps(const cpp2::GetPropRequest& req) {
//     LOG(INFO) << "messi GraphStorageServiceHandler::future_getProps()";
//     auto* processor = GetPropProcessor::instance(env_, &getPropQpsStat_, &vertexCache_);
//     RETURN_FUTURE(processor);
// }

}  // namespace storage
}  // namespace nebula
