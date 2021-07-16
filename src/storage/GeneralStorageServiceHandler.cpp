/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/kv/PutProcessor.h"
#include "storage/kv/GetProcessor.h"
#include "storage/kv/RemoveProcessor.h"
#include "storage/GeneralStorageServiceHandler.h"

#define RETURN_FUTURE(processor) \
    auto f = processor->getFuture(); \
    processor->process(req); \
    return f;

namespace nebula {
namespace storage {

ProcessorCounters kKVCounters;

GeneralStorageServiceHandler::GeneralStorageServiceHandler(StorageEnv* env)
        : env_(env) {
    kKVCounters.init("kv_query", nullptr);
    kPutCounters.init("put", &kKVCounters);
    kGetCounters.init("get", &kKVCounters);
    kRemoveCounters.init("remove", &kKVCounters);
}


folly::Future<cpp2::ExecResponse>
GeneralStorageServiceHandler::future_put(const cpp2::KVPutRequest& req) {
    auto* processor = PutProcessor::instance(env_);
    RETURN_FUTURE(processor);
}


folly::Future<cpp2::KVGetResponse>
GeneralStorageServiceHandler::future_get(const cpp2::KVGetRequest& req) {
    auto* processor = GetProcessor::instance(env_);
    RETURN_FUTURE(processor);
}


folly::Future<cpp2::ExecResponse>
GeneralStorageServiceHandler::future_remove(const cpp2::KVRemoveRequest& req) {
    auto* processor = RemoveProcessor::instance(env_);
    RETURN_FUTURE(processor);
}

}  // namespace storage
}  // namespace nebula
