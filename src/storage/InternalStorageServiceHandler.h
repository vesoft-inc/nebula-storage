/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_INTERNALSTORAGESERVICEHANDLER_H_
#define STORAGE_INTERNALSTORAGESERVICEHANDLER_H_

#include "common/base/Base.h"
#include "common/stats/Stats.h"
#include "common/stats/StatsManager.h"
#include "common/interface/gen-cpp2/InternalStorageService.h"
#include <folly/executors/IOThreadPoolExecutor.h>
#include "storage/CommonUtils.h"
#include "storage/StorageFlags.h"

namespace nebula {
namespace storage {

class StorageEnv;

class InternalStorageServiceHandler final : public cpp2::InternalStorageServiceSvIf {
public:
    explicit InternalStorageServiceHandler(StorageEnv* env)
        : env_(env)
        , readerPool_(std::make_unique<folly::IOThreadPoolExecutor>(FLAGS_reader_handlers)) {
        addEdgesQpsStat_ = stats::Stats("storage", "add_edges");
    }

    folly::Future<cpp2::ExecResponse>
    future_processTransaction(const cpp2::TransactionReq& req) override;

    // folly::Future<cpp2::GetPropResponse>
    // future_getProps(const cpp2::GetPropRequest& req) override;

private:
    StorageEnv*                                     env_{nullptr};
    std::unique_ptr<folly::IOThreadPoolExecutor>    readerPool_;

    stats::Stats                                    addEdgesQpsStat_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_INTERNALSTORAGESERVICEHANDLER_H_
