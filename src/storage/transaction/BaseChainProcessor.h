/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "storage/CommonUtils.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "common/time/WallClock.h"
#include "utils/MemoryLockWrapper.h"

namespace nebula {
namespace storage {

class BaseChainProcessor {
public:
    using Callback = folly::Function<void(cpp2::ErrorCode)>;

    explicit BaseChainProcessor(Callback&& cb, StorageEnv* env) : cb_(std::move(cb)), env_(env) {
        txnId_ = std::numeric_limits<int64_t>::max() - time::WallClock::slowNowInMicroSec();
    }

    virtual ~BaseChainProcessor() = default;

    virtual folly::SemiFuture<cpp2::ErrorCode> prepareLocal() = 0;

    virtual folly::SemiFuture<cpp2::ErrorCode> processRemote(cpp2::ErrorCode code) = 0;

    virtual folly::SemiFuture<cpp2::ErrorCode> processLocal(cpp2::ErrorCode code) = 0;

    virtual void cleanup() {}

    virtual void setErrorCode(cpp2::ErrorCode code) {
        if (code_ == cpp2::ErrorCode::SUCCEEDED && code != cpp2::ErrorCode::SUCCEEDED) {
            code_ = code;
        }
    }

    virtual void onFinished() {
        cb_(code_);
        cleanup();
        delete this;
    }

    void setVidLen(int32_t vIdLen) {
        vIdLen_ = vIdLen;
    }

protected:
    Callback cb_;
    StorageEnv* env_{nullptr};
    cpp2::ErrorCode code_{cpp2::ErrorCode::SUCCEEDED};
    int32_t vIdLen_{-1};
    int64_t txnId_{0};
};

}  // namespace storage
}  // namespace nebula
