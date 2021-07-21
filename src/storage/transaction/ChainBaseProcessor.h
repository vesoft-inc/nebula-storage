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

constexpr int32_t chainRetryLimit = 10;

using Code = ::nebula::cpp2::ErrorCode;
class ChainBaseProcessor {
public:
    virtual ~ChainBaseProcessor() = default;

    virtual folly::SemiFuture<Code> prepareLocal() { return Code::SUCCEEDED; }

    virtual folly::SemiFuture<Code> processRemote(Code code) { return code; }

    virtual folly::SemiFuture<Code> processLocal(Code code) { return code; }
};


}  // namespace storage
}  // namespace nebula
