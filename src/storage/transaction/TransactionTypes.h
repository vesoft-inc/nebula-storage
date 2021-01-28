/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <string>
#include <folly/Optional.h>
#include <folly/Range.h>

#pragma once

namespace nebula {
namespace storage {

struct PendingLock {
    explicit PendingLock(const folly::StringPiece& lk) : lockKey(lk.str()) {}
    std::string lockKey;
    folly::Optional<std::string> edgeProps;
    folly::Optional<std::string> lockProps;
};

}  // namespace storage
}  // namespace nebula
