/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_BASE_OPERATIONKEYUTILS_H_
#define COMMON_BASE_OPERATIONKEYUTILS_H_

#include "common/Types.h"

namespace nebula {

class OperationKeyUtils final {
public:
    ~OperationKeyUtils() = default;

    static bool isOperationKey(const folly::StringPiece& key) {
        constexpr int32_t len = static_cast<int32_t>(sizeof(NebulaKeyType));
        auto type = readInt<int32_t>(key.data(), len);
        return static_cast<uint32_t>(NebulaKeyType::kOperation) == type;
    }

    static std::string modifyOperationKey(PartitionID part, std::string key);

    static std::string deleteOperationKey(PartitionID part);

    static bool isModifyOperation(const folly::StringPiece& rawKey);

    static bool isDeleteOperation(const folly::StringPiece& rawKey);

    static std::string getOperationKey(const folly::StringPiece& rawValue);

    static std::string operationPrefix(PartitionID part);

private:
    OperationKeyUtils() = delete;

// private:
    // static std::atomic<int64_t> counter{0};
};

}  // namespace nebula

#endif  // COMMON_BASE_OPERATIONKEYUTILS_H_
