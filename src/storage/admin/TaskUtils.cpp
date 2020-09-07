/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/admin/TaskUtils.h"

namespace nebula {
namespace storage {

nebula::cpp2::ErrorCode toStorageErr(kvstore::ResultCode code) {
    switch (code) {
    case kvstore::ResultCode::SUCCEEDED:
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    case kvstore::ResultCode::ERR_LEADER_CHANGED:
        return nebula::cpp2::ErrorCode::E_LEADER_CHANGED;
    case kvstore::ResultCode::ERR_SPACE_NOT_FOUND:
        return nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND;
    case kvstore::ResultCode::ERR_PART_NOT_FOUND:
        return nebula::cpp2::ErrorCode::E_PART_NOT_FOUND;
    case kvstore::ResultCode::ERR_CONSENSUS_ERROR:
        return nebula::cpp2::ErrorCode::E_CONSENSUS_ERROR;
    case kvstore::ResultCode::ERR_CHECKPOINT_ERROR:
        return nebula::cpp2::ErrorCode::E_FAILED_TO_CHECKPOINT;
    case kvstore::ResultCode::ERR_WRITE_BLOCK_ERROR:
        return nebula::cpp2::ErrorCode::E_CHECKPOINT_BLOCKED;
    case kvstore::ResultCode::ERR_PARTIAL_RESULT:
        return nebula::cpp2::ErrorCode::E_PARTIAL_RESULT;
    default:
        return nebula::cpp2::ErrorCode::E_INTERNAL_ERROR;
    }
}

}  // namespace storage
}  // namespace nebula
