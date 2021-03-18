/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "utils/NebulaKeyUtils.h"
#include "storage/transaction/TransactionManager.h"
#include "storage/transaction/ChainBaseProcessor.h"
#include "storage/transaction/ChainAddEdgesProcessorLocal.h"
#include "storage/transaction/ChainUpdateEdgeProcessorLocal.h"
#include "common/clients/storage/InternalStorageClient.h"

namespace nebula {
namespace storage {

class ChainResumeProcessor {
    friend class ChainResumeProcessorTestHelper;
public:
    explicit ChainResumeProcessor(StorageEnv* env) : env_(env) {}

    void process();

private:
    StorageEnv* env_{nullptr};
    std::vector<folly::Future<Code>> futs;
};

}  // namespace storage
}  // namespace nebula
