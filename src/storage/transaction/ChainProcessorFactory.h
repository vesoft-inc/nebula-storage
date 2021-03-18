/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "storage/CommonUtils.h"
#include "storage/transaction/ChainBaseProcessor.h"

namespace nebula {
namespace storage {

enum class ResumeType {
    UNKNOWN = 0,
    RESUME_CHAIN,
    RESUME_REMOTE,
};

struct ResumeOptions {
    ResumeOptions(ResumeType tp, std::string val) : resumeType(tp), primeValue(std::move(val)) {}
    ResumeType resumeType;
    std::string primeValue;
};

class ChainProcessorFactory {
public:
    static ChainBaseProcessor* makeProcessor(StorageEnv* env, const ResumeOptions& options);
};

}  // namespace storage
}  // namespace nebula
