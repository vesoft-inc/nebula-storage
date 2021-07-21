/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "storage/query/QueryBaseProcessor.h"
#include "storage/transaction/ConsistUtil.h"
#include "storage/transaction/ChainBaseProcessor.h"
#include "storage/transaction/TransactionManager.h"
#include "utils/MemoryLockWrapper.h"

namespace nebula {
namespace storage {

class ChainUpdateEdgeProcessorLocal
    : public QueryBaseProcessor<cpp2::UpdateEdgeRequest, cpp2::UpdateResponse>,
      public ChainBaseProcessor {
    friend struct ChainUpdateEdgeTestHelper;
public:
    using Code = ::nebula::cpp2::ErrorCode;
    static ChainUpdateEdgeProcessorLocal* instance(StorageEnv* env) {
        return new ChainUpdateEdgeProcessorLocal(env);
    }

    void process(const cpp2::UpdateEdgeRequest& req) override;

    void resumeChain(folly::StringPiece val);

    void resumeRemote(folly::StringPiece val);

    void onProcessFinished() override {}

    folly::SemiFuture<Code> prepareLocal() override;

    folly::SemiFuture<Code> processRemote(Code code) override;

    folly::SemiFuture<Code> processLocal(Code code) override;

    void finish();

protected:
    explicit ChainUpdateEdgeProcessorLocal(StorageEnv* env)
        : QueryBaseProcessor<cpp2::UpdateEdgeRequest, cpp2::UpdateResponse>(env, nullptr) {}

    Code checkAndBuildContexts(const cpp2::UpdateEdgeRequest& req) override;

    void processInternal(const cpp2::UpdateEdgeRequest& req, ChainProcessType type);

    std::string edgeKey(const cpp2::UpdateEdgeRequest& req);

    void doRpc(folly::Promise<Code>&& promise, int retry = 0) noexcept;

    bool checkTerm();

    bool checkVersion();

    folly::SemiFuture<Code> processResumeRemoteLocal(Code code);

    folly::SemiFuture<Code> processNormalLocal(Code code);

    void doClean();

    cpp2::UpdateEdgeRequest reverseRequest(const cpp2::UpdateEdgeRequest& req);

    bool setLock();

    int64_t getVersion(const cpp2::UpdateEdgeRequest& req);

    nebula::cpp2::ErrorCode getErrorCode(const cpp2::UpdateResponse& resp);

protected:
    cpp2::UpdateEdgeRequest req_;
    std::unique_ptr<TransactionManager::LockGuard> lk_;
    int retryLimit_{10};
    TermID termOfPrepare_{-1};
    ChainProcessType processType_{ChainProcessType::NORMAL};
    std::vector<std::string> kvErased_;
    Code code_;
    folly::Optional<int64_t> ver_{folly::none};
};

}  // namespace storage
}  // namespace nebula
