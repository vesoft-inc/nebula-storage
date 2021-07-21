/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "storage/BaseProcessor.h"
#include "kvstore/LogEncoder.h"
#include "storage/transaction/ConsistUtil.h"
#include "storage/transaction/ChainBaseProcessor.h"
#include "storage/transaction/TransactionManager.h"

namespace nebula {
namespace storage {

class ChainAddEdgesProcessorLocal : public BaseProcessor<cpp2::ExecResponse>,
                                    public ChainBaseProcessor {
    friend class ChainResumeProcessorTestHelper;  // for test friendly
public:
    static ChainAddEdgesProcessorLocal* instance(StorageEnv* env) {
        return new ChainAddEdgesProcessorLocal(env);
    }

    virtual ~ChainAddEdgesProcessorLocal() = default;

    virtual void process(const cpp2::AddEdgesRequest& req);

    void resumeChain(GraphSpaceID spaceId, PartitionID partId, folly::StringPiece val);

    void resumeRemote(GraphSpaceID spaceId, PartitionID partId, folly::StringPiece val);

    folly::SemiFuture<nebula::cpp2::ErrorCode> prepareLocal() override;

    folly::SemiFuture<nebula::cpp2::ErrorCode> processRemote(nebula::cpp2::ErrorCode code) override;

    folly::SemiFuture<nebula::cpp2::ErrorCode> processLocal(nebula::cpp2::ErrorCode code) override;

    void setRemotePartId(PartitionID remotePartId) { remotePartId_ = remotePartId; }

    void finish();

protected:
    explicit ChainAddEdgesProcessorLocal(StorageEnv* env)
        : BaseProcessor<cpp2::ExecResponse>(env) {}

    void processInternal(const cpp2::AddEdgesRequest& req, ChainProcessType type);

    /**
     * @brief resume and set req_ & txnId_ from the val of (double)prime
     * @return true if resume succeeded
     */
    bool resumeRequest(GraphSpaceID spaceId, PartitionID partId, folly::StringPiece val);

    void doRpc(folly::Promise<nebula::cpp2::ErrorCode>&& pro,
               cpp2::AddEdgesRequest&& req,
               int retry = 0) noexcept;

    bool lockEdges(const cpp2::AddEdgesRequest& req);

    bool checkTerm(const cpp2::AddEdgesRequest& req);

    bool checkVersion(const cpp2::AddEdgesRequest& req);

    /**
     * @brief This is a call back function, to let AddEdgesProcessor so some
     *        addition thing for chain operation
     * @param batch if target edge has index
     * @param pData if target edge has no index.
     */
    void callbackOfChainOp(kvstore::BatchHolder& batch, std::vector<kvstore::KV>* pData);

    /**
     * @brief helper function to generate string form of keys of request
     */
    std::vector<std::string> strEdgekeys(const cpp2::AddEdgesRequest& req);

    /**
     * @brief normally, the prime/double prime keys will be deleted at AddEdgeProcessor
     *        (in a batch of finial edges), but sometimes ChainProcessor will stop early
     *        and we need this do the clean thing.
     */
    folly::SemiFuture<nebula::cpp2::ErrorCode> doClean();

    /**
     * @brief helper function to get the reversed request of the normal incoming req.
     *        (to use that for reversed edge)
     */
    cpp2::AddEdgesRequest reverseRequest(const cpp2::AddEdgesRequest& req);

    nebula::cpp2::ErrorCode extractRpcError(const cpp2::ExecResponse& resp);

private:
    cpp2::AddEdgesRequest req_;
    std::unique_ptr<TransactionManager::LockGuard> lk_;
    int retryLimit_{10};
    TermID localTerm_{-1};
    ChainProcessType processType_{ChainProcessType::UNKNOWN};

    std::vector<std::string> erasedItems_;
    std::list<std::pair<std::string, std::string>> appendData_;
    std::vector<int64_t> edgeVers_;
    std::string txnId_;
    PartitionID localPartId_;
    PartitionID remotePartId_;
    int64_t resumedEdgeVer_{-1};
};


}  // namespace storage
}  // namespace nebula
