/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "common/interface/gen-cpp2/storage_types.h"
#include "storage/BaseProcessor.h"
#include "kvstore/LogEncoder.h"
#include "storage/transaction/ConsistUtil.h"
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

    folly::SemiFuture<Code> prepareLocal() override;

    folly::SemiFuture<Code> processRemote(Code code) override;

    folly::SemiFuture<Code> processLocal(Code code) override;

    void setRemotePartId(PartitionID remotePartId) { remotePartId_ = remotePartId; }

    void finish() override;

protected:
    explicit ChainAddEdgesProcessorLocal(StorageEnv* env)
        : BaseProcessor<cpp2::ExecResponse>(env) {}

    bool prepareRequest(const cpp2::AddEdgesRequest& req);

    /**
     * @brief resume and set req_ & txnId_ from the val of (double)prime
     * @return true if resume succeeded
     */
    bool deserializeRequest(GraphSpaceID spaceId, PartitionID partId, folly::StringPiece val);

    void doRpc(folly::Promise<Code>&& pro,
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
    std::vector<std::string> sEdgeKey(const cpp2::AddEdgesRequest& req);

    /**
     * @brief normally, the prime/double prime keys will be deleted at AddEdgeProcessor
     *        (in a batch of finial edges), but sometimes ChainProcessor will stop early
     *        and we need this do the clean thing.
     */
    folly::SemiFuture<Code> abort();

    /**
     * @brief helper function to get the reversed request of the normal incoming req.
     *        (to use that for reversed edge)
     */
    cpp2::AddEdgesRequest reverseRequest(const cpp2::AddEdgesRequest& req);

    Code extractRpcError(const cpp2::ExecResponse& resp);

    /**
     * @brief   a normal AddEdgeRequest may contain multi edges
     *          even though they will fail or succeed as a batch in this time
     *          some of them may by overwrite by othere request
     *          so when resume each edge
     */
    cpp2::AddEdgesRequest makeSingleEdgeRequest(PartitionID partId, const cpp2::NewEdge& edge);

    std::vector<kvstore::KV> makePrime();

    std::vector<kvstore::KV> makeDoublePrime();

    void erasePrime();

    void eraseDoublePrime();

    folly::SemiFuture<Code> forwardToDelegateProcessor();

    void markDanglingEdge();

protected:
    GraphSpaceID spaceId_;
    PartitionID localPartId_;
    PartitionID remotePartId_;
    cpp2::AddEdgesRequest req_;
    std::unique_ptr<TransactionManager::LockGuard> lk_;
    int retryLimit_{10};
    TermID localTerm_{-1};

    std::vector<std::string> kvErased_;
    std::vector<kvstore::KV> kvAppend_;
    folly::Optional<int64_t> edgeVer_{folly::none};
    int64_t resumedEdgeVer_{-1};
};


}  // namespace storage
}  // namespace nebula
