/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_TRANSACTION_TRANSACTIONUTILS_H_
#define STORAGE_TRANSACTION_TRANSACTIONUTILS_H_

#include <folly/FBVector.h>
#include <folly/container/Enumerate.h>
#include "common/interface/gen-cpp2/storage_types.h"
#include "kvstore/Common.h"

namespace nebula {
namespace storage {

enum class TossPhase {
    PREPATRE            = 10,
    LOCK                = 15,
    EDGE2_PROCESSOR     = 20,
    COMMIT_EDGE2_REQ    = 25,
    COMMIT_EDGE2_RESP   = 26,
    COMMIT_EDGE1        = 30,
    UNLOCK_EDGE         = 35,
    CLEANUP             = 40,
};

class TransactionUtils {
public:
    static const std::string& edgeLockTable();

    static std::string dumpTransactionReq(const cpp2::TransactionReq& req);

    static std::string dumpAddEdgesRequest(const cpp2::AddEdgesRequest& req);

    static std::string dumpEdgeKey(const cpp2::EdgeKey& key);

    static std::string dumpEdgeKeyHint(const cpp2::EdgeKey& key, const std::string& msg);

    static std::string dumpNewEdge(const cpp2::NewEdge& edge);

    static std::string edgeLockKey(size_t vIdLen,
                                   GraphSpaceID spaceId,
                                   PartitionID partId,
                                   const cpp2::EdgeKey& e);

    static std::string edgeLockKey(size_t vIdLen,
                                   GraphSpaceID spaceId,
                                   PartitionID partId,
                                   VertexID srcId,
                                   EdgeType type,
                                   EdgeRanking rank,
                                   VertexID dstId);

    static std::string edgeLockPrefix(size_t vIdLen,
                                      GraphSpaceID spaceId,
                                      PartitionID partId,
                                      VertexID srcId,
                                      EdgeType type);

    static std::tuple<int, int, cpp2::EdgeKey> parseEdgeKey(const std::string& rawKey);

    static void reverseEdgeKeyInPlace(cpp2::EdgeKey& key);

    static cpp2::NewEdge reverseEdge(const cpp2::NewEdge& edge);

    static cpp2::ErrorCode to(kvstore::ResultCode);

    static cpp2::EdgeKey toEdgekey(size_t vIdLen, folly::StringPiece rawKey);

    static int64_t getSnowFlakeUUID();

    static void intrusiveTest(int rank,
                              TossPhase phase,
                              std::function<void()>&& f);

    static void intrusiveTest(const std::string& skey,
                              TossPhase phase,
                              std::function<void()>&& f);

    static bool triggerTest(int rank, TossPhase phase);
};

struct GoalKeeper {
    static cpp2::ErrorCode check(int ranking, TossPhase phase);
    static cpp2::ErrorCode doSomethingBad(int opCode);
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_TRANSACTION_TRANSACTIONUTILS_H_
