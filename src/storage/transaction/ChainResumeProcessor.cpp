/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/transaction/ChainResumeProcessor.h"
#include "storage/transaction/ChainUpdateEdgeProcessorLocal.h"
#include "storage/transaction/ChainAddEdgesProcessorLocal.h"
#include "storage/transaction/ConsistUtil.h"
#include "storage/transaction/TransactionManager.h"

namespace nebula {
namespace storage {

void ChainResumeProcessor::process() {
    std::unordered_map<GraphSpaceID, std::vector<meta::cpp2::LeaderInfo>> leaders;
    if (env_->kvstore_->allLeader(leaders) == 0) {
        LOG(INFO) << "no leader found, skip any resume process";
        return;
    }
    std::unique_ptr<kvstore::KVIterator> iter;
    for (auto& leader : leaders) {
        auto spaceId = leader.first;
        LOG(INFO) << "leader.second.size() = " << leader.second.size();
        for (auto& partInfo : leader.second) {
            auto partId = partInfo.get_part_id();
            auto prefix = ConsistUtil::primePrefix(partId);
            auto rc = env_->kvstore_->prefix(spaceId, partId, prefix, &iter);
            if (rc == nebula::cpp2::ErrorCode::SUCCEEDED && iter->valid()) {
                doResumeChain(spaceId, partId, iter->val());
                break;
            } else {
                LOG(INFO) << "no prime edge, partId = " << partId;
            }
            prefix = ConsistUtil::doublePrimePrefix(partId);
            rc = env_->kvstore_->prefix(spaceId, partId, prefix, &iter);
            if (rc == nebula::cpp2::ErrorCode::SUCCEEDED && iter->valid()) {
                doResumeRemote(spaceId, partId, iter->val());
                break;
            } else {
                LOG(INFO) << "no double prime edge, partId = " << partId;
            }
        }
    }
}

// prime indicate:
// prepareLocal() ==> succeeded
// processRemote() => not sure (can't determine the status of remote edge)
// processLocal() => not executed (local edge in not inserted)
void ChainResumeProcessor::doResumeChain(GraphSpaceID spaceId,
                                         PartitionID partId,
                                         folly::StringPiece val) {
    auto type = ConsistUtil::parseType(val);
    switch (type) {
        case TypeOfEdgePrime::INSERT: {
            if (addProc == nullptr) {
                addProc = ChainAddEdgesProcessorLocal::instance(env_);
            }
            auto f = addProc->getFuture();
            addProc->resumeChain(spaceId, partId, val);
            f.wait();
        } break;
        case TypeOfEdgePrime::UPDATE: {
            if (updProc == nullptr) {
                updProc = ChainUpdateEdgeProcessorLocal::instance(env_);
            }
            auto f = updProc->getFuture();
            updProc->resumeChain(val);
            f.wait();
        } break;
        default:
            LOG(WARNING) << "unknown parsed type";
            break;
    }
}

// double prime indicate:
// prepareLocal() ==> succeeded
// processRemote() => not sure (can't determine the status of remote edge)
// processLocal() => succeeded (local edge in inserted)
void ChainResumeProcessor::doResumeRemote(GraphSpaceID spaceId,
                                          PartitionID partId,
                                          folly::StringPiece val) {
    auto type = ConsistUtil::parseType(val);
    switch (type) {
        case TypeOfEdgePrime::INSERT: {
            if (addProc == nullptr) {
                addProc = ChainAddEdgesProcessorLocal::instance(env_);
            }
            auto f = addProc->getFuture();
            addProc->resumeRemote(spaceId, partId, val);
            f.wait();
        } break;
        case TypeOfEdgePrime::UPDATE: {
            if (updProc == nullptr) {
                updProc = ChainUpdateEdgeProcessorLocal::instance(env_);
            }
            auto f = updProc->getFuture();
            updProc->resumeRemote(val);
            f.wait();
        } break;
        default:
            LOG(WARNING) << "unknown parsed type";
            break;
    }
}

}  // namespace storage
}  // namespace nebula
