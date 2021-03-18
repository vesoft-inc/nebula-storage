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
#include "storage/transaction/ChainProcessorFactory.h"

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
            if (rc != nebula::cpp2::ErrorCode::SUCCEEDED) {
                break;
            }
            for (; iter->valid(); iter->next()) {
                ResumeOptions opt(ResumeType::RESUME_CHAIN, iter->val().str());
                auto* proc = ChainProcessorFactory::makeProcessor(env_, opt);
                futs.emplace_back(proc->getFinished());
                env_->txnMan_->addChainTask(proc);
            }

            prefix = ConsistUtil::doublePrimePrefix(partId);
            rc = env_->kvstore_->prefix(spaceId, partId, prefix, &iter);
            if (rc != nebula::cpp2::ErrorCode::SUCCEEDED) {
                break;
            }
            for (; iter->valid(); iter->next()) {
                ResumeOptions opt(ResumeType::RESUME_REMOTE, iter->val().str());
                auto* proc = ChainProcessorFactory::makeProcessor(env_, opt);
                futs.emplace_back(proc->getFinished());
                env_->txnMan_->addChainTask(proc);
            }
        }
    }
    folly::collectAll(futs).get();
}

}  // namespace storage
}  // namespace nebula
