/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <folly/container/Enumerate.h>
#include "storage/transaction/TransactionProcessor.h"
#include "storage/transaction/TransactionManager.h"
#include "storage/transaction/TransactionUtils.h"

namespace nebula {
namespace storage {

void TransactionProcessor::process(const cpp2::TransactionReq& req) {
    LOG(INFO) << "messi TransactionProcessor::process(): \n"
              << TransactionUtils::dumpTransactionReq(req);

    TransactionUtils::intrusiveTest(req.edges[0].ranking, TossPhase::COMMIT_EDGE2_REQ, [](){
        throw std::runtime_error("TransactionProcessor::process() invasionTest");
    });

    auto req1 = req;
    env_->txnManager_->prepareTransaction(std::move(req1))
        .via(env_->txnManager_->worker_.get())
        .thenValue([&, partId = req.part_id](cpp2::ErrorCode code) {
            LOG(INFO) << "messi TransactionProcessor code " << static_cast<int>(code);
            TransactionUtils::intrusiveTest(req.edges[0].ranking,
                                            TossPhase::COMMIT_EDGE2_RESP,
                                            [&](){
                code = cpp2::ErrorCode::E_USER_CANCEL;
            });
            pushResultCode(code, partId);
            onFinished();
        }).thenError([&, partId = req.part_id](auto&& e) {
            LOG(ERROR) << "messi TransactionProcessor err: " << e.what();
            pushResultCode(cpp2::ErrorCode::E_TXN_ERR_UNKNOWN, partId);
            onFinished();
        });
}

}  // namespace storage
}  // namespace nebula
