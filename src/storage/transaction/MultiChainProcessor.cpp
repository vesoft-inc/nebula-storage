/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/transaction/BaseChainProcessor.h"
#include "storage/transaction/MultiChainProcessor.h"

namespace nebula {
namespace storage {

void MultiChainProcessor::addChainProcessor(BaseChainProcessor* proc) {
    processors_.emplace_back(proc);
}

folly::SemiFuture<cpp2::ErrorCode> MultiChainProcessor::prepareLocal() {
    VLOG(1) << "enter MultiChainProcessor::prepareLocal()";
    for (auto& proc : processors_) {
        results_.emplace_back(proc->prepareLocal());
    }
    VLOG(1)  << "exit MultiChainProcessor::prepareLocal()";
    return folly::makeFuture<cpp2::ErrorCode>(cpp2::ErrorCode::SUCCEEDED);
}

folly::SemiFuture<cpp2::ErrorCode> MultiChainProcessor::processRemote(cpp2::ErrorCode) {
    VLOG(1) << "enter MultiChainProcessor::processRemote()";
    auto c = folly::makePromiseContract<cpp2::ErrorCode>();
    folly::collectAll(results_).thenValue([&, p = std::move(c.first)](auto&& tries) mutable {
        results_.clear();
        for (auto i = 0U; i != processors_.size(); ++i) {
            auto code = tries[i].hasValue() ? tries[i].value() : cpp2::ErrorCode::E_UNKNOWN;
            LOG_IF(WARNING, code != cpp2::ErrorCode::SUCCEEDED)
                << "prepareLocal: " << CommonUtils::name(code);
            results_.emplace_back(processors_[i]->processRemote(code));
        }
        VLOG(1) << "finish MultiChainProcessor::processRemote()";
        p.setValue(cpp2::ErrorCode::SUCCEEDED);
    });
    VLOG(1) << "exit MultiChainProcessor::processRemote()";
    return std::move(c.second);
}

folly::SemiFuture<cpp2::ErrorCode> MultiChainProcessor::processLocal(cpp2::ErrorCode) {
    VLOG(1) << "enter MultiChainProcessor::processLocal()";
    auto c = folly::makePromiseContract<cpp2::ErrorCode>();
    folly::collectAll(results_).thenValue([&, p = std::move(c.first)](auto&& tries) mutable {
        results_.clear();
        for (auto i = 0U; i != processors_.size(); ++i) {
            auto code = tries[i].hasValue() ? tries[i].value() : cpp2::ErrorCode::E_UNKNOWN;
            LOG_IF(WARNING, code != cpp2::ErrorCode::SUCCEEDED)
                << "processRemote: " << CommonUtils::name(code);
            results_.emplace_back(processors_[i]->processLocal(code));
        }
        VLOG(1) << "finish MultiChainProcessor::processLocal()";
        p.setValue(cpp2::ErrorCode::SUCCEEDED);
    });
    VLOG(1) << "exit MultiChainProcessor::processLocal()";
    return std::move(c.second);
}

void MultiChainProcessor::onFinished() {
    VLOG(1) << "enter MultiChainProcessor::onFinished()";
    folly::collectAll(results_).thenValue([&](auto&& tries) {
        for (auto i = 0U; i != processors_.size(); ++i) {
            auto code = tries[i].hasValue() ? tries[i].value() : cpp2::ErrorCode::E_UNKNOWN;
            LOG_IF(INFO, code != cpp2::ErrorCode::SUCCEEDED)
                << "processLocal: " << CommonUtils::name(code);
            processors_[i]->setErrorCode(code);
            processors_[i]->onFinished();
        }
        VLOG(1) << "all sub-processor finished";
        cb_(code_);
        VLOG(1) << "finish MultiChainProcessor::onFinished()";
        delete this;
    });
    VLOG(1) << "exit MultiChainProcessor::onFinished()";
}

void MultiChainProcessor::cleanup() {}

}   // namespace storage
}   // namespace nebula
