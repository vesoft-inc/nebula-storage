/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef UTILS_PROCESSORCOUNTERS_H_
#define UTILS_PROCESSORCOUNTERS_H_

#include "common/base/Base.h"
#include "common/stats/StatsManager.h"

namespace nebula {

class ProcessorCounters final {
public:
    virtual ~ProcessorCounters() = default;

    virtual void init(const std::string& counterName, ProcessorCounters* commonCounters) {
        commonCounters_ = commonCounters;
        if (!numCalls_.valid()) {
            numCalls_ = stats::StatsManager::registerStats("num_" + counterName,
                                                           "rate, sum");
            numErrors_ = stats::StatsManager::registerStats("num_" + counterName + "_errors",
                                                            "rate, sum");
            latency_ = stats::StatsManager::registerHisto(counterName + "_latency_us",
                                                          1000,
                                                          0,
                                                          20000,
                                                          "avg, p75, p95, p99");
            VLOG(1) << "Succeeded in initializing the ProcessorCounters instance";
        } else {
            VLOG(1) << "ProcessorCounters instance has been initialized";
        }
    }

    void addNumCalls() const {
        stats::StatsManager::addValue(numCalls_);
        if (commonCounters_) {
            commonCounters_->addNumCalls();
        }
    }

    void addNumErrors() const {
        stats::StatsManager::addValue(numErrors_);
        if (commonCounters_) {
            commonCounters_->addNumErrors();
        }
    }

    void addLatency(uint64_t latency) const {
        stats::StatsManager::addValue(latency_, latency);
        if (commonCounters_) {
            commonCounters_->addLatency(latency);
        }
    }


private:
    stats::CounterId numCalls_;
    stats::CounterId numErrors_;
    stats::CounterId latency_;

    ProcessorCounters* commonCounters_ = nullptr;
};

}  // namespace nebula
#endif  // UTILS_PROCESSORCOUNTERS_H_
