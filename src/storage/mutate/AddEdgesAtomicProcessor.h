/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_MUTATE_ADDEDGESATOMICPROCESSOR_H_
#define STORAGE_MUTATE_ADDEDGESATOMICPROCESSOR_H_

#include "common/base/Base.h"
#include "kvstore/LogEncoder.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

extern ProcessorCounters kAddEdgesAtomicCounters;

class AddEdgesAtomicProcessor : public BaseProcessor<cpp2::ExecResponse> {
public:
    static AddEdgesAtomicProcessor* instance(
            StorageEnv* env,
            const ProcessorCounters* counters = &kAddEdgesAtomicCounters) {
        return new AddEdgesAtomicProcessor(env, counters);
    }

    void process(const cpp2::AddEdgesRequest& req);

private:
    AddEdgesAtomicProcessor(StorageEnv* env, const ProcessorCounters* counters)
        : BaseProcessor<cpp2::ExecResponse>(env, counters) {}

    void processInEdges(const cpp2::AddEdgesRequest& req);

    std::pair<std::string, cpp2::ErrorCode> encodeEdge(const cpp2::NewEdge& e);

    GraphSpaceID                                                spaceId_;
    int64_t                                                     vIdLen_;
    std::vector<std::string>                                    propNames_;
    meta::cpp2::PropertyType                                    spaceVidType_;
    std::atomic<int>                                            activeSubRequest_;
    // bool convertVid_{false};
};

}   // namespace storage
}   // namespace nebula
#endif   // STORAGE_MUTATE_ADDEDGESATOMICPROCESSOR_H_
