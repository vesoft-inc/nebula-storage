/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_GETBALANCEPLANPROCESSOR_H_
#define META_GETBALANCEPLANPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class GetBalancePlanProcessor : public BaseProcessor<cpp2::GetBalancePlanResp> {
    public:
    static GetBalancePlanProcessor* instance(kvstore::KVStore* kvstore) {
        return new GetBalancePlanProcessor(kvstore);
    }

    void process(const cpp2::GetBalancePlanReq& req);

private:
    explicit GetBalancePlanProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::GetBalancePlanResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_GETBALANCEPLANPROCESSOR_H_
