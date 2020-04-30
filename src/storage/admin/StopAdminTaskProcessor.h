/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_ADMIN_STOPADMINTASKPROCESSOR_H_
#define STORAGE_ADMIN_STOPADMINTASKPROCESSOR_H_
#include "base/Base.h"
#include "kvstore/NebulaStore.h"
#include "storage/BaseProcessor.h"
#include "storage/StorageFlags.h"

#include "thrift/ThriftTypes.h"

namespace nebula {
namespace storage {

class StopAdminTaskProcessor : public BaseProcessor<cpp2::AdminExecResp> {
public:
    static StopAdminTaskProcessor* instance(StorageEnv* env) {
        return new StopAdminTaskProcessor(env);
    }

    void process(const cpp2::StopAdminTaskRequest& req);

private:
    explicit StopAdminTaskProcessor(StorageEnv* env)
            : BaseProcessor<cpp2::AdminExecResp>(env) {}
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_STOPADMINTASKPROCESSOR_H_
