/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_ADMIN_ADMINTASKPROCESSOR_H_
#define STORAGE_ADMIN_ADMINTASKPROCESSOR_H_
#include "base/Base.h"
#include "kvstore/NebulaStore.h"
#include "storage/BaseProcessor.h"
#include "storage/StorageFlags.h"

#include "thrift/ThriftTypes.h"

namespace nebula {
namespace storage {
class AdminTaskProcessor : public BaseProcessor<cpp2::AdminExecResp> {
public:
    // static AdminTaskProcessor* instance(kvstore::KVStore* kvstore) {
    static AdminTaskProcessor* instance(StorageEnv* env) {
        // return new AdminTaskProcessor(kvstore);
        return new AdminTaskProcessor(env);
    }

    void process(const cpp2::AddAdminTaskRequest& req);

private:
    // explicit AdminTaskProcessor(kvstore::KVStore* kvstore)
    explicit AdminTaskProcessor(StorageEnv* env)
            : BaseProcessor<cpp2::AdminExecResp>(env) {}
};
}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_ADMINTASKPROCESSOR_H_
