/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_CREATEBACKUPPROCESSOR_H_
#define META_CREATEBACKUPPROCESSOR_H_

#include <gtest/gtest_prod.h>
#include "meta/processors/BaseProcessor.h"
#include "meta/processors/admin/AdminClient.h"

namespace nebula {
namespace meta {

class CreateBackupProcessor : public BaseProcessor<cpp2::CreateBackupResp> {
public:
    static CreateBackupProcessor* instance(kvstore::KVStore* kvstore, AdminClient* client) {
        return new CreateBackupProcessor(kvstore, client);
    }
    void process(const cpp2::CreateBackupReq& req);

private:
    explicit CreateBackupProcessor(kvstore::KVStore* kvstore, AdminClient* client)
        : BaseProcessor<cpp2::CreateBackupResp>(kvstore), client_(client) {}
    folly::Optional<std::vector<std::string>> backupMeta(
        const std::unordered_set<GraphSpaceID>& spaces,
        const std::string& name,
        const std::vector<std::string>*);
    template <typename F>
    bool backupTable(const std::unordered_set<GraphSpaceID>& spaces,
                     const std::string& backupName,
                     std::vector<std::string>& files,
                     F f);
    cpp2::ErrorCode cancelWriteBlocking();
    folly::Optional<std::unordered_set<GraphSpaceID>> spaceNameToId(
        const std::vector<std::string>* backupSpaces);

private:
    AdminClient* client_;
};
}   // namespace meta
}   // namespace nebula

#endif   // META_CREATEBackupPROCESSOR_H_
