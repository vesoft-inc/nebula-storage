/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_GRAPHSTORAGESERVICEHANDLER_H_
#define STORAGE_GRAPHSTORAGESERVICEHANDLER_H_

#include "base/Base.h"
#include "interface/gen-cpp2/GraphStorageService.h"
#include "stats/Stats.h"

namespace nebula {
namespace storage {

class StorageEnv;
class GraphStorageServiceHandler final : public cpp2::GraphStorageServiceSvIf {
public:
    explicit GraphStorageServiceHandler(StorageEnv* env) : env_(env) {
        lookupQpsStat_ = stats::Stats("storage", "lookup_vertices");
    }

folly::Future<cpp2::LookupIndexResp>
future_lookupIndex(const cpp2::LookupIndexRequest& req) override;

private:
    StorageEnv*             env_{nullptr};
    stats::Stats            lookupQpsStat_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_GRAPHSTORAGESERVICEHANDLER_H_
