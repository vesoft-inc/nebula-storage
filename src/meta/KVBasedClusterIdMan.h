/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_CLUSTERIDMAN_H_
#define META_CLUSTERIDMAN_H_

#include "base/Base.h"
#include "fs/FileUtils.h"
#include "kvstore/Common.h"
#include "kvstore/KVStore.h"
#include "meta/processors/Common.h"
#include <folly/synchronization/Baton.h>


namespace nebula {
namespace meta {

/**
 * This class manages clusterId used for meta server and storage server.
 * */
class ClusterIdMan {
public:
    static ClusterID getClusterIdFromKV(kvstore::KVStore* kv, const std::string& key) {
        CHECK_NOTNULL(kv);
        std::string value;
        auto code = kv->get(0, 0, key, &value);
        if (code == kvstore::ResultCode::ERR_KEY_NOT_FOUND) {
            LOG(INFO) << "There is no clusterId existed in kvstore!";
            return 0;
        } else if (code == kvstore::ResultCode::SUCCEEDED) {
            if (value.size() != sizeof(ClusterID)) {
                LOG(ERROR) << "Bad clusterId " << value;
                return 0;
            }
            return *reinterpret_cast<const ClusterID*>(value.data());
        } else {
            LOG(ERROR) << "Error in kvstore, err " << static_cast<int32_t>(code);
            return 0;
        }
    }


    static bool persistInKV(kvstore::KVStore* kv,
                            const std::string& key,
                            ClusterID clusterId) {
        CHECK_NOTNULL(kv);
        std::vector<kvstore::KV> data;
        data.emplace_back(key,
                          std::string(reinterpret_cast<char*>(&clusterId), sizeof(ClusterID)));
        bool ret = true;
        folly::Baton<true, std::atomic> baton;
        kv->asyncMultiPut(0, 0, std::move(data), [&](kvstore::ResultCode code) {
                               if (code != kvstore::ResultCode::SUCCEEDED) {
                                   LOG(ERROR) << "Put failed, error "
                                              << static_cast<int32_t>(code);
                                    ret = false;
                               } else {
                                   LOG(INFO) << "Put key " << key
                                             << ", val " << clusterId;
                               }
                               baton.post();
                           });
        baton.wait();
        return ret;
    }
};

}  // namespace meta
}  // namespace nebula
#endif  // META_CLUSTERIDMAN_H_

