/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_CREATESPACEPROCESSOR_H_
#define META_CREATESPACEPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

using Hosts = std::vector<HostAddr>;

class CreateSpaceProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static CreateSpaceProcessor* instance(kvstore::KVStore* kvstore) {
        return new CreateSpaceProcessor(kvstore);
    }

    void process(const cpp2::CreateSpaceReq& req);

private:
    explicit CreateSpaceProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ExecResp>(kvstore) {}

    Hosts pickHosts(PartitionID partId,
                    const Hosts& hosts,
                    int32_t replicaFactor);

    StatusOr<Hosts>
    pickHostsWithZone(const std::vector<std::string>& zones,
                      std::unordered_map<HostAddr, int32_t>& loading,
                      std::unordered_map<std::string, Hosts>& zoneHosts);

    // Get all host's part loading
    StatusOr<std::unordered_map<HostAddr, int32_t>> getHostLoading();

    StatusOr<std::vector<std::string>>
    pickLightLoadZones(const std::vector<std::string>& zones,
                       int32_t replicaFactor,
                       std::unordered_map<HostAddr, int32_t>& loading,
                       std::unordered_map<std::string, Hosts>& zoneHosts);
};

}  // namespace meta
}  // namespace nebula

#endif  // META_CREATESPACEPROCESSOR_H_
