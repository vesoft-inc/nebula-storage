/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_ACTIVEHOSTSMAN_H_
#define META_ACTIVEHOSTSMAN_H_

#include "common/base/Base.h"
#include <gtest/gtest_prod.h>
#include "kvstore/KVStore.h"
#include "meta/MetaServiceUtils.h"
#include "interface/gen-cpp2/meta_types.h"

namespace nebula {
namespace meta {

struct HostInfo {
    HostInfo() = default;
    explicit HostInfo(int64_t lastHBTimeInMilliSec)
        : lastHBTimeInMilliSec_(lastHBTimeInMilliSec) {}

    HostInfo(int64_t lastHBTimeInMilliSec, cpp2::HostRole role, std::string gitInfoSha)
        : lastHBTimeInMilliSec_(lastHBTimeInMilliSec)
        , role_(role)
        , gitInfoSha_(std::move(gitInfoSha)) {}

    bool operator==(const HostInfo& that) const {
        return this->lastHBTimeInMilliSec_ == that.lastHBTimeInMilliSec_;
    }

    bool operator!=(const HostInfo& that) const {
        return !(*this == that);
    }

    int64_t lastHBTimeInMilliSec_ = 0;
    cpp2::HostRole  role_{cpp2::HostRole::UNKNOWN};
    std::string     gitInfoSha_;

    static std::string encode(const HostInfo& info) {
        std::string encode;
        encode.reserve(sizeof(int64_t));
        encode.append(reinterpret_cast<const char*>(&info.lastHBTimeInMilliSec_), sizeof(int64_t));
        return encode;
    }

    static HostInfo decode(const folly::StringPiece& data) {
        HostInfo info;
        info.lastHBTimeInMilliSec_ = *reinterpret_cast<const int64_t*>(data.data());
        return info;
    }

    static std::string encodeV2(const HostInfo& info) {
        std::string encode;
        encode.reserve(sizeof(int64_t)+sizeof(cpp2::HostRole));
        encode.append(reinterpret_cast<const char*>(&info.lastHBTimeInMilliSec_), sizeof(int64_t));
        if (info.role_ != cpp2::HostRole::UNKNOWN) {
            encode.append(reinterpret_cast<const char*>(&info.role_), sizeof(cpp2::HostRole));
            if (!info.gitInfoSha_.empty()) {
                int len = info.gitInfoSha_.size();
                encode.append(reinterpret_cast<const char*>(&len), sizeof(len));
                encode.append(info.gitInfoSha_.c_str(), len);
            }
        }
        return encode;
    }

    static HostInfo decodeV2(const folly::StringPiece& data) {
        HostInfo info;
        info.lastHBTimeInMilliSec_ = *reinterpret_cast<const int64_t*>(data.data());
        auto offset = sizeof(int64_t);
        if (offset + sizeof(cpp2::HostRole) <= data.size()) {
            info.role_ = *reinterpret_cast<const cpp2::HostRole*>(data.data() + offset);
        }
        offset += sizeof(cpp2::HostRole);
        if (offset + sizeof(int) <= data.size()) {
            int len = *reinterpret_cast<const int*>(data.data() + offset);
            offset += sizeof(int);
            if (offset + len <= data.size()) {
                info.gitInfoSha_ = std::string(data.data() + offset, len);
            } else {
                LOG(ERROR) << folly::format("trying to decode SHA from data, size = {0}",
                                            data.size());
            }
        } else {
            LOG(ERROR) << folly::format("trying to decode role from data, size = {0}",
                                        data.size());
        }
        return info;
    }
};

class ActiveHostsMan final {
public:
    ~ActiveHostsMan() = default;

    static kvstore::ResultCode updateHostInfo(kvstore::KVStore* kv,
                                              const HostAddr& hostAddr,
                                              const HostInfo& info,
                                              const LeaderParts* leaderParts = nullptr);

    static std::vector<HostAddr> getActiveHosts(kvstore::KVStore* kv,
                                                int32_t expiredTTL = 0,
                                                cpp2::HostRole role = cpp2::HostRole::STORAGE);

    static bool isLived(kvstore::KVStore* kv, const HostAddr& host);

protected:
    ActiveHostsMan() = default;
};

class LastUpdateTimeMan final {
public:
    ~LastUpdateTimeMan() = default;

    static kvstore::ResultCode update(kvstore::KVStore* kv, const int64_t timeInMilliSec);

    static int64_t get(kvstore::KVStore* kv);

protected:
    LastUpdateTimeMan() = default;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_ACTIVEHOSTSMAN_H_
