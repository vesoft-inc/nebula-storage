/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef UTILS_UTILS_H_
#define UTILS_UTILS_H_

#include "common/base/Base.h"

namespace nebula {
class Utils final {
public:
    // Calculate the admin service address based on the storage service address
    static HostAddr getAdminAddrFromStoreAddr(HostAddr storeAddr) {
        if (storeAddr == HostAddr("", 0)) {
            return storeAddr;
        }
        return HostAddr(storeAddr.host, storeAddr.port - 1);
    }

    static HostAddr getStoreAddrFromAdminAddr(HostAddr adminAddr) {
        if (adminAddr == HostAddr("", 0)) {
            return adminAddr;
        }
        return HostAddr(adminAddr.host, adminAddr.port + 1);
    }

    // Calculate the raft service address based on the storage service address
    static HostAddr getRaftAddrFromStoreAddr(const HostAddr& srvcAddr) {
        if (srvcAddr == HostAddr("", 0)) {
            return srvcAddr;
        }
        return HostAddr(srvcAddr.host, srvcAddr.port + 1);
    }

    static HostAddr getStoreAddrFromRaftAddr(const HostAddr& raftAddr) {
        if (raftAddr == HostAddr("", 0)) {
            return raftAddr;
        }
        return HostAddr(raftAddr.host, raftAddr.port - 1);
    }

    static HostAddr getInternalAddrFromStoreAddr(HostAddr adminAddr) {
        if (adminAddr == HostAddr("", 0)) {
            return adminAddr;
        }
        return HostAddr(adminAddr.host, adminAddr.port - 2);
    }

    static Value vidStrToValue(bool isIntVid, const std::string &vidStr) {
        Value vid;
        if (isIntVid) {
            int64_t val;
            memcpy(reinterpret_cast<void*>(&val), vidStr.c_str(), sizeof(int64_t));
            vid.setInt(val);
        } else {
            vid.setStr(vidStr);
        }
        return vid;
    }
};
}  // namespace nebula
#endif  // UTILS_UTILS_H_
