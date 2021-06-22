/* Copyright (c) 2018 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "meta/MetaServiceUtils.h"
#include "meta/upgradeData/oldThrift/MetaServiceUtilsV1.h"

#include <thrift/lib/cpp2/protocol/Serializer.h>
#include <thrift/lib/cpp2/protocol/CompactProtocol.h>

namespace nebula {
namespace oldmeta {

cpp2::SpaceProperties MetaServiceUtilsV1::parseSpace(folly::StringPiece rawData) {
    cpp2::SpaceProperties properties;
    apache::thrift::CompactSerializer::deserialize(rawData, properties);
    return properties;
}

GraphSpaceID MetaServiceUtilsV1::parsePartKeySpaceId(folly::StringPiece key) {
    return *reinterpret_cast<const GraphSpaceID*>(key.data() + kPartsTable.size());
}

PartitionID MetaServiceUtilsV1::parsePartKeyPartId(folly::StringPiece key) {
    return *reinterpret_cast<const PartitionID*>(key.data()
                                                 + kPartsTable.size()
                                                 + sizeof(GraphSpaceID));
}

std::vector<cpp2::HostAddr> MetaServiceUtilsV1::parsePartVal(folly::StringPiece val) {
    std::vector<cpp2::HostAddr> hosts;
    static const size_t unitSize = sizeof(int32_t) * 2;
    auto hostsNum = val.size() / unitSize;
    hosts.reserve(hostsNum);
    VLOG(3) << "Total size:" << val.size()
            << ", host size:" << unitSize
            << ", host num:" << hostsNum;
    for (decltype(hostsNum) i = 0; i < hostsNum; i++) {
        cpp2::HostAddr h;
        h.set_ip(*reinterpret_cast<const int32_t*>(val.data() + i * unitSize));
        h.set_port(*reinterpret_cast<const int32_t*>(val.data() + i * unitSize + sizeof(int32_t)));
        hosts.emplace_back(std::move(h));
    }
    return hosts;
}

cpp2::HostAddr MetaServiceUtilsV1::parseHostKey(folly::StringPiece key) {
    cpp2::HostAddr host;
    memcpy(&host, key.data() + kHostsTable.size(), sizeof(host));
    return host;
}

cpp2::HostAddr MetaServiceUtilsV1::parseLeaderKey(folly::StringPiece key) {
    cpp2::HostAddr host;
    memcpy(&host, key.data() + kLeadersTable.size(), sizeof(host));
    return host;
}

cpp2::Schema MetaServiceUtilsV1::parseSchema(folly::StringPiece rawData) {
    cpp2::Schema schema;
    int32_t offset = sizeof(int32_t) + *reinterpret_cast<const int32_t *>(rawData.begin());
    auto schval = rawData.subpiece(offset, rawData.size() - offset);
    apache::thrift::CompactSerializer::deserialize(schval, schema);
    return schema;
}

cpp2::IndexItem MetaServiceUtilsV1::parseIndex(const folly::StringPiece& rawData) {
    cpp2::IndexItem item;
    apache::thrift::CompactSerializer::deserialize(rawData, item);
    return item;
}

ConfigName MetaServiceUtilsV1::parseConfigKey(folly::StringPiece rawKey) {
    std::string key;
    auto offset = kConfigsTable.size();
    auto module = *reinterpret_cast<const cpp2::ConfigModule*>(rawKey.data() + offset);
    offset += sizeof(cpp2::ConfigModule);
    int32_t nSize = *reinterpret_cast<const int32_t*>(rawKey.data() + offset);
    offset += sizeof(int32_t);
    auto name = rawKey.subpiece(offset, nSize);
    return {module, name.str()};
}

cpp2::ConfigItem MetaServiceUtilsV1::parseConfigValue(folly::StringPiece rawData) {
    int32_t offset = 0;
    cpp2::ConfigType type = *reinterpret_cast<const cpp2::ConfigType*>(rawData.data() + offset);
    offset += sizeof(cpp2::ConfigType);
    cpp2::ConfigMode mode = *reinterpret_cast<const cpp2::ConfigMode*>(rawData.data() + offset);
    offset += sizeof(cpp2::ConfigMode);
    auto value = rawData.subpiece(offset, rawData.size() - offset);

    cpp2::ConfigItem item;
    item.set_type(type);
    item.set_mode(mode);
    item.set_value(value.str());
    return item;
}

int32_t MetaServiceUtilsV1::parseJobId(const folly::StringPiece& rawKey) {
    auto offset = nebula::meta::MetaServiceUtils::jobPrefix().size();
    return *reinterpret_cast<const int32_t*>(rawKey.begin() + offset);
}

std::tuple<nebula::meta::cpp2::AdminCmd,
           std::vector<std::string>,
           nebula::meta::cpp2::JobStatus,
           Timestamp,
           Timestamp>
MetaServiceUtilsV1::parseJobDesc(const folly::StringPiece& rawVal) {
    return nebula::meta::MetaServiceUtils::parseJobValue(rawVal);
}
}  // namespace oldmeta
}  // namespace nebula
