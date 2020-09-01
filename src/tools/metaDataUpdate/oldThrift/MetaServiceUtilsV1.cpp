/* Copyright (c) 2018 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "tools/metaDataUpdate/oldThrift/MetaServiceUtilsV1.h"
#include <thrift/lib/cpp2/protocol/Serializer.h>
#include <thrift/lib/cpp2/protocol/CompactProtocol.h>

namespace nebula {
namespace oldmeta {

const std::string kSpacesTable         = "__spaces__";         // NOLINT
const std::string kPartsTable          = "__parts__";          // NOLINT
const std::string kHostsTable          = "__hosts__";          // NOLINT
const std::string kTagsTable           = "__tags__";           // NOLINT
const std::string kEdgesTable          = "__edges__";          // NOLINT
const std::string kIndexesTable        = "__indexes__";        // NOLINT
const std::string kIndexTable          = "__index__";          // NOLINT
const std::string kIndexStatusTable    = "__index_status__";   // NOLINT
const std::string kUsersTable          = "__users__";          // NOLINT
const std::string kRolesTable          = "__roles__";          // NOLINT
const std::string kConfigsTable        = "__configs__";        // NOLINT
const std::string kDefaultTable        = "__default__";        // NOLINT
const std::string kSnapshotsTable      = "__snapshots__";      // NOLINT
const std::string kLastUpdateTimeTable = "__last_update_time__"; // NOLINT
const std::string kLeadersTable        = "__leaders__";          // NOLINT

const std::string kHostOnline  = "Online";       // NOLINT
const std::string kHostOffline = "Offline";      // NOLINT

std::string MetaServiceUtilsV1::lastUpdateTimeKey() {
    std::string key;
    key.reserve(kLastUpdateTimeTable.size());
    key.append(kLastUpdateTimeTable.data(), kLastUpdateTimeTable.size());
    return key;
}

std::string MetaServiceUtilsV1::lastUpdateTimeVal(const int64_t timeInMilliSec) {
    std::string val;
    val.reserve(sizeof(int64_t));
    val.append(reinterpret_cast<const char*>(&timeInMilliSec), sizeof(int64_t));
    return val;
}

std::string MetaServiceUtilsV1::spaceKey(GraphSpaceID spaceId) {
    std::string key;
    key.reserve(kSpacesTable.size() + sizeof(GraphSpaceID));
    key.append(kSpacesTable.data(), kSpacesTable.size())
            .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
    return key;
}

std::string MetaServiceUtilsV1::spaceVal(const cpp2::SpaceProperties &properties) {
    std::string val;
    apache::thrift::CompactSerializer::serialize(properties, &val);
    return val;
}

cpp2::SpaceProperties MetaServiceUtilsV1::parseSpace(folly::StringPiece rawData) {
    cpp2::SpaceProperties properties;
    apache::thrift::CompactSerializer::deserialize(rawData, properties);
    return properties;
}

const std::string& MetaServiceUtilsV1::spacePrefix() {
    return kSpacesTable;
}

GraphSpaceID MetaServiceUtilsV1::spaceId(folly::StringPiece rawKey) {
    return *reinterpret_cast<const GraphSpaceID*>(rawKey.data() + kSpacesTable.size());
}

std::string MetaServiceUtilsV1::spaceName(folly::StringPiece rawVal) {
    return parseSpace(rawVal).get_space_name();
}

std::string MetaServiceUtilsV1::partKey(GraphSpaceID spaceId, PartitionID partId) {
    std::string key;
    key.reserve(kPartsTable.size() + sizeof(GraphSpaceID) + sizeof(PartitionID));
    key.append(kPartsTable.data(), kPartsTable.size())
            .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID))
            .append(reinterpret_cast<const char*>(&partId), sizeof(PartitionID));
    return key;
}

GraphSpaceID MetaServiceUtilsV1::parsePartKeySpaceId(folly::StringPiece key) {
    return *reinterpret_cast<const GraphSpaceID*>(key.data() + kPartsTable.size());
}

PartitionID MetaServiceUtilsV1::parsePartKeyPartId(folly::StringPiece key) {
    return *reinterpret_cast<const PartitionID*>(key.data()
                                                 + kPartsTable.size()
                                                 + sizeof(GraphSpaceID));
}

std::string MetaServiceUtilsV1::partVal(const std::vector<cpp2::HostAddr>& hosts) {
    std::string val;
    val.reserve(hosts.size() * (sizeof(cpp2::IPv4) + sizeof(cpp2::Port)));
    for (auto& h : hosts) {
        val.append(reinterpret_cast<const char*>(&h.ip), sizeof(h.ip))
                .append(reinterpret_cast<const char*>(&h.port), sizeof(h.port));
    }
    return val;
}

std::string MetaServiceUtilsV1::partPrefix(GraphSpaceID spaceId) {
    std::string prefix;
    prefix.reserve(kPartsTable.size() + sizeof(GraphSpaceID));
    prefix.append(kPartsTable.data(), kPartsTable.size())
            .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
    return prefix;
}

std::string MetaServiceUtilsV1::partPrefix() {
    std::string prefix;
    prefix.reserve(kPartsTable.size() + sizeof(GraphSpaceID));
    prefix.append(kPartsTable.data(), kPartsTable.size());
    return prefix;
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

std::string MetaServiceUtilsV1::hostKey(cpp2::IPv4 ip, Port port) {
    std::string key;
    key.reserve(kHostsTable.size() + sizeof(cpp2::IPv4) + sizeof(cpp2::Port));
    key.append(kHostsTable.data(), kHostsTable.size())
            .append(reinterpret_cast<const char*>(&ip), sizeof(ip))
            .append(reinterpret_cast<const char*>(&port), sizeof(port));
    return key;
}

std::string MetaServiceUtilsV1::hostValOnline() {
    return kHostOnline;
}

std::string MetaServiceUtilsV1::hostValOffline() {
    return kHostOffline;
}

const std::string& MetaServiceUtilsV1::hostPrefix() {
    return kHostsTable;
}

cpp2::HostAddr MetaServiceUtilsV1::parseHostKey(folly::StringPiece key) {
    cpp2::HostAddr host;
    memcpy(&host, key.data() + kHostsTable.size(), sizeof(host));
    return host;
}

std::string MetaServiceUtilsV1::leaderKey(cpp2::IPv4 ip, Port port) {
    std::string key;
    key.reserve(kLeadersTable.size() + sizeof(cpp2::IPv4) + sizeof(cpp2::Port));
    key.append(kLeadersTable.data(), kLeadersTable.size());
    key.append(reinterpret_cast<const char*>(&ip), sizeof(ip));
    key.append(reinterpret_cast<const char*>(&port), sizeof(port));
    return key;
}

std::string MetaServiceUtilsV1::leaderVal(const LeaderParts& leaderParts) {
    std::string value;
    value.reserve(512);
    for (const auto& spaceEntry : leaderParts) {
        GraphSpaceID spaceId = spaceEntry.first;
        value.append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
        size_t leaderCount = spaceEntry.second.size();
        value.append(reinterpret_cast<const char*>(&leaderCount), sizeof(size_t));
        for (const auto& partId : spaceEntry.second) {
            value.append(reinterpret_cast<const char*>(&partId), sizeof(PartitionID));
        }
    }
    return value;
}

const std::string& MetaServiceUtilsV1::leaderPrefix() {
    return kLeadersTable;
}

cpp2::HostAddr MetaServiceUtilsV1::parseLeaderKey(folly::StringPiece key) {
    cpp2::HostAddr host;
    memcpy(&host, key.data() + kLeadersTable.size(), sizeof(host));
    return host;
}

LeaderParts MetaServiceUtilsV1::parseLeaderVal(folly::StringPiece val) {
    LeaderParts leaderParts;
    size_t size = val.size();
    // decode leader info
    size_t offset = 0;
    while (offset + sizeof(GraphSpaceID) + sizeof(size_t) < size) {
        GraphSpaceID spaceId = *reinterpret_cast<const GraphSpaceID*>(val.data() + offset);
        offset += sizeof(GraphSpaceID);
        size_t leaderCount = *reinterpret_cast<const size_t*>(val.data() + offset);
        offset += sizeof(size_t);
        std::vector<PartitionID> partIds;
        for (size_t i = 0; i < leaderCount && offset < size; i++) {
            partIds.emplace_back(*reinterpret_cast<const PartitionID*>(val.data() + offset));
            offset += sizeof(PartitionID);
        }
        leaderParts.emplace(spaceId, std::move(partIds));
    }
    return leaderParts;
}

std::string MetaServiceUtilsV1::schemaEdgePrefix(GraphSpaceID spaceId, EdgeType edgeType) {
    std::string key;
    key.reserve(kEdgesTable.size() + sizeof(GraphSpaceID) + sizeof(edgeType));
    key.append(kEdgesTable.data(), kEdgesTable.size())
            .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID))
            .append(reinterpret_cast<const char*>(&edgeType), sizeof(edgeType));
    return key;
}

std::string MetaServiceUtilsV1::schemaEdgesPrefix(GraphSpaceID spaceId) {
    std::string key;
    key.reserve(kEdgesTable.size() + sizeof(GraphSpaceID));
    key.append(kEdgesTable.data(), kEdgesTable.size())
            .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
    return key;
}

std::string MetaServiceUtilsV1::schemaEdgeKey(GraphSpaceID spaceId,
                                            EdgeType edgeType,
                                            SchemaVer version) {
    auto storageVer = std::numeric_limits<SchemaVer>::max() - version;
    std::string key;
    key.reserve(kEdgesTable.size() + sizeof(GraphSpaceID) + sizeof(EdgeType) + sizeof(SchemaVer));
    key.append(kEdgesTable.data(), kEdgesTable.size())
            .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID))
            .append(reinterpret_cast<const char*>(&edgeType), sizeof(EdgeType))
            .append(reinterpret_cast<const char*>(&storageVer), sizeof(SchemaVer));
    return key;
}

std::string MetaServiceUtilsV1::schemaEdgeVal(const std::string& name,
                                            const cpp2::Schema& schema) {
    auto len = name.size();
    std::string val, sval;
    apache::thrift::CompactSerializer::serialize(schema, &sval);
    val.reserve(sizeof(int32_t) + name.size() + sval.size());
    val.append(reinterpret_cast<const char*>(&len), sizeof(int32_t))
            .append(name)
            .append(sval);
    return val;
}

SchemaVer MetaServiceUtilsV1::parseEdgeVersion(folly::StringPiece key) {
    auto offset = kEdgesTable.size() + sizeof(GraphSpaceID) + sizeof(EdgeType);
    return std::numeric_limits<SchemaVer>::max() -
           *reinterpret_cast<const SchemaVer*>(key.begin() + offset);
}

std::string MetaServiceUtilsV1::schemaTagKey(GraphSpaceID spaceId, TagID tagId, SchemaVer version) {
    auto storageVer = std::numeric_limits<SchemaVer>::max() - version;
    std::string key;
    key.reserve(kTagsTable.size() + sizeof(GraphSpaceID) + sizeof(TagID) + sizeof(SchemaVer));
    key.append(kTagsTable.data(), kTagsTable.size())
            .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID))
            .append(reinterpret_cast<const char*>(&tagId), sizeof(TagID))
            .append(reinterpret_cast<const char*>(&storageVer), sizeof(SchemaVer));
    return key;
}

std::string MetaServiceUtilsV1::schemaTagVal(const std::string& name,
                                           const cpp2::Schema& schema) {
    int32_t len = name.size();
    std::string val, sval;
    apache::thrift::CompactSerializer::serialize(schema, &sval);
    val.reserve(sizeof(int32_t) + name.size() + sval.size());
    val.append(reinterpret_cast<const char*>(&len), sizeof(int32_t))
            .append(name)
            .append(sval);
    return val;
}

SchemaVer MetaServiceUtilsV1::parseTagVersion(folly::StringPiece key) {
    auto offset = kTagsTable.size() + sizeof(GraphSpaceID) + sizeof(TagID);
    return std::numeric_limits<SchemaVer>::max() -
           *reinterpret_cast<const SchemaVer*>(key.begin() + offset);
}

std::string MetaServiceUtilsV1::schemaTagPrefix(GraphSpaceID spaceId, TagID tagId) {
    std::string key;
    key.reserve(kTagsTable.size() + sizeof(GraphSpaceID) + sizeof(TagID));
    key.append(kTagsTable.data(), kTagsTable.size())
            .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID))
            .append(reinterpret_cast<const char*>(&tagId), sizeof(TagID));
    return key;
}

std::string MetaServiceUtilsV1::schemaTagsPrefix(GraphSpaceID spaceId) {
    std::string key;
    key.reserve(kTagsTable.size() + sizeof(GraphSpaceID));
    key.append(kTagsTable.data(), kTagsTable.size())
            .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
    return key;
}

cpp2::Schema MetaServiceUtilsV1::parseSchema(folly::StringPiece rawData) {
    cpp2::Schema schema;
    int32_t offset = sizeof(int32_t) + *reinterpret_cast<const int32_t *>(rawData.begin());
    auto schval = rawData.subpiece(offset, rawData.size() - offset);
    apache::thrift::CompactSerializer::deserialize(schval, schema);
    return schema;
}

std::string MetaServiceUtilsV1::indexKey(GraphSpaceID spaceID, IndexID indexID) {
    std::string key;
    key.reserve(sizeof(GraphSpaceID) + sizeof(IndexID));
    key.append(kIndexesTable.data(), kIndexesTable.size());
    key.append(reinterpret_cast<const char*>(&spaceID), sizeof(GraphSpaceID))
            .append(reinterpret_cast<const char*>(&indexID), sizeof(IndexID));
    return key;
}

std::string MetaServiceUtilsV1::indexVal(const cpp2::IndexItem& item) {
    std::string value;
    apache::thrift::CompactSerializer::serialize(item, &value);
    return value;
}

std::string MetaServiceUtilsV1::indexPrefix(GraphSpaceID spaceId) {
    std::string key;
    key.reserve(kIndexesTable.size() + sizeof(GraphSpaceID));
    key.append(kIndexesTable.data(), kIndexesTable.size())
            .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
    return key;
}

cpp2::IndexItem MetaServiceUtilsV1::parseIndex(const folly::StringPiece& rawData) {
    cpp2::IndexItem item;
    apache::thrift::CompactSerializer::deserialize(rawData, item);
    return item;
}

// This method should replace with JobManager when it ready.
std::string MetaServiceUtilsV1::rebuildIndexStatus(GraphSpaceID space,
                                                 char type,
                                                 const std::string& indexName) {
    std::string key;
    key.reserve(64);
    key.append(kIndexStatusTable.data(), kIndexStatusTable.size())
            .append(reinterpret_cast<const char*>(&space), sizeof(GraphSpaceID))
            .append(1, type)
            .append(indexName);
    return key;
}

// This method should replace with JobManager when it ready.
std::string MetaServiceUtilsV1::rebuildIndexStatusPrefix(GraphSpaceID space,
                                                       char type) {
    std::string key;
    key.reserve(kIndexStatusTable.size() + sizeof(GraphSpaceID) + sizeof(char));
    key.append(kIndexStatusTable.data(), kIndexStatusTable.size())
            .append(reinterpret_cast<const char*>(&space), sizeof(GraphSpaceID))
            .append(1, type);
    return key;
}

std::string MetaServiceUtilsV1::rebuildIndexStatusPrefix() {
    std::string key;
    key.reserve(kIndexStatusTable.size());
    key.append(kIndexStatusTable.data(), kIndexStatusTable.size());
    return key;
}

std::string MetaServiceUtilsV1::indexSpaceKey(const std::string& name) {
    EntryType type = EntryType::SPACE;
    std::string key;
    key.reserve(128);
    key.append(kIndexTable.data(), kIndexTable.size())
            .append(reinterpret_cast<const char*>(&type), sizeof(type))
            .append(name);
    return key;
}

std::string MetaServiceUtilsV1::indexTagKey(GraphSpaceID spaceId,
                                          const std::string& name) {
    EntryType type = EntryType::TAG;
    std::string key;
    key.reserve(128);
    key.append(kIndexTable.data(), kIndexTable.size())
            .append(reinterpret_cast<const char*>(&type), sizeof(type))
            .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID))
            .append(name);
    return key;
}

std::string MetaServiceUtilsV1::indexEdgeKey(GraphSpaceID spaceId,
                                           const std::string& name) {
    EntryType type = EntryType::EDGE;
    std::string key;
    key.reserve(128);
    key.append(kIndexTable.data(), kIndexTable.size())
            .append(reinterpret_cast<const char*>(&type), sizeof(type))
            .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID))
            .append(name);
    return key;
}

std::string MetaServiceUtilsV1::indexIndexKey(GraphSpaceID spaceID,
                                            const std::string& indexName) {
    std::string key;
    key.reserve(128);
    key.append(kIndexTable.data(), kIndexTable.size());
    EntryType type = EntryType::INDEX;
    key.append(reinterpret_cast<const char*>(&type), sizeof(type))
            .append(reinterpret_cast<const char*>(&spaceID), sizeof(GraphSpaceID))
            .append(indexName);
    return key;
}

std::string MetaServiceUtilsV1::assembleSegmentKey(const std::string& segment,
                                                 const std::string& key) {
    std::string segmentKey;
    segmentKey.reserve(64);
    segmentKey.append(segment)
            .append(key.data(), key.size());
    return segmentKey;
}

std::string MetaServiceUtilsV1::defaultKey(GraphSpaceID spaceId,
                                         TagID id,
                                         const std::string& field) {
    // Assume edge/tag default value key is same formated
    static_assert(std::is_same<TagID, EdgeType>::value, "");
    std::string key;
    key.reserve(kDefaultTable.size() + sizeof(GraphSpaceID) + sizeof(TagID));
    key.append(kDefaultTable.data(), kDefaultTable.size())
            .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID))
            .append(reinterpret_cast<const char*>(&id), sizeof(TagID))
            .append(field);
    return key;
}

const std::string& MetaServiceUtilsV1::defaultPrefix() {
    return kDefaultTable;
}

std::string MetaServiceUtilsV1::configKey(const cpp2::ConfigModule& module,
                                        const std::string& name) {
    int32_t nSize = name.size();
    std::string key;
    key.reserve(128);
    key.append(kConfigsTable.data(), kConfigsTable.size())
            .append(reinterpret_cast<const char*>(&module), sizeof(cpp2::ConfigModule))
            .append(reinterpret_cast<const char*>(&nSize), sizeof(int32_t))
            .append(name);
    return key;
}

std::string MetaServiceUtilsV1::configKeyPrefix(const cpp2::ConfigModule& module) {
    std::string key;
    key.reserve(128);
    key.append(kConfigsTable.data(), kConfigsTable.size());
    if (module != cpp2::ConfigModule::ALL) {
        key.append(reinterpret_cast<const char*>(&module), sizeof(cpp2::ConfigModule));
    }
    return key;
}

std::string MetaServiceUtilsV1::configValue(const cpp2::ConfigType& valueType,
                                          const cpp2::ConfigMode& valueMode,
                                          const std::string& config) {
    std::string val;
    val.reserve(sizeof(cpp2::ConfigType) + sizeof(cpp2::ConfigMode) + config.size());
    val.append(reinterpret_cast<const char*>(&valueType), sizeof(cpp2::ConfigType))
            .append(reinterpret_cast<const char*>(&valueMode), sizeof(cpp2::ConfigMode))
            .append(config);
    return val;
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
}  // namespace oldmeta
}  // namespace nebula
