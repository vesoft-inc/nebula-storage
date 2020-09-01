/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


#include "common/datatypes/Value.h"
#include "common/datatypes/Map.h"
#include "common/conf/Configuration.h"
#include "kvstore/Common.h"
#include "meta/MetaServiceUtils.h"
#include "meta/ActiveHostsMan.h"
#include "tools/metaDataUpdate/MetaDataUpdate.h"
#include "tools/metaDataUpdate/oldThrift/MetaServiceUtils.h"

namespace nebula {
namespace meta {

Status MetaDataUpdate::initDB(const std::string& dataPath) {
    rocksdb::Options options;
    rocksdb::DB* db = nullptr;
    auto status = rocksdb::DB::Open(options, dataPath, &db);
    if (!status.ok()) {
        return Status::Error("Open rocksdb failed: %s", status.ToString().c_str());
    }
    db_.reset(db);
    return Status::OK();
}

Status MetaDataUpdate::rewriteHosts(const folly::StringPiece &key,
                                    const folly::StringPiece &val) {
    auto host = oldmeta::MetaServiceUtils::parseHostKey(key);
    auto info = HostInfo::decodeV1(val);
    auto newVal = HostInfo::encodeV2(info);
    auto newKey = MetaServiceUtils::hostKeyV2(
            network::NetworkUtils::intToIPv4(host.ip), host.port);
    NG_LOG_AND_RETURN_IF_ERROR(put(newKey, newVal));
    NG_LOG_AND_RETURN_IF_ERROR(remove(key));
    return Status::OK();
}

Status MetaDataUpdate::rewriteLeaders(const folly::StringPiece &key,
                                      const folly::StringPiece &val) {
    auto host = oldmeta::MetaServiceUtils::parseLeaderKey(key);
    auto newKey = MetaServiceUtils::leaderKey(
            network::NetworkUtils::intToIPv4(host.ip), host.port);
    NG_LOG_AND_RETURN_IF_ERROR(put(newKey, val));
    NG_LOG_AND_RETURN_IF_ERROR(remove(key));
    return Status::OK();
}

Status MetaDataUpdate::rewriteSpaces(const folly::StringPiece &key,
                                     const folly::StringPiece &val) {
    auto oldProps = oldmeta::MetaServiceUtils::parseSpace(val);
    cpp2::SpaceProperties spaceProps;
    spaceProps.set_space_name(oldProps.get_space_name());
    spaceProps.set_partition_num(oldProps.get_partition_num());
    spaceProps.set_replica_factor(oldProps.get_replica_factor());
    spaceProps.set_charset_name(oldProps.get_charset_name());
    spaceProps.set_collate_name(oldProps.get_collate_name());
    spaceProps.set_vid_size(8);
    NG_LOG_AND_RETURN_IF_ERROR(put(key, MetaServiceUtils::spaceVal(spaceProps)));
    return Status::OK();
}

Status MetaDataUpdate::rewriteParts(const folly::StringPiece &key,
                                    const folly::StringPiece &val) {
    auto oldHosts = oldmeta::MetaServiceUtils::parsePartVal(val);
    std::vector<HostAddr> newHosts;
    for (auto &host : oldHosts) {
        HostAddr hostAddr;
        hostAddr.host = network::NetworkUtils::intToIPv4(host.ip);
        hostAddr.port = host.port;
        newHosts.emplace_back(std::move(hostAddr));
    }
    NG_LOG_AND_RETURN_IF_ERROR(put(key, MetaServiceUtils::partVal(newHosts)));
    return Status::OK();
}

Status MetaDataUpdate::rewriteSchemas(const folly::StringPiece &key,
                                      const folly::StringPiece &val) {
    auto oldSchema = oldmeta::MetaServiceUtils::parseSchema(val);
    cpp2::Schema newSchema;
    cpp2::SchemaProp newSchemaProps;
    auto &schemaProp = oldSchema.get_schema_prop();
    if (schemaProp.__isset.ttl_duration) {
        newSchemaProps.set_ttl_duration(*schemaProp.get_ttl_duration());
    }
    if (schemaProp.__isset.ttl_col) {
        newSchemaProps.set_ttl_col(*schemaProp.get_ttl_col());
    }
    newSchema.set_schema_prop(std::move(newSchemaProps));
    NG_LOG_AND_RETURN_IF_ERROR(convertToNewColumns(oldSchema.columns, newSchema.columns));

    auto nameLen = *reinterpret_cast<const int32_t *>(val.data());
    auto schemaName = val.subpiece(sizeof(int32_t), nameLen).str();
    auto encodeVal = MetaServiceUtils::schemaVal(schemaName, newSchema);
    NG_LOG_AND_RETURN_IF_ERROR(put(key, encodeVal));
    return Status::OK();
}

Status MetaDataUpdate::rewriteIndexes(const folly::StringPiece &key,
                                      const folly::StringPiece &val) {
    auto oldItem = oldmeta::MetaServiceUtils::parseIndex(val);
    cpp2::IndexItem newItem;
    newItem.set_index_id(oldItem.get_index_id());
    newItem.set_index_name(oldItem.get_index_name());
    cpp2::SchemaID schemaId;
    if (oldItem.get_schema_id().getType() == oldmeta::cpp2::SchemaID::Type::tag_id) {
        schemaId.set_tag_id(oldItem.get_schema_id().getType());
    } else {
        schemaId.set_edge_type(oldItem.get_schema_id().getType());
    }
    newItem.set_schema_id(schemaId);
    NG_LOG_AND_RETURN_IF_ERROR(convertToNewColumns(oldItem.fields, newItem.fields));
    NG_LOG_AND_RETURN_IF_ERROR(put(key, MetaServiceUtils::indexVal(newItem)));
    return Status::OK();
}

Status MetaDataUpdate::rewriteConfigs(const folly::StringPiece &key,
                                      const folly::StringPiece &val) {
    auto item = oldmeta::MetaServiceUtils::parseConfigValue(val);

    Value configVal;
    switch (item.get_type()) {
        case oldmeta::cpp2::ConfigType::INT64: {
            auto value = *reinterpret_cast<const int64_t *>(item.get_value().data());
            configVal.setInt(boost::get<int64_t>(value));
            break;
        }
        case oldmeta::cpp2::ConfigType::DOUBLE: {
            auto value = *reinterpret_cast<const double *>(item.get_value().data());
            configVal.setFloat(boost::get<double>(value));
            break;
        }
        case oldmeta::cpp2::ConfigType::BOOL: {
            auto value = *reinterpret_cast<const bool *>(item.get_value().data());
            configVal.setBool(boost::get<bool>(value) ? "True" : "False");
            break;
        }
        case oldmeta::cpp2::ConfigType::STRING: {
            configVal.setStr(boost::get<std::string>(item.get_value()));
            break;
        }
        case oldmeta::cpp2::ConfigType::NESTED: {
            auto value = item.get_value();
            // transform to map value
            conf::Configuration conf;
            auto status = conf.parseFromString(boost::get<std::string>(value));
            if (!status.ok()) {
                LOG(ERROR) << "Parse value: " << value
                           << " failed: " << status;
                return Status::Error("Parse value: %s failed", value.c_str());
            }
            Map map;
            conf.forEachItem(
                    [&map](const folly::StringPiece&confKey, const folly::dynamic &confVal) {
                map.kvs.emplace(confKey, confVal.asString());
            });
            configVal.setMap(std::move(map));
            break;
        }
    }
    auto newVal = MetaServiceUtils::configValue(
            static_cast<cpp2::ConfigMode>(item.get_mode()), configVal);
    NG_LOG_AND_RETURN_IF_ERROR(put(key, newVal));
    return Status::OK();
}

Status MetaDataUpdate::deleteDefault(const folly::StringPiece &key) {
    NG_LOG_AND_RETURN_IF_ERROR(remove(key));
    return Status::OK();
}

Status MetaDataUpdate::convertToNewColumns(const std::vector<oldmeta::cpp2::ColumnDef> &oldCols,
                                           std::vector<cpp2::ColumnDef> &newCols) {
    for (auto &colDef : oldCols) {
        cpp2::ColumnDef columnDef;
        columnDef.set_name(colDef.get_name());
        columnDef.set_type(static_cast<cpp2::PropertyType>(colDef.get_type().get_type()));
        if (colDef.__isset.default_value) {
            switch (colDef.get_type().get_type()) {
                case oldmeta::cpp2::SupportedType::BOOL:
                    columnDef.set_default_value(colDef.get_default_value()->get_bool_value());
                    break;
                case oldmeta::cpp2::SupportedType::INT:
                    columnDef.set_default_value(colDef.get_default_value()->get_int_value());
                    break;
                case oldmeta::cpp2::SupportedType::DOUBLE:
                    columnDef.set_default_value(colDef.get_default_value()->get_double_value());
                    break;
                case oldmeta::cpp2::SupportedType::STRING:
                    columnDef.set_default_value(colDef.get_default_value()->get_string_value());
                    break;
                case oldmeta::cpp2::SupportedType::TIMESTAMP:
                    columnDef.set_default_value(colDef.get_default_value()->get_timestamp());
                    break;
                default:
                return Status::Error("Wrong default type: %s",
                        oldmeta::cpp2::_SupportedType_VALUES_TO_NAMES.at(
                                colDef.get_type().get_type()));
            }
        }
        columnDef.set_nullable(true);
        newCols.emplace_back(std::move(columnDef));
    }
    return Status::OK();
}

void MetaDataUpdate::printHosts(const folly::StringPiece &key, const folly::StringPiece &val) {
    auto host = oldmeta::MetaServiceUtils::parseHostKey(key);
    auto info = HostInfo::decodeV1(val);
    LOG(INFO) << "Host ip: " << network::NetworkUtils::intToIPv4(host.ip);
    LOG(INFO) << "Host port: " << host.port;
    LOG(INFO) << "Host info: lastHBTimeInMilliSec: " << info.lastHBTimeInMilliSec_;
    LOG(INFO) << "Host info: role_: " << cpp2::_HostRole_VALUES_TO_NAMES.at(info.role_);
    LOG(INFO) << "Host info: gitInfoSha_: " << info.gitInfoSha_;
}

void MetaDataUpdate::printSpaces(const folly::StringPiece &val) {
    auto oldProps = oldmeta::MetaServiceUtils::parseSpace(val);
    LOG(INFO) << "Space name: " << oldProps.get_space_name();
    LOG(INFO) << "Partition num: " << oldProps.get_partition_num();
    LOG(INFO) << "Replica factor: " << oldProps.get_replica_factor();
    LOG(INFO) << "Charset name: " << oldProps.get_charset_name();
    LOG(INFO) << "Collate name: " << oldProps.get_collate_name();
}

void MetaDataUpdate::printParts(const folly::StringPiece &key, const folly::StringPiece &val) {
    auto spaceId = oldmeta::MetaServiceUtils::parsePartKeySpaceId(key);
    auto partId = oldmeta::MetaServiceUtils::parsePartKeyPartId(key);
    auto oldHosts = oldmeta::MetaServiceUtils::parsePartVal(val);
    LOG(INFO) << "Part spaceId: " << spaceId;
    LOG(INFO) << "Part      id: " << partId;
    for (auto &host : oldHosts) {
        LOG(INFO) << "Part host   ip: " << network::NetworkUtils::intToIPv4(host.ip);
        LOG(INFO) << "Part host port: " << host.port;
    }
}

void MetaDataUpdate::printLeaders(const folly::StringPiece &key) {
    auto host = oldmeta::MetaServiceUtils::parseLeaderKey(key);
    LOG(INFO) << "Leader host ip: " << network::NetworkUtils::intToIPv4(host.ip);
    LOG(INFO) << "Leader host port: " << host.port;
}

void MetaDataUpdate::printSchemas(const folly::StringPiece &val) {
    auto oldSchema = oldmeta::MetaServiceUtils::parseSchema(val);
    auto nameLen = *reinterpret_cast<const int32_t *>(val.data());
    auto schemaName = val.subpiece(sizeof(int32_t), nameLen).str();
    LOG(INFO) << "Schema name: " << schemaName;
    for (auto &colDef : oldSchema.get_columns()) {
        LOG(INFO) << "Schema column name: " << colDef.get_name();
        LOG(INFO) << "Schema column type: "
                  << oldmeta::cpp2::_SupportedType_VALUES_TO_NAMES.at(
                          colDef.get_type().get_type());
        Value defaultValue;
        if (colDef.__isset.default_value) {
            switch (colDef.get_type().get_type()) {
                case oldmeta::cpp2::SupportedType::BOOL:
                    defaultValue = colDef.get_default_value()->get_bool_value();
                    break;
                case oldmeta::cpp2::SupportedType::INT:
                    defaultValue = colDef.get_default_value()->get_int_value();
                    break;
                case oldmeta::cpp2::SupportedType::DOUBLE:
                    defaultValue = colDef.get_default_value()->get_double_value();
                    break;
                case oldmeta::cpp2::SupportedType::STRING:
                    defaultValue = colDef.get_default_value()->get_string_value();
                    break;
                case oldmeta::cpp2::SupportedType::TIMESTAMP:
                    defaultValue = colDef.get_default_value()->get_timestamp();
                    break;
                default:
                    LOG(ERROR) << "Wrong default type: "
                               << oldmeta::cpp2::_SupportedType_VALUES_TO_NAMES.at(
                                       colDef.get_type().get_type());
            }
            LOG(INFO) << "Schema default value: " << defaultValue;
        }
    }
}

void MetaDataUpdate::printIndexes(const folly::StringPiece &val) {
    auto oldItem = oldmeta::MetaServiceUtils::parseIndex(val);
    LOG(INFO) << "Index   id: " << oldItem.get_index_id();
    LOG(INFO) << "Index name: " << oldItem.get_index_name();
    for (auto &colDef : oldItem.get_fields()) {
        LOG(INFO) << "Index field name: " << colDef.get_name();
        LOG(INFO) << "Index field type: "
                  << oldmeta::cpp2::_SupportedType_VALUES_TO_NAMES.at(
                          colDef.get_type().get_type());
    }
}

void MetaDataUpdate::printConfigs(const folly::StringPiece &key, const folly::StringPiece &val) {
    auto item = oldmeta::MetaServiceUtils::parseConfigValue(val);
    auto configName = oldmeta::MetaServiceUtils::parseConfigKey(key);
    Value configVal;
    switch (item.get_type()) {
        case oldmeta::cpp2::ConfigType::INT64: {
            auto value = *reinterpret_cast<const int64_t *>(item.get_value().data());
            configVal.setInt(boost::get<int64_t>(value));
            break;
        }
        case oldmeta::cpp2::ConfigType::DOUBLE: {
            auto value = *reinterpret_cast<const double *>(item.get_value().data());
            configVal.setFloat(boost::get<double>(value));
            break;
        }
        case oldmeta::cpp2::ConfigType::BOOL: {
            auto value = *reinterpret_cast<const bool *>(item.get_value().data());
            configVal.setBool(boost::get<bool>(value) ? "True" : "False");
            break;
        }
        case oldmeta::cpp2::ConfigType::STRING: {
            configVal.setStr(boost::get<std::string>(item.get_value()));
            break;
        }
        case oldmeta::cpp2::ConfigType::NESTED: {
            auto value = item.get_value();
            // transform to map value
            conf::Configuration conf;
            auto status = conf.parseFromString(boost::get<std::string>(value));
            if (!status.ok()) {
                LOG(ERROR) << "Parse value: " << value
                           << " failed: " << status;
                return;
            }
            Map map;
            conf.forEachItem(
                    [&map](const folly::StringPiece &confKey, const folly::dynamic &confVal) {
                map.kvs.emplace(confKey, confVal.asString());
            });
            configVal.setMap(std::move(map));
            break;
        }
    }
    LOG(INFO) << "Config   name: " << configName.second;
    LOG(INFO) << "Config module: "
              << oldmeta::cpp2::_ConfigModule_VALUES_TO_NAMES.at(configName.first);
    LOG(INFO) << "Config   mode: "
              << oldmeta::cpp2::_ConfigMode_VALUES_TO_NAMES.at(item.get_mode());
    LOG(INFO) << "Config  value: " << configVal;
}

}  // namespace meta
}  // namespace nebula

