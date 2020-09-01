/* Copyright (c) 2018 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#ifndef TOOLS_METADATAUPDATETOOL_OLDTHRIFT_METADATAUPDATE_H_
#define TOOLS_METADATAUPDATETOOL_OLDTHRIFT_METADATAUPDATE_H_

#include "common/base/Base.h"
#include "common/base/Status.h"
#include "common/thrift/ThriftTypes.h"
#include "tools/metaDataUpdate/oldThrift/gen-cpp2/old_meta_types.h"
#include "kvstore/Common.h"


namespace nebula {
namespace oldmeta {

enum class EntryType : int8_t {
    SPACE       = 0x01,
    TAG         = 0x02,
    EDGE        = 0x03,
    INDEX       = 0x04,
    CONFIG      = 0x05,
};

using ConfigName = std::pair<cpp2::ConfigModule, std::string>;
using LeaderParts = std::unordered_map<GraphSpaceID, std::vector<PartitionID>>;

class MetaServiceUtilsV1 final {
public:
    MetaServiceUtilsV1() = delete;

    static std::string spaceVal(const cpp2::SpaceProperties &properties);

    static cpp2::SpaceProperties parseSpace(folly::StringPiece rawData);

    static GraphSpaceID parsePartKeySpaceId(folly::StringPiece key);

    static PartitionID parsePartKeyPartId(folly::StringPiece key);

    static std::vector<cpp2::HostAddr> parsePartVal(folly::StringPiece val);

    static cpp2::HostAddr parseHostKey(folly::StringPiece key);

    static cpp2::HostAddr parseLeaderKey(folly::StringPiece key);

    static cpp2::Schema parseSchema(folly::StringPiece rawData);

    static cpp2::IndexItem parseIndex(const folly::StringPiece& rawData);

    static ConfigName parseConfigKey(folly::StringPiece rawData);

    static cpp2::ConfigItem parseConfigValue(folly::StringPiece rawData);
};

}  // namespace oldmeta
}  // namespace nebula
#endif  // TOOLS_METADATAUPDATETOOL_OLDTHRIFT_METADATAUPDATE_H_

