/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef TOOLS_DBUPGRADE_DBUPGRADER_H_
#define TOOLS_DBUPGRADE_DBUPGRADER_H_

#include "common/base/Base.h"
#include "common/base/Status.h"
#include "common/clients/meta/MetaClient.h"
#include "common/meta/ServerBasedSchemaManager.h"
#include "common/meta/ServerBasedIndexManager.h"
#include <rocksdb/db.h>
#include "kvstore/RocksEngine.h"
#include "codec/RowReaderWrapper.h"


DECLARE_string(src_db_path);
DECLARE_string(dst_db_path);
DECLARE_string(upgrade_meta_server);
DECLARE_uint32(write_batch_num);
DECLARE_uint32(write_batch_num);
DECLARE_bool(update_v1);

namespace nebula {
namespace storage {

// Upgrade a space of path in storage conf
class UpgraderSpace {
public:
    UpgraderSpace() = default;

    ~UpgraderSpace() = default;

    Status init(meta::MetaClient* mclient,
                meta::ServerBasedSchemaManager*sMan,
                meta::IndexManager* iMan,
                const std::string& srcPath,
                const std::string& dstPath,
                const std::string& entry);

    // Process v1 data and upgrade to v2 Ga
    void doProcessV1();

    // Processing v2 Rc data upgrade to v2 Ga
    void doProcessV2();

private:
    Status initSpace(const std::string& spaceId);

    Status buildSchemaAndIndex();

    bool isValidVidLen(VertexID srcVId, VertexID dstVId = "");

    // Think that the old and new values are exactly the same, so use one reader
    void encodeVertexValue(PartitionID partId,
                           RowReader* reader,
                           const meta::NebulaSchemaProvider* schema,
                           std::string& newkey,
                           VertexID& strVid,
                           TagID   tagId,
                           std::vector<kvstore::KV>& data);

    // Used for vertex and edge
    std::string encodeRowVal(const RowReader* reader,
                             const meta::NebulaSchemaProvider* schema,
                             std::vector<std::string>& fieldName);

    std::string indexVertexKey(PartitionID partId,
                               VertexID& vId,
                               RowReader* reader,
                               std::shared_ptr<nebula::meta::cpp2::IndexItem> index);

    void encodeEdgeValue(PartitionID partId,
                         RowReader* reader,
                         const meta::NebulaSchemaProvider* schema,
                         std::string& newkey,
                         VertexID& svId,
                         EdgeType type,
                         EdgeRanking rank,
                         VertexID& dstId,
                         std::vector<kvstore::KV>& data);

   std::string indexEdgeKey(PartitionID partId,
                            RowReader* reader,
                            VertexID& svId,
                            EdgeRanking rank,
                            VertexID& dstId,
                            std::shared_ptr<nebula::meta::cpp2::IndexItem> index);

private:
    meta::MetaClient*                                              metaClient_;
    meta::ServerBasedSchemaManager*                                schemaMan_;
    meta::IndexManager*                                            indexMan_;
    // Souce data path
    std::string                                                    srcPath_;

    // Destination data path
    std::string                                                    dstPath_;
    std::vector<std::string>                                       subDirs_;

    // The following variables are space level, When switching space, need to change
    GraphSpaceID                                                   spaceId_;
    int32_t                                                        spaceVidLen_;
    std::string                                                    spaceName_;
    std::vector<PartitionID>                                       parts_;
    std::unique_ptr<kvstore::RocksEngine>                          readEngine_;
    std::unique_ptr<kvstore::RocksEngine>                          writeEngine_;

    // Get all tag newest schema in space
    std::unordered_map<TagID,
        std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>>       tagSchemas_;

    // tag all field name in newest schema
    std::unordered_map<TagID, std::vector<std::string>>                       tagFieldName_;

    std::unordered_map<TagID,
        std::unordered_set<std::shared_ptr<nebula::meta::cpp2::IndexItem>>>   tagIndexes_;

    // Get all edge newest schema in space
    std::unordered_map<EdgeType,
        std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>>       edgeSchemas_;

    // tag all field name in newest schema
    std::unordered_map<TagID, std::vector<std::string>>                       edgeFieldName_;

    std::unordered_map<EdgeType,
        std::unordered_set<std::shared_ptr<nebula::meta::cpp2::IndexItem>>>   edgeIndexes_;
};

// Upgrade one path in storage conf
class DbUpgrader {
public:
    DbUpgrader() = default;

    ~DbUpgrader() = default;

    Status init(meta::MetaClient* mclient,
                meta::ServerBasedSchemaManager*sMan,
                meta::IndexManager* iMan,
                const std::string& srcPath,
                const std::string& dstPath);

    void run();

private:
    // Get all string spaceId
    Status listSpace();

    Status buildSchemaAndIndex();

    void doProcessAllTagsAndEdges();


private:
    meta::MetaClient*                                              metaClient_;
    meta::ServerBasedSchemaManager*                                schemaMan_;
    meta::IndexManager*                                            indexMan_;
    // Souce data path
    std::string                                                    srcPath_;

    // Destination data path
    std::string                                                    dstPath_;
    std::vector<std::string>                                       subDirs_;
};

}  // namespace storage
}  // namespace nebula

#endif  // TOOLS_DBUPGRADE_DBUPGRADER_H_
