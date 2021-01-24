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



DECLARE_string(upgrade_db_path);
DECLARE_string(upgrade_meta_server);

namespace nebula {
namespace storage {

class DbUpgrader {
public:
    DbUpgrader() = default;

    ~DbUpgrader() = default;

    Status init();

    void run();

private:
    Status initMeta();

    Status listSpace();

    Status initSpace(std::string& spaceId);

    Status buildSchemaAndIndex();

    void doProcessAllTagsAndEdges();

    // Used for vertex and edge
    std::string encodeRowVal(const RowReader* reader,
                             const meta::NebulaSchemaProvider* schema);

    bool isValidVidLen(VertexID srcVId, VertexID dstVId = "");

    // Think that the old and new values are exactly the same, so use one reader
    void encodeVertexValue(PartitionID partId,
                           RowReader* reader,
                           const meta::NebulaSchemaProvider* schema,
                           std::string& newkey,
                           VertexID& strVid,
                           TagID   tagId,
                           std::vector<kvstore::KV>& data);

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
    std::unique_ptr<meta::MetaClient>                              metaClient_;
    std::unique_ptr<meta::ServerBasedSchemaManager>                schemaMan_;
    std::unique_ptr<meta::IndexManager>                            indexMan_;
    std::vector<std::string>                                       subDirs_;

    // The following variables are space level, When switching space, need to change
    GraphSpaceID                                                   spaceId_;
    int32_t                                                        spaceVidLen_;
    std::string                                                    spaceName_;
    std::unordered_set<PartitionID>                                parts_;
    std::unique_ptr<kvstore::RocksEngine>                          engine_;

    // Get all tag in space
    std::unordered_map<TagID,
        std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>>      tagSchemas_;

    std::unordered_map<TagID,
        std::unordered_set<std::shared_ptr<nebula::meta::cpp2::IndexItem>>>  tagIndexes_;

    // Get all edge in space
    std::unordered_map<EdgeType,
        std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>>      edgeSchemas_;

    std::unordered_map<EdgeType,
        std::unordered_set<std::shared_ptr<nebula::meta::cpp2::IndexItem>>>  edgeIndexes_;

};

}  // namespace storage
}  // namespace nebula

#endif  // TOOLS_DBUPGRADE_DBUPGRADER_H_
