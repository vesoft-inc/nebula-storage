/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef MOCK_ADHOCSCHEMAMANAGER_H_
#define MOCK_ADHOCSCHEMAMANAGER_H_

#include <folly/RWSpinLock.h>
#include "meta/SchemaProviderIf.h"
#include "meta/SchemaManager.h"
#include "meta/NebulaSchemaProvider.h"
#include "clients/meta/MetaClient.h"

namespace nebula {
namespace mock {

// the different version of tag schema, from newest to oldest
using TagSchemas =
    std::unordered_map<TagID, std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>>;
// the different version of edge schema, from newest to oldest
using EdgeSchemas =
    std::unordered_map<EdgeType, std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>>;

class AdHocSchemaManager final : public nebula::meta::SchemaManager {
public:
    AdHocSchemaManager() = default;
    ~AdHocSchemaManager() = default;

    void addTagSchema(GraphSpaceID space,
                      TagID tag,
                      std::shared_ptr<nebula::meta::NebulaSchemaProvider> schema);

    void addEdgeSchema(GraphSpaceID space,
                       EdgeType edge,
                       std::shared_ptr<nebula::meta::NebulaSchemaProvider> schema);

    void removeTagSchema(GraphSpaceID space, TagID tag);

    std::shared_ptr<const nebula::meta::NebulaSchemaProvider>
    getTagSchema(GraphSpaceID space,
                 TagID tag,
                 SchemaVer version = -1) override;


    // Returns a negative number when the schema does not exist
    StatusOr<SchemaVer> getLatestTagSchemaVersion(GraphSpaceID space, TagID tag) override;

    std::shared_ptr<const nebula::meta::NebulaSchemaProvider>
    getEdgeSchema(GraphSpaceID space,
                  EdgeType edge,
                  SchemaVer version = -1) override;

    // Returns a negative number when the schema does not exist
    StatusOr<SchemaVer> getLatestEdgeSchemaVersion(GraphSpaceID space, EdgeType edge) override;

    StatusOr<GraphSpaceID> toGraphSpaceID(folly::StringPiece spaceName) override;

    StatusOr<TagID> toTagID(GraphSpaceID space, folly::StringPiece tagName) override;

    StatusOr<std::string> toTagName(GraphSpaceID, TagID tagId) override;

    StatusOr<EdgeType> toEdgeType(GraphSpaceID space, folly::StringPiece typeName) override;

    StatusOr<std::string> toEdgeName(GraphSpaceID space, EdgeType edgeType) override;

    StatusOr<std::vector<std::string>> getAllEdge(GraphSpaceID) override {
        LOG(FATAL) << "Unimplemented";
        return Status::Error("Unimplemented");
    }

    StatusOr<int32_t> getSpaceVidLen(GraphSpaceID space) override;

    // get all version of all tags
    StatusOr<TagSchemas> getAllVerTagSchema(GraphSpaceID space) override;

    // get all version of all tags
    StatusOr<EdgeSchemas> getAllVerEdgeSchema(GraphSpaceID space) override;

    // mock previous version of get schema from cache in MetaClient, only used of benchmark
    std::shared_ptr<const nebula::meta::NebulaSchemaProvider>
    getTagSchemaFromMap(GraphSpaceID space,
                        TagID tag,
                        SchemaVer ver);

    // mock previous version of get schema from cache in MetaClient, only used of benchmark
    std::shared_ptr<const nebula::meta::NebulaSchemaProvider>
    getEdgeSchemaFromMap(GraphSpaceID space,
                         EdgeType edge,
                         SchemaVer ver);

protected:
    folly::RWSpinLock tagLock_;

    // all version of the same tag schema is stored in a vector
    std::unordered_map<GraphSpaceID, TagSchemas> tagSchemasInVector_;

    folly::RWSpinLock edgeLock_;

    // all version of the same edge schema is stored in a vector
    std::unordered_map<GraphSpaceID, EdgeSchemas> edgeSchemasInVector_;

    folly::RWSpinLock spaceLock_;
    std::set<GraphSpaceID> spaces_;
    // Key: spaceId + tagName,  Val: tagId
    std::unordered_map<std::string, TagID> tagNameToId_;

private:
    // all version of the same tag schema is stored in map, same as previous MetaClient cache
    // only used for benchmark comparison
    std::unordered_map<GraphSpaceID,
        std::unordered_map<std::pair<TagID, SchemaVer>,
            std::shared_ptr<const nebula::meta::NebulaSchemaProvider>>> tagSchemasInMap_;

    // all version of the same edge schema is stored in map, same as previous MetaClient cache
    // only used for benchmark comparison
    std::unordered_map<GraphSpaceID,
        std::unordered_map<std::pair<EdgeType, SchemaVer>,
            std::shared_ptr<const nebula::meta::NebulaSchemaProvider>>> edgeSchemasInMap_;
};

}  // namespace mock
}  // namespace nebula
#endif  // MOCK_ADHOCSCHEMAMANAGER_H_
