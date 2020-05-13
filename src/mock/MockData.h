/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef MOCK_MOCKDATA_H_
#define MOCK_MOCKDATA_H_

#include "base/Base.h"
#include "interface/gen-cpp2/common_types.h"
#include "interface/gen-cpp2/storage_types.h"
#include "meta/NebulaSchemaProvider.h"

DECLARE_bool(mock_ttl_col);
DECLARE_int32(mock_ttl_duration);

namespace nebula {
namespace mock {

struct VertexData {
    VertexID            vId_;
    TagID               tId_;
    std::vector<Value>  props_;
};

struct EdgeData {
    VertexID            srcId_;
    EdgeType            type_;
    EdgeRanking         rank_;
    VertexID            dstId_;
    std::vector<Value>  props_;
};

struct Player {
    std::string         name_;
    int                 age_;
    bool                playing_;
    int                 career_;
    int                 startYear_;
    int                 endYear_;
    int                 games_;
    double              avgScore_;
    int                 serveTeams_;
    std::string         country_{""};
    int                 champions_{0};
};

struct Serve {
    std::string         playerName_;
    std::string         teamName_;
    int                 startYear_;
    int                 endYear_;
    int                 teamCareer_;
    int                 teamGames_;
    double              teamAvgScore_;
    bool                starting_{false};
    int                 champions_{0};;
};

class MockData {
public:
    /*
     * Mock schema
     */
    static std::shared_ptr<meta::NebulaSchemaProvider> mockPlayerTagSchema();

    static std::shared_ptr<meta::NebulaSchemaProvider> mockTeamTagSchema();

    static std::shared_ptr<meta::NebulaSchemaProvider> mockEdgeSchema();

    static std::vector<nebula::meta::cpp2::ColumnDef> mockGeneralTagIndexColumns();

    static std::vector<nebula::meta::cpp2::ColumnDef> mockEdgeIndexColumns();

    static std::shared_ptr<meta::NebulaSchemaProvider> mockGeneralTagSchemaV1();

    static std::shared_ptr<meta::NebulaSchemaProvider> mockGeneralTagSchemaV2();

    /*
     * Mock data
     */
    // Construct data in the order of schema
    static std::vector<VertexData> mockVertices();

    static std::vector<EdgeData> mockEdges();

    static std::vector<VertexID> mockVerticeIds();

    // Only has EdgeKey data, not props
    static std::vector<EdgeData> mockEdgeKeys();

    // Construct data in the specified order
    // For convenience, here is the reverse order
    static std::vector<VertexData> mockVerticesSpecifiedOrder();

    static std::vector<EdgeData> mockEdgesSpecifiedOrder();

    /*
     * Mock request
     */
    static nebula::storage::cpp2::AddVerticesRequest
    mockAddVerticesReq(int32_t parts = 6);

    static nebula::storage::cpp2::AddEdgesRequest
    mockAddEdgesReq(int32_t parts = 6);

    static nebula::storage::cpp2::DeleteVerticesRequest
    mockDeleteVerticesReq(int32_t parts = 6);

    static nebula::storage::cpp2::DeleteEdgesRequest
    mockDeleteEdgesReq(int32_t parts = 6);

    static nebula::storage::cpp2::AddVerticesRequest
    mockAddVerticesSpecifiedOrderReq(int32_t parts = 6);

    static nebula::storage::cpp2::AddEdgesRequest
    mockAddEdgesSpecifiedOrderReq(int32_t parts = 6);

private:
    static std::vector<std::string> teams_;

    static std::vector<Player> players_;

    static std::vector<Serve> serve_;
};

}  // namespace mock
}  // namespace nebula

#endif  // MOCK_MOCKDATA_H_
