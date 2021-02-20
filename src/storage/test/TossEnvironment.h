/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "TossTestUtils.h"
#include "common/meta/ServerBasedSchemaManager.h"

#define FLOG_FMT(...) LOG(INFO) << folly::sformat(__VA_ARGS__)

DECLARE_int32(heartbeat_interval_secs);
class TossEnvironment;

namespace nebula {
namespace storage {

using StorageClient = storage::GraphStorageClient;

struct TossEnvironment {
public:
    static TossEnvironment* getInstance() {
        static TossEnvironment inst;
        return &inst;
    }

    TossEnvironment() {
        executor_ = std::make_shared<folly::IOThreadPoolExecutor>(20);
    }

    bool connectToMeta(const std::string& metaName, int32_t metaPort) {
        mClient_ = setupMetaClient(metaName, metaPort);
        sClient_ = std::make_unique<StorageClient>(executor_, mClient_.get());
        iClient_ = std::make_unique<storage::InternalStorageClient>(executor_, mClient_.get());
        schemaMan_ = meta::ServerBasedSchemaManager::create(mClient_.get());
        return !!mClient_;
    }

    std::unique_ptr<meta::MetaClient>
    setupMetaClient(const std::string& metaName, uint32_t metaPort) {
        std::vector<HostAddr> metas;
        metas.emplace_back(HostAddr(metaName, metaPort));
        meta::MetaClientOptions options;
        auto client = std::make_unique<meta::MetaClient>(executor_, metas, options);
        if (!client->waitForMetadReady()) {
            LOG(FATAL) << "!client->waitForMetadReady()";
        }
        return client;
    }

    int createSpace(const std::string& spaceName, int nPart, int nReplica) {
        LOG(INFO) << "TossEnvironment::createSpace()";
        if (spaceId_ != 0) {
            return spaceId_;
        }
        auto fDropSpace = mClient_->dropSpace(spaceName, true);
        fDropSpace.wait();
        LOG(INFO) << "drop space " << spaceName;

        meta::cpp2::SpaceDesc spaceDesc;
        spaceDesc.set_space_name(spaceName);
        spaceDesc.set_partition_num(nPart);
        spaceDesc.set_replica_factor(nReplica);
        meta::cpp2::ColumnTypeDef colType;
        colType.set_type(meta::cpp2::PropertyType::INT64);
        spaceDesc.set_vid_type(colType);
        spaceDesc.set_isolation_level(meta::cpp2::IsolationLevel::TOSS);

        auto fCreateSpace = mClient_->createSpace(spaceDesc, true);
        fCreateSpace.wait();
        if (!fCreateSpace.valid()) {
            LOG(FATAL) << "!fCreateSpace.valid()";
        }
        if (!fCreateSpace.value().ok()) {
            LOG(FATAL) << "!fCreateSpace.value().ok(): "
                       << fCreateSpace.value().status().toString();
        }
        spaceId_ = fCreateSpace.value().value();
        LOG(INFO) << folly::sformat("spaceId_ = {}", spaceId_);

        return spaceId_;
    }

    EdgeType setupEdgeSchema(const std::string& edgeName,
                             std::vector<meta::cpp2::ColumnDef> columns) {
        meta::cpp2::Schema schema;
        schema.set_columns(std::move(columns));

        auto fCreateEdgeSchema = mClient_->createEdgeSchema(spaceId_, edgeName, schema, true);
        fCreateEdgeSchema.wait();

        if (!fCreateEdgeSchema.valid() || !fCreateEdgeSchema.value().ok()) {
            LOG(FATAL) << "createEdgeSchema failed";
        }
        edgeType_ = fCreateEdgeSchema.value().value();
        return edgeType_;
    }

    void waitLeaderCollection() {
        int sleepSecs = FLAGS_heartbeat_interval_secs + 2;
        while (sleepSecs) {
            LOG(INFO) << "sleep for " << sleepSecs-- << " sec";
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        auto stVIdLen = mClient_->getSpaceVidLen(spaceId_);
        LOG_IF(FATAL, !stVIdLen.ok());
        vIdLen_ = stVIdLen.value();

        bool leaderLoaded = false;
        while (!leaderLoaded) {
            auto statusOrLeaderMap = mClient_->loadLeader();
            if (!statusOrLeaderMap.ok()) {
                LOG(FATAL) << "mClient_->loadLeader() failed!!!!!!";
            }

            std::this_thread::sleep_for(std::chrono::seconds(1));
            for (auto& leader : statusOrLeaderMap.value()) {
                LOG(INFO) << "spaceId=" << leader.first.first
                            << ", part=" << leader.first.second
                            << ", host=" << leader.second;
                if (leader.first.first == spaceId_) {
                    leaderLoaded = true;
                }
            }
        }
    }

    int32_t getSpaceId() { return spaceId_; }

    EdgeType getEdgeType() { return edgeType_; }

    int32_t getPartId(const std::string& src) {
        auto stPart = mClient_->partId(spaceId_, src);
        LOG_IF(FATAL, !stPart.ok()) << "mClient_->partId() failed";
        return stPart.value();
    }

    std::string strEdgeKey(const cpp2::EdgeKey& e) {
        auto edgeKey = TossTestUtils::toVidKey(e);
        auto partId = getPartId(edgeKey.src.getStr());
        return TransactionUtils::edgeKey(vIdLen_, partId, edgeKey);
    }

    std::string strOutEdgeKey(const cpp2::EdgeKey& e) {
        return strEdgeKey(e);
    }

    std::string strInEdgeKey(const cpp2::EdgeKey& e) {
        CHECK_GT(e.edge_type, 0);
        return strEdgeKey(reverseEdgeKey(e));
    }

    std::string strLockKey(const cpp2::EdgeKey& e) {
        CHECK_GT(e.edge_type, 0);
        auto key = strInEdgeKey(e);
        return NebulaKeyUtils::toLockKey(key);
    }

    cpp2::EdgeKey reverseEdgeKey(const cpp2::EdgeKey& key) {
        cpp2::EdgeKey ret(key);
        std::swap(ret.src, ret.dst);
        ret.edge_type = 0 - ret.edge_type;
        return ret;
    }

    std::string encodeProps(const cpp2::NewEdge& e) {
        auto edgeType = e.key.get_edge_type();
        auto pSchema = schemaMan_->getEdgeSchema(spaceId_, std::abs(edgeType)).get();
        LOG_IF(FATAL, !pSchema) << "Space " << spaceId_ << ", Edge " << edgeType << " invalid";
        auto propNames = TossTestUtils::makeColNames(e.props.size());
        return encodeRowVal(pSchema, propNames, e.props);
    }

    /**
     * @brief insert a lock according to the given edge e.
     *        also insert reverse edge
     * @return lockKey
     */
    std::string insertLock(const cpp2::NewEdge& e) {
        CHECK_GT(e.key.edge_type, 0);
        auto key = strLockKey(e.key);
        auto val = encodeProps(e);
        int32_t part = NebulaKeyUtils::getPart(key);
        return putValue(key, val, part);
    }

    std::string insertInvalidLock(const cpp2::NewEdge& e) {
        CHECK_GT(e.key.edge_type, 0);
        return insertLock(e);
    }

    std::string insertValidLock(const cpp2::NewEdge& e) {
        CHECK_GT(e.key.edge_type, 0);
        insertOutEdge(e);
        return insertLock(e);
    }

    std::string insertOutEdge(const cpp2::NewEdge& e) {
        CHECK_GT(e.key.edge_type, 0);
        auto key = strOutEdgeKey(e.key);
        auto val = encodeProps(e);
        auto part = NebulaKeyUtils::getPart(key);
        return putValue(key, val, part);
    }

    std::string insertInEdge(const cpp2::NewEdge& e) {
        CHECK_GT(e.key.edge_type, 0);
        auto key = strInEdgeKey(e.key);
        auto val = encodeProps(e);
        auto part = NebulaKeyUtils::getPart(key);
        return putValue(key, val, part);
    }

    std::string insertBiEdge(const cpp2::NewEdge& e) {
        insertInEdge(e);
        return insertOutEdge(e);
    }

    std::string putValue(std::string key, std::string val, int32_t partId) {
        LOG(INFO) << "put value, partId=" << partId << ", key=" << folly::hexlify(key);
        kvstore::BatchHolder bat;
        // bat.put(std::move(key), std::move(val));
        bat.put(std::string(key), std::move(val));
        auto batch = encodeBatchValue(bat.getBatch());

        auto txnId = 0;
        auto sf = iClient_->forwardTransaction(txnId, spaceId_, partId, std::move(batch));
        sf.wait();

        if (sf.value() != cpp2::ErrorCode::SUCCEEDED) {
            LOG(FATAL) << "forward txn return=" << static_cast<int>(sf.value());
        }
        return key;
    }

    bool outEdgeExist(const cpp2::NewEdge& e) {
        LOG(INFO) << "check outEdgeExist: " << folly::hexlify(strOutEdgeKey(e.key));
        CHECK_GT(e.key.edge_type, 0);
        return keyExist(strOutEdgeKey(e.key));
    }

    bool inEdgeExist(const cpp2::NewEdge& e) {
        LOG(INFO) << "check inEdgeExist: " << folly::hexlify(strInEdgeKey(e.key));
        CHECK_GT(e.key.edge_type, 0);
        return keyExist(strInEdgeKey(e.key));
    }

    bool lockExist(const cpp2::NewEdge& e) {
        LOG(INFO) << "check lockExist: " << folly::hexlify(strLockKey(e.key));
        CHECK_GT(e.key.edge_type, 0);
        return keyExist(strLockKey(e.key));
    }

    bool keyExist(folly::StringPiece key) {
        // LOG(INFO) << "check key exist: " << folly::hexlify(key);
        auto sf = iClient_->getValue(vIdLen_, spaceId_, key);
        sf.wait();
        if (!sf.hasValue()) {
            LOG(FATAL) << "iClient_->getValue has no value";
            return false;
        }
        return nebula::ok(sf.value());
    }

    // simple copy of Storage::BaseProcessor::encodeRowVal
    std::string encodeRowVal(const meta::NebulaSchemaProvider* schema,
                             const std::vector<std::string>& propNames,
                             const std::vector<Value>& props) {
        RowWriterV2 rowWrite(schema);
        WriteResult wRet;
        if (!propNames.empty()) {
            for (size_t i = 0; i < propNames.size(); i++) {
                wRet = rowWrite.setValue(propNames[i], props[i]);
                if (wRet != WriteResult::SUCCEEDED) {
                    LOG(FATAL) << "Add field faild";
                }
            }
        } else {
            for (size_t i = 0; i < props.size(); i++) {
                wRet = rowWrite.setValue(i, props[i]);
                if (wRet != WriteResult::SUCCEEDED) {
                    LOG(FATAL) << "Add field faild";
                }
            }
        }
        wRet = rowWrite.finish();
        if (wRet != WriteResult::SUCCEEDED) {
            LOG(FATAL) << "Add field faild";
        }

        return std::move(rowWrite).moveEncodedStr();
    }

public:
    std::shared_ptr<folly::IOThreadPoolExecutor>        executor_;
    std::unique_ptr<meta::MetaClient>                   mClient_;
    std::unique_ptr<StorageClient>                      sClient_;
    std::unique_ptr<storage::InternalStorageClient>     iClient_;
    std::unique_ptr<meta::SchemaManager>                schemaMan_;

    int32_t                                             spaceId_{0};
    int32_t                                             edgeType_{0};
    int32_t                                             vIdLen_{0};
};

template <typename Response>
class StorageResponseReader {
public:
    explicit StorageResponseReader(StorageRpcResponse<Response>& resp) : resp_(&resp) {}

    bool isLeaderChange() {
        auto& c = resp_->failedParts();
        return std::any_of(c.begin(), c.end(), [](auto it){
            return it.second == cpp2::ErrorCode::E_LEADER_CHANGED;
        });
    }

    std::vector<Response>& data() {
        return resp_->responses();
    }

private:
    StorageRpcResponse<Response>* resp_;
};

class GetNeighborsExecutor {
    std::vector<std::string> data_;

public:
    explicit GetNeighborsExecutor(cpp2::NewEdge edge)
        : GetNeighborsExecutor(std::vector<cpp2::NewEdge>{edge}) {
    }

    GetNeighborsExecutor(std::vector<cpp2::NewEdge> edges,
                         int64_t limit = std::numeric_limits<int64_t>::max())
        : edges_(std::move(edges)), limit_(limit) {
        env_ = TossEnvironment::getInstance();
        auto uniEdges = uniqueEdges(edges_);
        data_ = run(uniEdges);
    }

    std::vector<std::string> data() {
        return data_;
    }

    std::vector<cpp2::NewEdge> uniqueEdges(const std::vector<cpp2::NewEdge>& __edges) {
        std::vector<cpp2::NewEdge> edges(__edges);
        std::sort(edges.begin(), edges.end(), [](const auto& a, const auto& b) {
            if (a.key.src == b.key.src) {
                return a.key.dst < b.key.dst;
            }
            return a.key.src < b.key.src;
        });
        auto last = std::unique(edges.begin(), edges.end(), [](const auto& a, const auto& b) {
            return a.key.src == b.key.src && a.key.dst == b.key.dst;
        });
        edges.erase(last, edges.end());
        return edges;
    }

    /**
     * @brief Get the Nei Props object,
     *        will unique same src & dst input edges.
     */
    std::vector<std::string> run(const std::vector<cpp2::NewEdge>& edges) {
        bool retLeaderChange = false;
        LOG(INFO) << "edges.size()=" << edges.size() << ", edges.size()=" << edges.size();
        do {
            auto f = rpc(edges);
            f.wait();
            if (!f.valid()) {
                LOG(ERROR) << "!f.valid()";
                break;
            }
            if (f.value().succeeded()) {
                return readStorageResp(f.value());
            } else {
                LOG(ERROR) << "!f.value().succeeded()";
            }
            auto parts = f.value().failedParts();
            for (auto& part : parts) {
                if (part.second == cpp2::ErrorCode::E_LEADER_CHANGED) {
                    retLeaderChange = true;
                    break;
                }
            }
        } while (retLeaderChange);

        LOG(ERROR) << "getOutNeighborsProps failed";
        std::vector<std::string> ret;
        return ret;
    }

    folly::SemiFuture<StorageRpcResponse<cpp2::GetNeighborsResponse>> rpc(
        const std::vector<cpp2::NewEdge>& edges) {
        // para3
        std::vector<Row> vertices;
        std::set<Value> vids;
        for (auto& e : edges) {
            // vids.insert(e.key.src);
            vids.insert(e.key.dst);
        }
        for (auto& vid : vids) {
            Row row;
            row.emplace_back(vid);
            vertices.emplace_back(row);
        }
        // para 4
        std::vector<EdgeType> edgeTypes;
        // para 5
        cpp2::EdgeDirection edgeDirection = cpp2::EdgeDirection::BOTH;
        // para 6
        std::vector<cpp2::StatProp>* statProps = nullptr;
        // para 7
        std::vector<cpp2::VertexProp>* vertexProps = nullptr;
        // para 8
        // const std::vector<cpp2::EdgeProp> edgeProps(1);
        std::vector<cpp2::EdgeProp> edgeProps(1);
        edgeProps.back().type = 0 - edges[0].key.edge_type;
        // para 9
        const std::vector<cpp2::Expr>* expressions = nullptr;
        // para 10
        bool dedup = false;
        // para 11
        bool random = false;
        // para 12
        const std::vector<cpp2::OrderBy> orderBy = std::vector<cpp2::OrderBy>();

        auto colNames = TossTestUtils::makeColNames(edges.back().props.size());

        return env_->sClient_->getNeighbors(env_->spaceId_,
                                            colNames,
                                            vertices,
                                            edgeTypes,
                                            edgeDirection,
                                            statProps,
                                            vertexProps,
                                            &edgeProps,
                                            expressions,
                                            dedup,
                                            random,
                                            orderBy,
                                            limit_);
    }

    std::vector<std::string> readStorageResp(StorageRpcResponse<cpp2::GetNeighborsResponse>& rpc) {
        std::vector<std::string> ret;
        LOG(INFO) << "rpc.responses().size()=" << rpc.responses().size();
        for (auto& resp : rpc.responses()) {
            auto sub = readGetNeighborsResp(resp);
            ret.insert(ret.end(), sub.begin(), sub.end());
        }
        return ret;
    }

    std::vector<std::string> readGetNeighborsResp(cpp2::GetNeighborsResponse& resp) {
        std::vector<std::string> ret;
        auto& ds = resp.vertices;
        LOG(INFO) << "ds.rows.size()=" << ds.rows.size();
        for (auto& row : ds.rows) {
            LOG(INFO) << "row.values.size()=" << row.values.size();
            for (auto& val : row.values) {
                LOG(INFO) << "row.val = " << val.toString();
            }
            ret.emplace_back(row.values[3].toString());
        }
        return ret;
    }

private:
    std::vector<cpp2::NewEdge> edges_;
    int64_t limit_{0};
    TossEnvironment* env_;
};

struct AddEdgeExecutor {
    TossEnvironment* env_;
    std::vector<cpp2::NewEdge> edges_;
    cpp2::ErrorCode code_;

public:
    explicit AddEdgeExecutor(cpp2::NewEdge edge) {
        env_ = TossEnvironment::getInstance();
        CHECK_GT(edge.key.edge_type, 0);
        CHECK_EQ(edge.key.src.type(), nebula::Value::Type::INT);
        LOG(INFO) << "src.type() = " << static_cast<int>(edge.key.src.type());
        edges_.push_back(edge);
        std::swap(edges_.back().key.src, edges_.back().key.dst);
        edges_.back().key.edge_type = 0 - edges_.back().key.edge_type;
        LOG(INFO) << "src.type() = " << static_cast<int>(edges_.back().key.src.type());
        CHECK_EQ(edges_.back().key.src.type(), nebula::Value::Type::INT);
        rpc();
    }

    void rpc() {
        auto propNames = TossTestUtils::makeColNames(edges_.back().props.size());
        bool overwritable = true;
        folly::EventBase* evb = nullptr;
        bool useToss = true;
        int retry = 10;
        do {
            LOG(INFO) << "AddEdgeExecutor::rpc(), do retry = " << retry;
            auto f = env_->sClient_->addEdges(
                env_->spaceId_, edges_, propNames, overwritable, evb, useToss);
            f.wait();
            CHECK(f.hasValue());
            StorageResponseReader<cpp2::ExecResponse> reader(f.value());
            if (reader.isLeaderChange()) {
                continue;
            }
            break;
        } while (retry-- > 0);
    }
    // cpp2::ErrorCode syncAddMultiEdges(std::vector<cpp2::NewEdge>& edges, bool useToss) {
    //     bool retLeaderChange = false;
    //     int32_t retry = 0;
    //     int32_t retryMax = 10;
    //     do {
    //         if (retry > 0 && retry != retryMax) {
    //             LOG(INFO) << "\n leader changed, retry=" << retry;
    //             std::this_thread::sleep_for(std::chrono::milliseconds(500));
    //             retLeaderChange = false;
    //         }
    //         auto f = addEdgesAsync(edges, useToss);
    //         f.wait();
    //         if (!f.valid()) {
    //             LOG(INFO) << cpp2::_ErrorCode_VALUES_TO_NAMES.at(cpp2::ErrorCode::E_UNKNOWN);
    //             return cpp2::ErrorCode::E_UNKNOWN;
    //         }
    //         if (!f.value().succeeded()) {
    //             LOG(INFO) << "addEdgeAsync() !f.value().succeeded()";
    //             LOG(INFO) << "f.value().failedParts().size()=" << f.value().failedParts().size();
    //             for (auto& part : f.value().failedParts()) {
    //                 LOG(INFO) << "partId=" << part.first
    //                           << ", ec=" << cpp2::_ErrorCode_VALUES_TO_NAMES.at(part.second);
    //                 if (part.second == cpp2::ErrorCode::E_LEADER_CHANGED) {
    //                     retLeaderChange = true;
    //                 }
    //             }
    //         }

    //         std::vector<cpp2::ExecResponse>& execResps = f.value().responses();
    //         for (auto& execResp : execResps) {
    //             // ResponseCommon
    //             auto& respComn = execResp.result;
    //             auto& failedParts = respComn.get_failed_parts();
    //             for (auto& part : failedParts) {
    //                 if (part.code == cpp2::ErrorCode::E_LEADER_CHANGED) {
    //                     retLeaderChange = true;
    //                     LOG(INFO) << "addEdgeAsync() !f.value().succeeded(), retry";
    //                 }
    //             }
    //         }

    //         if (++retry == retryMax) {
    //             break;
    //         }
    //     } while (retLeaderChange);
    //     LOG(INFO) << "addEdgeAsync() succeeded";
    //     return cpp2::ErrorCode::SUCCEEDED;
    // }

    // cpp2::ErrorCode syncAddEdge(const cpp2::NewEdge& edge, bool useToss = true) {
    //     std::vector<cpp2::NewEdge> edges{edge};
    //     return syncAddMultiEdges(edges, useToss);
    // }

    // folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>>
    // addEdgesAsync(const std::vector<cpp2::NewEdge>& edges, bool useToss = true) {
    //     auto propNames = TossTestUtils::makeColNames(edges.back().props.size());
    //     return sClient_->addEdges(spaceId_, edges, propNames, true, nullptr, useToss);
    // }
};

struct GetPropsExecutor {
    TossEnvironment* env_;
    cpp2::NewEdge edge_;
    std::vector<Value> result_;

public:
    explicit GetPropsExecutor(cpp2::NewEdge edge) : edge_(std::move(edge)) {
        env_ = TossEnvironment::getInstance();
        CHECK_GT(edge_.key.edge_type, 0);
        std::swap(edge_.key.src, edge_.key.dst);
        edge_.key.edge_type = 0 - edge_.key.edge_type;
        rpc();
    }

    explicit GetPropsExecutor(std::vector<cpp2::NewEdge> edges) : GetPropsExecutor(edges[0]) {}

    std::vector<Value> data() {
        return result_;
    }

    void rpc() {
        nebula::Row row;
        CHECK_EQ(Value::Type::INT, edge_.key.src.type());
        row.values.emplace_back(edge_.key.src);
        row.values.emplace_back(edge_.key.edge_type);
        row.values.emplace_back(edge_.key.ranking);
        CHECK_EQ(Value::Type::INT, edge_.key.dst.type());
        row.values.emplace_back(edge_.key.dst);

        nebula::DataSet ds;
        ds.rows.emplace_back(std::move(row));

        std::vector<cpp2::EdgeProp> props(1);
        props.back().type = edge_.key.edge_type;

        int retry = 10;

        CHECK_EQ(Value::Type::INT, ds.rows[0].values[0].type());
        CHECK_EQ(Value::Type::INT, ds.rows[0].values[3].type());
        do {
            LOG(INFO) << "enter do{} while";
            auto frpc = env_->sClient_
                            ->getProps(env_->spaceId_,
                                       ds, /*DataSet*/
                                       nullptr,       /*vector<cpp2::VertexProp>*/
                                       &props,        /*vector<cpp2::EdgeProp>*/
                                       nullptr        /*expressions*/)
                            .via(env_->executor_.get());
            frpc.wait();
            CHECK(frpc.hasValue());
            StorageResponseReader<cpp2::GetPropResponse> reader(frpc.value());
            if (reader.isLeaderChange()) {
                continue;
            }

            auto resps = reader.data();
            CHECK_EQ(resps.size(), 1);
            cpp2::GetPropResponse& propResp = resps.front();
            cpp2::ResponseCommon result = propResp.get_result();
            nebula::DataSet& dataSet = propResp.props;
            std::vector<Row>& rows = dataSet.rows;
            if (rows.empty()) {
                LOG(FATAL) << "getProps() dataSet.rows.empty())";
            }
            LOG(INFO) << "rows.size() = " << rows.size();
            for (auto& r : rows) {
                LOG(INFO) << "values.size() = " << r.values.size();
                for (auto& val : r.values) {
                    LOG(INFO) << "val: " << val.toString();
                }
            }
            result_ = rows[0].values;
            if (result_.empty()) {
                LOG(FATAL) << "getProps() ret.empty())";
            }
            break;
        } while (retry-- > 0);
    }
};

struct UpdateExecutor {
    TossEnvironment* env_;
    cpp2::NewEdge edge_;
    std::vector<cpp2::UpdatedProp> updatedProps_;
    bool insertable_{false};
    std::vector<std::string> returnProps_;
    std::string condition_;

public:
    explicit UpdateExecutor(cpp2::NewEdge edge) : edge_(std::move(edge)) {
        env_ = TossEnvironment::getInstance();
        CHECK_GT(edge_.key.edge_type, 0);
        std::swap(edge_.key.src, edge_.key.dst);
        edge_.key.edge_type = 0 - edge_.key.edge_type;
        prepareParameters();
        rpc();
    }

    void prepareParameters() {
        cpp2::UpdatedProp uProp1;
        uProp1.set_name("c1");
        ConstantExpression val1(edge_.props[0].getInt());
        uProp1.set_value(Expression::encode(val1));
        updatedProps_.emplace_back(uProp1);

        cpp2::UpdatedProp uProp2;
        uProp2.set_name("c2");
        ConstantExpression val2(edge_.props[1].getStr());
        uProp2.set_value(Expression::encode(val2));

        updatedProps_.emplace_back(uProp2);
    }

    void rpc() {
        for (;;) {
            // folly::Future<StatusOr<storage::cpp2::UpdateResponse>> updateEdge(
                // GraphSpaceID space,
                // storage::cpp2::EdgeKey edgeKey,
                // std::vector<cpp2::UpdatedProp> updatedProps,
                // bool insertable,
                // std::vector<std::string> returnProps,
                // std::string condition,
                // folly::EventBase* evb = nullptr);
            auto f = env_->sClient_->updateEdge(
                env_->spaceId_, edge_.key, updatedProps_, insertable_, returnProps_, condition_);
            f.wait();
            CHECK(f.hasValue());
            if (!f.value().ok()) {
                LOG(FATAL) << f.value().status().toString();
            }
            storage::cpp2::UpdateResponse resp = f.value().value();
            if (resp.result.get_failed_parts().empty()) {
                break;
            }

            CHECK_EQ(resp.result.get_failed_parts().size(), 1U);
            cpp2::PartitionResult result =  resp.result.get_failed_parts().back();
            if (result.code == cpp2::ErrorCode::E_LEADER_CHANGED) {
                LOG(INFO) << "update edge leader changed, retry";
                continue;
            }
            LOG(ERROR) << "update edge err: " << cpp2::_ErrorCode_VALUES_TO_NAMES.at(result.code);
            break;
        }
    }
};

}  // namespace storage
}  // namespace nebula
