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
    std::shared_ptr<folly::IOThreadPoolExecutor>        executor_;
    std::unique_ptr<meta::MetaClient>                   mClient_;
    std::unique_ptr<StorageClient>                      sClient_;
    std::unique_ptr<storage::InternalStorageClient>     iClient_;
    std::unique_ptr<meta::SchemaManager>                schemaMan_;

    int32_t                                             iSpaceId_{0};
    int32_t                                             iEdgeType_{0};
    int32_t                                             spaceIdS_{0};
    int32_t                                             edgeTypeS_{0};
    int32_t                                             vIdLen_{0};

public:
    static TossEnvironment* getInstance() {
        static TossEnvironment inst;
        return &inst;
    }

    void init(const std::string& spaceName,
              int nPart,
              int nReplica,
              const std::string& edgeName,
              std::vector<meta::cpp2::PropertyType> colTypes) {
        auto colDefs = TossTestUtils::makeColDefs(colTypes);

        auto intVid = meta::cpp2::PropertyType::INT64;
        iSpaceId_ = createSpace(spaceName+"_int", nPart, nReplica, intVid);
        iEdgeType_ = setupEdgeSchema(iSpaceId_, edgeName, colDefs);

        auto stringVid = meta::cpp2::PropertyType::FIXED_STRING;
        spaceIdS_ = createSpace(spaceName+"_str", nPart, nReplica, stringVid);
        edgeTypeS_ = setupEdgeSchema(spaceIdS_, edgeName, colDefs);

        waitLeaderCollection();
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

    int createSpace(const std::string& spaceName,
                    int nPart,
                    int nReplica,
                    meta::cpp2::PropertyType type) {
        LOG(INFO) << "TossEnvironment::createSpace()";
        auto fDropSpace = mClient_->dropSpace(spaceName, true);
        fDropSpace.wait();
        LOG(INFO) << "drop space " << spaceName;

        meta::cpp2::SpaceDesc spaceDesc;
        spaceDesc.set_space_name(spaceName);
        spaceDesc.set_partition_num(nPart);
        spaceDesc.set_replica_factor(nReplica);
        meta::cpp2::ColumnTypeDef colType;
        // colType.set_type(meta::cpp2::PropertyType::INT64);
        colType.set_type(type);
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
        // iSpaceId_ = fCreateSpace.value().value();
        // LOG(INFO) << folly::sformat("iSpaceId_ = {}", iSpaceId_);

        return fCreateSpace.value().value();
    }

    EdgeType setupEdgeSchema(GraphSpaceID spaceId,
                             const std::string& edgeName,
                             std::vector<meta::cpp2::ColumnDef> columns) {
        meta::cpp2::Schema schema;
        schema.set_columns(std::move(columns));

        auto fCreateEdgeSchema = mClient_->createEdgeSchema(spaceId, edgeName, schema, true);
        fCreateEdgeSchema.wait();

        if (!fCreateEdgeSchema.valid() || !fCreateEdgeSchema.value().ok()) {
            LOG(FATAL) << "createEdgeSchema failed";
        }
        return fCreateEdgeSchema.value().value();
    }

    void waitLeaderCollection() {
        int sleepSecs = FLAGS_heartbeat_interval_secs + 2;
        while (sleepSecs) {
            LOG(INFO) << "sleep for " << sleepSecs-- << " sec";
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        auto stVIdLen = mClient_->getSpaceVidLen(iSpaceId_);
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
                if (leader.first.first == iSpaceId_) {
                    leaderLoaded = true;
                }
            }
        }
    }

    int32_t getSpaceIdS() { return spaceIdS_; }

    EdgeType getEdgeType() { return iEdgeType_; }

    EdgeType getEdgeTypeS() { return edgeTypeS_; }

    int32_t getPartId(const std::string& src) {
        auto stPart = mClient_->partId(iSpaceId_, src);
        LOG_IF(FATAL, !stPart.ok()) << "mClient_->partId() failed";
        return stPart.value();
    }

    std::string strEdgeKey(const cpp2::EdgeKey& e) {
        auto edgeKey = TossTestUtils::makeEdgeKeyS(e);
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
        auto pSchema = schemaMan_->getEdgeSchema(iSpaceId_, std::abs(edgeType)).get();
        LOG_IF(FATAL, !pSchema) << "Space " << iSpaceId_ << ", Edge " << edgeType << " invalid";
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
        bat.put(std::string(key), std::move(val));
        auto batch = encodeBatchValue(bat.getBatch());

        auto txnId = 0;
        auto sf = iClient_->forwardTransaction(txnId, iSpaceId_, partId, std::move(batch));
        sf.wait();

        if (sf.value() != cpp2::ErrorCode::SUCCEEDED) {
            LOG(FATAL) << "forward txn return=" << static_cast<int>(sf.value());
        }
        return key;
    }

    bool outEdgeExist(const cpp2::NewEdge& e, GraphSpaceID spaceId = 0) {
        LOG(INFO) << "check outEdgeExist: " << folly::hexlify(strOutEdgeKey(e.key));
        CHECK_GT(e.key.edge_type, 0);
        return keyExist(strOutEdgeKey(e.key), spaceId);
    }

    bool inEdgeExist(const cpp2::NewEdge& e, GraphSpaceID spaceId = 0) {
        LOG(INFO) << "check inEdgeExist: " << folly::hexlify(strInEdgeKey(e.key));
        CHECK_GT(e.key.edge_type, 0);
        return keyExist(strInEdgeKey(e.key), spaceId);
    }

    bool lockExist(const cpp2::NewEdge& e, GraphSpaceID spaceId = 0) {
        LOG(INFO) << "check lockExist: " << folly::hexlify(strLockKey(e.key));
        CHECK_GT(e.key.edge_type, 0);
        return keyExist(strLockKey(e.key), spaceId);
    }

    bool keyExist(folly::StringPiece key, GraphSpaceID spaceId = 0) {
        if (spaceId == 0) {
            spaceId = iSpaceId_;
        }
        auto sf = iClient_->getValue(vIdLen_, spaceId, key);
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
};

}  // namespace storage
}  // namespace nebula
