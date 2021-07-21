/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "storage/CommonUtils.h"
#include "storage/transaction/ChainResumeProcessor.h"
#include "storage/transaction/ChainUpdateEdgeProcessorLocal.h"

namespace nebula {
namespace storage {

using ECode = ::nebula::cpp2::ErrorCode;

extern const int32_t mockSpaceId;
extern const int32_t mockPartNum;

using KeyGenerator = std::function<std::string(PartitionID partId, const cpp2::NewEdge& edge)>;

class ChainTestUtils {
public:
    ChainTestUtils() {
        genKey = [&](PartitionID partId, const cpp2::NewEdge& edge) {
            auto key = ConsistUtil::edgeKey(spaceVidLen_, partId, edge.get_key());
            return key;
        };
        genPrime = [&](PartitionID partId, const cpp2::NewEdge& edge) {
            auto key = ConsistUtil::edgeKeyPrime(spaceVidLen_, partId, edge.get_key());
            return key;
        };
        genDoublePrime = [&](PartitionID partId, const cpp2::NewEdge& edge) {
            auto key = ConsistUtil::doublePrime(spaceVidLen_, partId, edge.get_key());
            return key;
        };
    }
public:
    int32_t spaceVidLen_{32};
    KeyGenerator genKey;
    KeyGenerator genPrime;
    KeyGenerator genDoublePrime;
};

// , StorageEnv* env
int numOfKey(const cpp2::AddEdgesRequest& req, KeyGenerator gen, StorageEnv* env) {
    int numOfEdges = 0;
    int totalEdge = 0;
    auto spaceId = req.get_space_id();
    for (auto& edgesOfPart : *req.parts_ref()) {
        auto partId = edgesOfPart.first;
        auto& edgeVec = edgesOfPart.second;
        for (auto& edge : edgeVec) {
            ++totalEdge;
            auto key = gen(partId, edge);
            std::unique_ptr<kvstore::KVIterator> iter;
            EXPECT_EQ(ECode::SUCCEEDED, env->kvstore_->prefix(spaceId, partId, key, &iter));
            if (iter && iter->valid()) {
                ++numOfEdges;
            } else {
                // LOG(INFO) << "key: " << key << " not exist";
            }
        }
    }
    LOG(INFO) << "numOfEdges = " << numOfEdges;
    LOG(INFO) << "totalEdge = " << totalEdge;
    return numOfEdges;
}

std::pair<GraphSpaceID, PartitionID> extractSpaceAndPart(const cpp2::AddEdgesRequest& req) {
    auto spaceId = req.get_space_id();
    CHECK_EQ(req.get_parts().size(), 1);
    auto partId = req.get_parts().begin()->first;
    return std::make_pair(spaceId, partId);
}

bool keyExist(StorageEnv* env, GraphSpaceID spaceId, PartitionID partId, std::string key) {
    // std::unique_ptr<kvstore::KVIterator> iter;
    std::string ignoreVal;
    auto rc = env->kvstore_->get(spaceId, partId, key, &ignoreVal);
    return rc == ECode::SUCCEEDED;
}

class FakeChainAddEdgesProcessorLocal : public ChainAddEdgesProcessorLocal {
    FRIEND_TEST(ChainAddEdgesTest, prepareLocalSucceededTest);
    FRIEND_TEST(ChainAddEdgesTest, processRemoteSucceededTest);
    FRIEND_TEST(ChainAddEdgesTest, processRemoteFailedTest);
    FRIEND_TEST(ChainAddEdgesTest, processRemoteOutdatedTest);
    // all the above will test succeeded path of process local
    // the failed path of process local will be tested in resume test
public:
    explicit FakeChainAddEdgesProcessorLocal(StorageEnv* env) : ChainAddEdgesProcessorLocal(env) {
        spaceVidLen_ = 32;
    }

    folly::SemiFuture<ECode> prepareLocal() override {
        LOG(INFO) << "FakeChainAddEdgesProcessorLocal::" << __func__ << "()";
        if (rcPrepareLocal) {
            LOG(INFO) << "Fake return " << apache::thrift::util::enumNameSafe(*rcPrepareLocal);
            return *rcPrepareLocal;
        }
        LOG(INFO) << "forward to ChainAddEdgesProcessorLocal::prepareLocal()";
        return ChainAddEdgesProcessorLocal::prepareLocal();
    }

    folly::SemiFuture<ECode> processRemote(ECode code) override {
        LOG(INFO) << "FakeChainAddEdgesProcessorLocal::" << __func__ << "()";
        if (rcProcessRemote) {
            LOG(INFO) << "processRemote() fake return "
                      << apache::thrift::util::enumNameSafe(*rcProcessRemote);
            LOG_IF(FATAL, code != ECode::SUCCEEDED) << "cheat must base on truth";
            return *rcProcessRemote;
        }
        LOG(INFO) << "forward to ChainAddEdgesProcessorLocal::processRemote()";
        return ChainAddEdgesProcessorLocal::processRemote(code);
    }

    folly::SemiFuture<ECode> processLocal(ECode code) override {
        LOG(INFO) << "FakeChainAddEdgesProcessorLocal::" << __func__ << "()";
        if (rcProcessLocal) {
            LOG(INFO) << "Fake return " << apache::thrift::util::enumNameSafe(*rcProcessLocal);
            return *rcProcessLocal;
        }
        LOG(INFO) << "forward to ChainAddEdgesProcessorLocal::processLocal()";
        return ChainAddEdgesProcessorLocal::processLocal(code);
    }

    cpp2::AddEdgesRequest reverseRequestForward(const cpp2::AddEdgesRequest& req) {
        return ChainAddEdgesProcessorLocal::reverseRequest(req);
    }

    folly::Optional<ECode> rcPrepareLocal;

    folly::Optional<ECode> rcProcessRemote;

    folly::Optional<ECode> rcProcessLocal;
};

class FakeChainUpdateProcessor : public ChainUpdateEdgeProcessorLocal {
public:
    explicit FakeChainUpdateProcessor(StorageEnv* env) : ChainUpdateEdgeProcessorLocal(env) {
        spaceVidLen_ = 32;
    }

    folly::SemiFuture<ECode> prepareLocal() override {
        LOG(INFO) << "FakeChainUpdateEdgeProcessorA::" << __func__ << "()";
        if (rcPrepareLocal) {
            LOG(INFO) << "Fake return " << apache::thrift::util::enumNameSafe(*rcPrepareLocal);
            return *rcPrepareLocal;
        }
        LOG(INFO) << "forward to ChainUpdateEdgeProcessorLocal::prepareLocal()";
        return ChainUpdateEdgeProcessorLocal::prepareLocal();
    }

    folly::SemiFuture<ECode> processRemote(ECode code) override {
        LOG(INFO) << "FakeChainUpdateEdgeProcessorA::" << __func__ << "()";
        if (rcProcessRemote) {
            LOG(INFO) << "processRemote() fake return "
                      << apache::thrift::util::enumNameSafe(*rcProcessRemote);
            LOG_IF(FATAL, code != ECode::SUCCEEDED) << "cheat must base on truth";
            return *rcProcessRemote;
        }
        LOG(INFO) << "forward to ChainUpdateEdgeProcessorLocal::processRemote()";
        return ChainUpdateEdgeProcessorLocal::processRemote(code);
    }

    folly::SemiFuture<ECode> processLocal(ECode code) override {
        LOG(INFO) << "FakeChainUpdateEdgeProcessorA::" << __func__ << "()";
        if (rcProcessLocal) {
            LOG(INFO) << "processLocal() fake return "
                      << apache::thrift::util::enumNameSafe(*rcProcessLocal);
            return *rcProcessLocal;
        }
        LOG(INFO) << "forward to ChainUpdateEdgeProcessorLocal::processLocal()";
        return ChainUpdateEdgeProcessorLocal::processLocal(code);
    }

public:
    folly::Optional<ECode> rcPrepareLocal;
    folly::Optional<ECode> rcProcessRemote;
    folly::Optional<ECode> rcProcessLocal;
};

class MetaClientTestUpdater {
public:
    MetaClientTestUpdater() = default;

    static void addLocalCache(meta::MetaClient& mClient,
                       GraphSpaceID spaceId,
                       std::shared_ptr<meta::SpaceInfoCache> spInfoCache) {
        mClient.localCache_[spaceId] = spInfoCache;
    }

    static meta::SpaceInfoCache* getLocalCache(meta::MetaClient* mClient, GraphSpaceID spaceId) {
        if (mClient->localCache_.count(spaceId) == 0) {
            return nullptr;
        }
        return mClient->localCache_[spaceId].get();
    }

    static std::unique_ptr<meta::MetaClient> makeDefaultMetaClient() {
        auto exec = std::make_shared<folly::IOThreadPoolExecutor>(3);
        std::vector<HostAddr> addrs(1);
        meta::MetaClientOptions options;

        auto mClient = std::make_unique<meta::MetaClient>(exec, addrs, options);
        mClient->localCache_[mockSpaceId] = std::make_shared<meta::SpaceInfoCache>();
        for (int i = 0; i != mockPartNum; ++i) {
            mClient->localCache_[mockSpaceId]->termOfPartition_[i] = i;
            auto ignoreItem = mClient->localCache_[mockSpaceId]->partsAlloc_[i];
            UNUSED(ignoreItem);
        }
        meta::cpp2::ColumnTypeDef type;
        type.set_type(meta::cpp2::PropertyType::FIXED_STRING);
        type.set_type_length(32);

        mClient->localCache_[mockSpaceId]->spaceDesc_.set_vid_type(std::move(type));
        mClient->ready_ = true;
        return mClient;
    }
};

class ChainResumeProcessorTestHelper {
public:
    explicit ChainResumeProcessorTestHelper(ChainResumeProcessor* proc) : proc_(proc) {}

    void setAddEdgeProc(ChainAddEdgesProcessorLocal* proc) {
        proc_->addProc = proc;
    }

    // setUpdProc
    void setUpdProc(ChainUpdateEdgeProcessorLocal* proc) {
        proc_->updProc = proc;
    }

    std::string getTxnId() {
        return proc_->addProc->txnId_;
    }
public:
    ChainResumeProcessor* proc_{nullptr};
};


}  // namespace storage
}  // namespace nebula
