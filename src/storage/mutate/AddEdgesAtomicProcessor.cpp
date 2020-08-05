/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


#include <algorithm>
#include <folly/container/Enumerate.h>

#include "codec/RowWriterV2.h"
#include "storage/mutate/AddEdgesAtomicProcessor.h"
#include "storage/transaction/TransactionManager.h"
#include "utils/IndexKeyUtils.h"
#include "utils/NebulaKeyUtils.h"
#include "storage/transaction/TransactionUtils.h"

namespace nebula {
namespace storage {

folly::Future<cpp2::ErrorCode>
AddEdgesAtomicProcessor::processPartition(int spaceId,
                                             int partId,
                                             int spaceVidLen,
                                             std::vector<cpp2::NewEdge> edges,
                                             int version) {
    std::vector<folly::SemiFuture<cpp2::ErrorCode>> futures;
    for (auto& edge : edges) {
        auto sf = addSingleEdge(spaceId, partId, spaceVidLen, edge, version);
        futures.emplace_back(std::move(sf));
    }
    return folly::collectAll(futures)
                .via(env_->txnManager_->worker_.get())
                .thenTry([&, partId = partId](auto&& collectedFut) {
                    auto ret = cpp2::ErrorCode::SUCCEEDED;
                    for (auto& t : collectedFut.value()) {
                        if (t.hasException()) {
                            LOG(INFO) << "messi catch exception set err E_TXN_ERR_UNKNOWN";
                            ret = cpp2::ErrorCode::E_TXN_ERR_UNKNOWN;
                            break;
                        }
                        if (t.value() != ret) {
                            LOG(INFO) << "messi set err code " << static_cast<int>(t.value());
                            ret = t.value();
                            break;
                        }
                    }
                    pushResultCode(ret, partId);
                    return folly::makeSemiFuture(ret);
                });
}

void AddEdgesAtomicProcessor::process(const cpp2::AddEdgesRequest& req) {
    LOG(INFO) << "AddEdgesAtomicProcessor::process()";
    LOG(INFO) << TransactionUtils::dumpAddEdgesRequest(req);

    auto version =
            std::numeric_limits<int64_t>::max() - time::WallClock::fastNowInMicroSec();
    // Switch version to big-endian, make sure the key is in ordered.
    version = folly::Endian::big(version);

    auto spaceId = req.get_space_id();
    const auto& partEdges = req.get_parts();
    propNames_ = req.get_prop_names();

    CHECK_NOTNULL(env_->schemaMan_);
    auto ret = env_->schemaMan_->getSpaceVidLen(spaceId);
    if (!ret.ok()) {
        LOG(ERROR) << ret.status();
        for (auto& part : partEdges) {
            pushResultCode(cpp2::ErrorCode::E_INVALID_SPACEVIDLEN, part.first);
        }
        onFinished();
        return;
    }

    auto spaceVidLen = ret.value();
    callingNum_ = req.parts.size();

    CHECK_NOTNULL(env_->kvstore_);
    std::vector<folly::Future<cpp2::ErrorCode>> futures;
    for (auto& part : req.parts) {
        futures.emplace_back(processPartition(spaceId,
                                              part.first,
                                              spaceVidLen,
                                              part.second,
                                              version));
    }

    folly::collectAll(futures)
        .via(env_->txnManager_->worker_.get())
        .thenTry([&](auto&&) {
            onFinished();
        });
}


folly::SemiFuture<cpp2::ErrorCode>
AddEdgesAtomicProcessor::addSingleEdge(int spaceId,
                                       int partId,
                                       int spaceVidLen,
                                       cpp2::NewEdge edge,
                                       int version) {
    cpp2::TransactionReq txnReq;
    txnReq.txn_id = TransactionUtils::getSnowFlakeUUID();
    txnReq.space_id = spaceId;
    txnReq.pos_of_chain = 0;
    txnReq.part_id = partId;
    txnReq.space_vid_len = spaceVidLen;

    appendEdgeInfo(txnReq, edge);
    appendEdgeInfo(txnReq, TransactionUtils::reverseEdge(edge));

    auto edgeKey = edge.key;
    LOG(INFO) << "PartitionID: " << partId
              << ", VertexID: " << edgeKey.src
              << ", EdgeType: " << edgeKey.edge_type
              << ", EdgeRanking: " << edgeKey.ranking
              << ", VertexID: " << edgeKey.dst
              << ", EdgeVersion: " << version;

    if (!NebulaKeyUtils::isValidVidLen(spaceVidLen, edgeKey.src, edgeKey.dst)) {
        LOG(ERROR) << "Space " << spaceId << " vertex length invalid, "
                    << "space vid len: " << spaceVidLen << ", edge srcVid: "
                    << edgeKey.src << " dstVid: " << edgeKey.dst;
        pushResultCode(cpp2::ErrorCode::E_INVALID_VID, partId);
        // onFinished();
        return folly::makeSemiFuture(cpp2::ErrorCode::E_TXN_ERR_UNKNOWN);
    }

    auto key = NebulaKeyUtils::edgeKey(spaceVidLen,
                                       partId,
                                       edgeKey.src,
                                       edgeKey.edge_type,
                                       edgeKey.ranking,
                                       edgeKey.dst,
                                       version);
    auto schema = env_->schemaMan_->getEdgeSchema(spaceId,
                                                  std::abs(edgeKey.edge_type));
    if (!schema) {
        LOG(ERROR) << "Space " << spaceId << ", Edge "
                    << edgeKey.edge_type << " invalid";
        pushResultCode(cpp2::ErrorCode::E_EDGE_NOT_FOUND, partId);
        // onFinished();
        return folly::makeSemiFuture(cpp2::ErrorCode::E_EDGE_NOT_FOUND);
    }

    auto props = edge.get_props();
    auto encodedProp = encodeRowVal(schema.get(), propNames_, props);
    if (!encodedProp.ok()) {
        LOG(ERROR) << encodedProp.status();
        pushResultCode(cpp2::ErrorCode::E_DATA_TYPE_MISMATCH, partId);
        return folly::makeSemiFuture(cpp2::ErrorCode::E_DATA_TYPE_MISMATCH);
    }

    txnReq.encoded_prop = std::move(encodedProp.value());

    // LOG(INFO) << TransactionUtils::dumpEdgeKeyHint(edgeKey,
    //     "exit AddEdgesAtomicProcessor::addSingleEdge");
    return env_->txnManager_->prepareTransaction(std::move(txnReq));
}

void AddEdgesAtomicProcessor::appendEdgeInfo(cpp2::TransactionReq& txnReq,
                                             const cpp2::NewEdge& e) {
    auto chainInfo = env_->txnManager_->getPartIdAndLeaderHost(txnReq.space_id, e.key.src);

    txnReq.parts.emplace_back(chainInfo.first);
    txnReq.chain.emplace_back(chainInfo.second);
    txnReq.edges.emplace_back(e.key);
}

}  // namespace storage
}  // namespace nebula

