/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include <folly/container/Enumerate.h>

#include "codec/RowWriterV2.h"
#include "storage/transaction/TransactionUtils.h"
#include "storage/transaction/TransactionManager.h"
#include "utils/NebulaKeyUtils.h"

#define CheckBlackBox 1
#define INTRUSIVE_TEST 1

namespace nebula {
namespace storage {

// folly::Future<cpp2::ErrorCode>
// TransactionManager::prepareTransaction(cpp2::TransactionReq&& req) {
//     LOG(INFO) << " TransactionManager::prepareTransaction() "
//               << TransactionUtils::dumpTransactionReq(req);
//     auto space = req.space_id;
//     auto part = req.part_id;
//     auto edgeKey = req.edges[req.pos_of_chain];

//     auto key = NebulaKeyUtils::edgeKey(req.space_vid_len,
//                                        part,
//                                        edgeKey.src,
//                                        edgeKey.edge_type,
//                                        edgeKey.ranking,
//                                        edgeKey.dst,
//                                        req.version);

// // #ifndef NDEBUG
// //     auto code = GoalKeeper::check(edgeKey.ranking, TossPhase::PREPATRE);
// //     if (code != cpp2::ErrorCode::SUCCEEDED) {
// //         return folly::makeFuture(code);
// //     }
// // #endif

//     auto prop = req.encoded_prop;
//     if (req.pos_of_chain == req.chain.size() - 1) {
//         LOG(INFO) << TransactionUtils::dumpEdgeKeyHint
//                  (edgeKey, "TransactionManager::before commit edge 2");
//         return commitEdge(space, part, std::move(key), std::move(prop)).via(worker_.get());
//     }

//     auto forwardTransaction = [&, req = req](cpp2::ErrorCode&& code) mutable {
//         if (code != cpp2::ErrorCode::SUCCEEDED) {
//             LOG(INFO) << "forwardTransaction pass err code " << static_cast<int>(code);
//             return folly::makeSemiFuture(code);
//         }
//         ++req.pos_of_chain;
//         auto c = folly::makePromiseContract<cpp2::ErrorCode>();
//         storageClient_->forwardTransaction(req)
//             .thenValue([pro = std::move(c.first), req = req]
                        // (StatusOr<storage::cpp2::ExecResponse>&& s) mutable {
//                 LOG(INFO) << TransactionUtils::
                                // dumpEdgeKeyHint(req.edges[1], "callback forwardTransaction()");

//                 auto code = cpp2::ErrorCode::SUCCEEDED;
//                 if (!s.ok()) {
//                     LOG(INFO) << "storageClient_->forwardTransaction return status: "
//                               << s.status().toString();
//                     pro.setValue(cpp2::ErrorCode::E_RPC_FAILURE);
//                     return;
//                 }
//                 storage::cpp2::ExecResponse execResp = s.value();
//                 cpp2::ResponseCommon result = execResp.get_result();
//                 std::vector<cpp2::PartitionResult> failed_parts = result.get_failed_parts();
//                 for (cpp2::PartitionResult& failed_part : failed_parts) {
//                     FLOG_INFO("messi forwardTransaction() failed part: %d, code %d",
//                               failed_part.part_id,
//                               static_cast<int>(failed_part.code));
//                     code = failed_part.code;
//                 }
//                 pro.setValue(code);
//         });

//         return std::move(c.second);
//     };

//     auto commitOutEdge = [&, space = space,
//                             part = part,
//                             eKey = edgeKey,
//                             key = std::move(key),
//                             prop = std::move(prop)] (cpp2::ErrorCode&& code) mutable {
//         if (code != cpp2::ErrorCode::SUCCEEDED) {
//             LOG(INFO) << "commitOutEdge pass err code " << static_cast<int>(code);
//             return folly::makeSemiFuture(code);
//         }
//         return commitEdge(space, part, std::move(key), std::move(prop));
//     };

//     auto unlockEdgeFn = [&, space = space, part = part, key = edgeKey](cpp2::ErrorCode&& code) {
//         if (code != cpp2::ErrorCode::SUCCEEDED) {
//             LOG(INFO) << "unlockEdgeFn pass err code " << static_cast<int>(code);
//             return folly::makeSemiFuture(code);
//         }
//         LOG(INFO) << TransactionUtils::dumpEdgeKeyHint(key, "enter unlockEdgeFn");
//         return unlockEdge(space, part, key);
//     };

//     auto cleanUpLocal = [&, key = edgeKey, k = key](cpp2::ErrorCode&& code) {
//         LOG(INFO) << TransactionUtils::dumpEdgeKeyHint(key, "enter cleanUpLocal");
//         inMemEdgeLock_.erase(k);
//         return folly::makeFuture(code);
//     };

//     return lockEdge(space, part, edgeKey)
//             .via(worker_.get())
//             .thenValue(forwardTransaction)
//             .thenValue(commitOutEdge)
//             .thenValue(unlockEdgeFn)
//             .thenValue(cleanUpLocal)
//             .thenError([](auto&& e){
//                 LOG(ERROR) << e.what();
//                 return folly::makeFuture(cpp2::ErrorCode::E_TXN_ERR_UNKNOWN);
//             });
// }

folly::Future<cpp2::ErrorCode>
TransactionManager::prepareTransaction(cpp2::TransactionReq&& req) {
    LOG(INFO) << __func__ << " " << TransactionUtils::dumpTransactionReq(req);
    auto space = req.space_id;
    auto part = req.part_id;
    auto vIdLen = req.space_vid_len;
    auto version = req.version;
    auto edgeKey = req.edges[req.pos_of_chain];

    auto rawKey = NebulaKeyUtils::edgeKey(vIdLen,
                                            part,
                                            edgeKey.src,
                                            edgeKey.edge_type,
                                            edgeKey.ranking,
                                            edgeKey.dst,
                                            version);
    // LOG(INFO) << __func__ << " messi version = " << version;
    LOG(INFO) << "messi hex " << TransactionUtils::dumpRawKey(vIdLen, rawKey);
    LOG(INFO) << "salah \n" << folly::hexDump(rawKey.data(), rawKey.size());
    // LOG(INFO) << TransactionUtils::dumpEdgeKeyHint(edgeKey, "prepareTransaction");

    auto& prop = req.encoded_prop;
    if (req.pos_of_chain == req.chain.size() - 1) {
        // LOG(INFO) << TransactionUtils::dumpEdgeKeyHint(edgeKey, "before commit edge 2");
        LOG(INFO) << TransactionUtils::dumpRawKey(vIdLen, rawKey);
        return commitEdge(space, part, std::move(rawKey), std::move(prop)).via(worker_.get());
    }

    auto forwardTransaction = [&, req = req](cpp2::ErrorCode&& code) mutable {
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            LOG(ERROR) << "forwardTransaction pass err code " << static_cast<int>(code);
            return folly::makeSemiFuture(code);
        }
        // LOG(INFO) << TransactionUtils::dumpEdgeKeyHint
        // (req.edges[1], "lock succeeded, before forwardTransaction()");
        ++req.pos_of_chain;
        auto c = folly::makePromiseContract<cpp2::ErrorCode>();
        interClient_->forwardTransaction(req)
            .thenValue([pro = std::move(c.first),
                        req = req](StatusOr<storage::cpp2::ExecResponse>&& s) mutable {
                auto code = cpp2::ErrorCode::SUCCEEDED;
                if (!s.ok()) {
                    LOG(ERROR) << "forwardTransaction return status: "
                               << s.status().toString();
                    pro.setValue(cpp2::ErrorCode::E_RPC_FAILURE);
                    return;
                }
                storage::cpp2::ExecResponse execResp = s.value();
                cpp2::ResponseCommon result = execResp.get_result();
                std::vector<cpp2::PartitionResult> failed_parts = result.get_failed_parts();
                for (cpp2::PartitionResult& failed_part : failed_parts) {
                    FLOG_INFO("messi forwardTransaction() failed part: %d, code %d",
                              failed_part.part_id,
                              static_cast<int>(failed_part.code));
                    code = failed_part.code;
                }
                pro.setValue(code);
        });

        return std::move(c.second);
    };

    auto commitOutEdge = [&,
                          vIdLen = vIdLen,
                          space = space,
                          part = part,
                          rawKey = rawKey,
                          prop = std::move(prop)] (cpp2::ErrorCode&& code) mutable {
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            LOG(INFO) << "commitOutEdge pass err code " << static_cast<int>(code);
            return folly::makeSemiFuture(code);
        }
        LOG(INFO) << TransactionUtils::dumpRawKey(vIdLen, rawKey);
        return commitEdge(space, part, std::move(rawKey), std::move(prop));
    };

    auto unlockEdgeFn = [&,
                         space = space,
                         part = part,
                         rawKey = rawKey](cpp2::ErrorCode&& code) mutable {
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            LOG(INFO) << "unlockEdgeFn pass err code " << static_cast<int>(code);
            return folly::makeSemiFuture(code);
        }
        return unlockEdge(space, part, std::move(rawKey));
    };

    auto cleanUpLocal = [&, rawKey = rawKey](cpp2::ErrorCode&& code) {
        auto keyWoVer = NebulaKeyUtils::keyWithNoVersion(rawKey);
        inMemEdgeLock_.erase(keyWoVer.str());
        return folly::makeFuture(code);
    };

    return lockEdge(vIdLen, space, part, std::move(rawKey))
            .via(worker_.get())
            .thenValue(forwardTransaction)
            .thenValue(commitOutEdge)
            .thenValue(unlockEdgeFn)
            .thenValue(cleanUpLocal)
            .thenError([](auto&& e){
                LOG(ERROR) << e.what();
                return folly::makeFuture(cpp2::ErrorCode::E_TXN_ERR_UNKNOWN);
            });
}


folly::SemiFuture<cpp2::ErrorCode>
TransactionManager::commitEdge(GraphSpaceID spaceId,
                               PartitionID partId,
                               std::string&& key,
                               std::string&& encodedProp) {
    std::string k0 = key;
    std::vector<kvstore::KV> data;
    data.emplace_back(std::move(key), std::move(encodedProp));

    auto contract = folly::makePromiseContract<cpp2::ErrorCode>();
    kvstore_->asyncMultiPut(
        spaceId,
        partId,
        std::move(data),
        [pro = std::move(contract.first), key = std::move(k0)] (kvstore::ResultCode code) mutable {
            // LOG(INFO) << "messi commit edge key: " << key << ", rc=" << static_cast<int>(code);
            pro.setValue(TransactionUtils::to(code));
        });
    return std::move(contract.second);
}


// // there is no need to check if there is a dangle transaction.
// // because the new 'addEdge' will overwrite the old one.
// folly::SemiFuture<cpp2::ErrorCode> TransactionManager::lockEdge(GraphSpaceID spaceId,
//                                                                 PartitionID partId,
//                                                                 const cpp2::EdgeKey& e) {
//     LOG(INFO) << TransactionUtils::dumpEdgeKeyHint(e, "enter lockEdge");
//     auto key = TransactionUtils::edgeLockKey(spaceId, partId, e);

//     TransactionUtils::intrusiveTest(e.ranking, TossPhase::LOCK, [&](){
//         LOG(INFO) << "messi add a redundant key in inMemEdgeLock";
//         inMemEdgeLock_.insert(std::make_pair(key, 0));
//     });

//     auto retInsert = inMemEdgeLock_.insert(std::make_pair(key, 0));
//     if (!retInsert.second) {
//         LOG(ERROR) << TransactionUtils::dumpEdgeKeyHint(e, "enter lockEdge") << " conflict";
//         return folly::makeSemiFuture(cpp2::ErrorCode::E_ADD_EDGE_CONFILCT);
//     }

//     std::vector<kvstore::KV> data{{key, ""}};
//     FLOG_INFO("messi lock key: %s", key.c_str());
//     auto contract = folly::makePromiseContract<cpp2::ErrorCode>();
//     kvstore_->asyncMultiPut(
//         spaceId,
//         partId,
//         std::move(data),
//         [pro = std::move(contract.first)] (kvstore::ResultCode code) mutable {
//             pro.setValue(TransactionUtils::to(code));
//         });
//     return std::move(contract.second);
// }

// there is no need to check if there is a dangle transaction.
// because the new 'addEdge' will overwrite the old one.
folly::SemiFuture<cpp2::ErrorCode> TransactionManager::lockEdge(size_t vIdLen,
                                                                GraphSpaceID spaceId,
                                                                PartitionID partId,
                                                                std::string&& rawKey) {
    // LOG(INFO) << TransactionUtils::dumprawKeyHint(e, "enter lockEdge");

    auto keyWoVer = NebulaKeyUtils::keyWithNoVersion(rawKey);
    int64_t ver = NebulaKeyUtils::getVersion(vIdLen, rawKey);

    // TransactionUtils::intrusiveTest(rawKey.ranking, TossPhase::LOCK, [&](){
    //     LOG(INFO) << "messi add a redundant key in inMemEdgeLock";
    //     inMemEdgeLock_.insert(std::make_pair(keyWoVer.str(), ver));
    // });

    auto retInsert = inMemEdgeLock_.insert(std::make_pair(keyWoVer.str(), ver));
    if (!retInsert.second) {
        return folly::makeSemiFuture(cpp2::ErrorCode::E_ADD_EDGE_CONFILCT);
    }

    std::vector<kvstore::KV> data{{std::move(rawKey), ""}};
    FLOG_INFO("messi lock rawKey: %s", rawKey.c_str());
    auto contract = folly::makePromiseContract<cpp2::ErrorCode>();
    kvstore_->asyncMultiPut(
        spaceId,
        partId,
        std::move(data),
        [pro = std::move(contract.first)] (kvstore::ResultCode code) mutable {
            pro.setValue(TransactionUtils::to(code));
        });
    return std::move(contract.second);
}


// bool TransactionManager::hasDangleLock(GraphSpaceID spaceId,
//                                        size_t vIdLen,
//                                        PartitionID partId,
//                                        const cpp2::EdgeKey& edgeKey) {
//     if (edgeKey.edge_type < 0) {
//         return false;
//     }
//     FLOG_INFO("messi [enter] %s, rank %ld", __func__, edgeKey.ranking);
//     auto key = TransactionUtils::edgeLockKey(spaceId, vIdLen, partId, edgeKey);

//     TransactionUtils::intrusiveTest(edgeKey.ranking, TossPhase::COMMIT_EDGE2_RESP, [&](){
//         inMemEdgeLock_.erase(key);
//     });
//     auto retInsert = inMemEdgeLock_.insert(std::make_pair(key, 0));
//     if (!retInsert.second) {
//         LOG(INFO) << "messi try lock in mem failed";
//         return false;
//     }

//     std::string val;
//     // auto key = TransactionUtils::edgeLockKey(spaceId, partId, edgeKey);
//     // local read is enough,
//     // nebula guarantee edge lock will only write on leader
//     // raft guarantee only the host has edge lock can be voted as leader
//     auto rc = kvstore_->get(spaceId, partId, key, &val);

//     LOG(INFO) << "messi kvstore_->get(edge) ret " << static_cast<int>(rc);
//     return rc == kvstore::ResultCode::SUCCEEDED;
// }

std::string TransactionManager::hasDangleLock(size_t vIdLen,
                                       GraphSpaceID spaceId,
                                       PartitionID partId,
                                       const std::string& rawEdgeKey) {
    std::string ret;
    EdgeType edgeType = NebulaKeyUtils::getEdgeType(vIdLen, rawEdgeKey);
    if (edgeType < 0) {
        // return false;
        return ret;
    }

#ifdef NDEBUG
    // if () {
    //     inMemEdgeLock_.erase(key);
    // }
    // TransactionUtils::intrusiveTest(NebulaKeyUtils::getEdgeType(),
    //                                 TossPhase::COMMIT_EDGE2_RESP,
    //                                 [&](){
    //     inMemEdgeLock_.erase(key);
    // });
#endif

    auto lockKey = NebulaKeyUtils::keyWithNoVersion(rawEdgeKey);

    auto retInsert = inMemEdgeLock_.insert(std::make_pair(lockKey.str(), 0));
    if (!retInsert.second) {
        LOG(ERROR) << "messi set mem lock failed";
        return ret;
    }
    // auto key = TransactionUtils::edgeLockKey(spaceId, partId, edgeKey);
    // local read is enough,
    // nebula guarantee edge lock will only write on leader
    // raft guarantee only the host has edge lock can be voted as leader
    kvstore_->get(spaceId, partId, rawEdgeKey, &ret);
    return ret;
    // return rc == kvstore::ResultCode::SUCCEEDED;
}

folly::SemiFuture<cpp2::ErrorCode>
TransactionManager::clearDangleTransaction(size_t vIdLen,
                                           GraphSpaceID spaceId,
                                           PartitionID partId,
                                           const cpp2::EdgeKey& edgeKey) {
    // LOG(INFO) << folly::format("messi clearDangleTransaction() space: {}, part {}, edge {}",
    //                             spaceId, partId, TransactionUtils::dumpEdgeKey(e));

    auto key = TransactionUtils::edgeLockKey(vIdLen, spaceId, partId, edgeKey);
    auto contract = folly::makePromiseContract<cpp2::ErrorCode>();
    kvstore_->asyncRemove(
        spaceId,
        partId,
        key,
        [pro = std::move(contract.first)] (kvstore::ResultCode code) mutable {
            pro.setValue(TransactionUtils::to(code));
        });
    return std::move(contract.second);
}


folly::SemiFuture<cpp2::ErrorCode>
TransactionManager::resumeTransaction(GraphSpaceID spaceId,
                                      PartitionID partId,
                                      const cpp2::EdgeKey& edgeKey,
                                      std::vector<nebula::Value>&& props) {
    FLOG_INFO("messi [enter] %s", __func__);
    for (auto&& it : folly::enumerate(props)) {
        LOG(INFO) << "messi vals[" << it.index << "], type= " << it->type() << ", val=" << *it;
    }
    auto schema = schemaMan_->getEdgeSchema(spaceId,
                                                  std::abs(edgeKey.edge_type));
    if (!schema) {
        LOG(ERROR) << "Space " << spaceId << ", Edge "
                    << edgeKey.edge_type << " invalid";
        return folly::makeSemiFuture(cpp2::ErrorCode::E_EDGE_NOT_FOUND);
    }

    auto ret = schemaMan_->getSpaceVidLen(spaceId);
    if (!ret.ok()) {
        return folly::makeSemiFuture(cpp2::ErrorCode::E_SPACE_NOT_FOUND);
    }
    LOG(INFO) << "messi schema_->getNumFields()=" << schema->getNumFields();
    auto spaceVidLen = ret.value();

    RowWriterV2 rowWrite(schema.get());
    for (size_t i = 4; i < props.size(); i++) {
        LOG(INFO) << "messi props[" << i << "].val type = " << props[i].type()
                  << ", val.toString()=" << props[i].toString();
        auto wRet = rowWrite.setValue(i-4, props[i]);
        if (wRet != WriteResult::SUCCEEDED) {
            FLOG_INFO("messi rowWrite.setValue[%zu] failed, rc=%d", i, static_cast<int>(wRet));
            return folly::makeSemiFuture(cpp2::ErrorCode::E_INVALID_FIELD_VALUE);
        }
    }
    if (rowWrite.finish() != WriteResult::SUCCEEDED) {
        folly::makeSemiFuture(cpp2::ErrorCode::E_INVALID_FIELD_VALUE);
    }

    auto encodedProp = rowWrite.moveEncodedStr();

    auto key = NebulaKeyUtils::edgeKey(spaceVidLen,
                                       partId,
                                       edgeKey.src,
                                       edgeKey.edge_type,
                                       edgeKey.ranking,
                                       edgeKey.dst,
                                       0);
    FLOG_INFO("messi before commitEdge");

    LOG(INFO) << "messi commit edge: " << TransactionUtils::dumpRawKey(spaceVidLen, key);

    return commitEdge(spaceId, partId, std::move(key), std::move(encodedProp));
}

// folly::SemiFuture<cpp2::ErrorCode>
// TransactionManager::unlockEdge(GraphSpaceID spaceId,
//                                PartitionID partId,
//                                const cpp2::EdgeKey& e) {
//     auto key = TransactionUtils::edgeLockKey(spaceId, partId, e);
//     LOG(INFO) << " messi unlockEdge key " << key;
//     auto contract = folly::makePromiseContract<cpp2::ErrorCode>();
//     kvstore_->asyncRemove(
//         spaceId,
//         partId,
//         key,
//         [pro = std::move(contract.first)] (kvstore::ResultCode code) mutable {
//             pro.setValue(TransactionUtils::to(code));
//         });
//     return std::move(contract.second);
// }

folly::SemiFuture<cpp2::ErrorCode>
TransactionManager::unlockEdge(GraphSpaceID spaceId,
                               PartitionID partId,
                               std::string&& rawKey) {
    // auto key = TransactionUtils::edgeLockKey(spaceId, partId, e);
    LOG(INFO) << " messi unlockEdge key " << rawKey;
    auto contract = folly::makePromiseContract<cpp2::ErrorCode>();
    kvstore_->asyncRemove(
        spaceId,
        partId,
        rawKey,
        [pro = std::move(contract.first)] (kvstore::ResultCode code) mutable {
            pro.setValue(TransactionUtils::to(code));
        });
    return std::move(contract.second);
}

// folly::Future<cpp2::ErrorCode>
// TransactionManager::resumeTransactionIfAny(GraphSpaceID spaceId,
//                                            PartitionID partId,
//                                            cpp2::EdgeKey&& edgeKey) {
//     LOG(INFO) << "messi [enter] " << __func__;
//     if (!hasDangleLock(spaceId, partId, edgeKey)) {
//         return;
//     }

//     nebula::List row = getInEdgeProp(spaceId, edgeKey);
//     if (row[0].isNull()) {
//         auto sf = clearDangleTransaction(spaceId, partId, edgeKey);
//         sf.wait();
//     } else {
//         auto sf = resumeTransaction(spaceId, partId, edgeKey, std::move(row.values));
//         sf.wait();
//     }
// }

cpp2::ErrorCode TransactionManager::resumeTransactionIfAny(size_t vIdLen,
                                                           GraphSpaceID spaceId,
                                                           PartitionID partId,
                                                           const VertexID& vId,
                                                           EdgeType edgeType) {
    LOG(INFO) << "messi [enter] " << __func__
              << ", vid = " << vId << ", type = " << edgeType;

    std::vector<folly::SemiFuture<cpp2::ErrorCode>> futs;
    auto prefix = TransactionUtils::edgeLockPrefix(vIdLen, spaceId, partId, vId, edgeType);

    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(spaceId, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        return cpp2::ErrorCode::SUCCEEDED;
    }
    std::vector<std::string> memLocks;
    while (iter->valid()) {
        auto rawKey = iter->key();
        rawKey.advance(TransactionUtils::edgeLockTable().size());
        auto rawKeyWoVer = NebulaKeyUtils::keyWithNoVersion(rawKey);
        int64_t ver = NebulaKeyUtils::getVersion(vIdLen, rawKey);
        auto insResult = inMemEdgeLock_.insert(std::make_pair(rawKeyWoVer.str(), ver));
        if (!insResult.second) {
            continue;
        }

        cpp2::EdgeKey edgeKey = TransactionUtils::toEdgekey(vIdLen, rawKey);
        futs.emplace_back(resumeTransactionIfAny(vIdLen,
                                                 spaceId,
                                                 partId,
                                                 std::move(edgeKey)));
        memLocks.emplace_back(rawKey.str());
    }
    if (futs.empty()) {
        return cpp2::ErrorCode::SUCCEEDED;
    }

    auto allf = collectAll(futs);
    allf.wait();

    for (auto& lock : memLocks) {
        inMemEdgeLock_.erase(lock);
    }

    return cpp2::ErrorCode::SUCCEEDED;
    // if (ret == kvstore::ResultCode::SUCCEEDED && iter && iter->valid()) {
    //     iter_.reset(new SingleEdgeIterator(
    //         planContext_, std::move(iter), edgeType_, schemas_, &ttl_));
    // } else {
    //     iter_.reset();
    // }

    // nebula::List row = getInEdgeProp(spaceId, e);
    // if (row[0].isNull()) {
    //     auto sf = clearDangleTransaction(spaceId, partId, e);
    //     sf.wait();
    // }
    // auto sf = resumeTransaction(spaceId, partId, e, std::move(row.values));
    // sf.wait();
    // FLOG_INFO("messi [exit] %s", __func__);
}


nebula::List TransactionManager::getInEdgeProp(GraphSpaceID spaceId,
                                               const cpp2::EdgeKey& outEdgeKey) {
    auto keyDesc = TransactionUtils::dumpEdgeKey(outEdgeKey);
    FLOG_INFO("messi [enter] %s, outEdgeKey %s", __func__, keyDesc.c_str());

    cpp2::EdgeKey key{outEdgeKey};
    TransactionUtils::reverseEdgeKeyInPlace(key);

    nebula::DataSet ds({kSrc, kType, kRank, kDst});
    std::vector<Value> edgeInfo{key.src, key.edge_type, key.ranking, key.dst};
    List row(std::move(edgeInfo));
    ds.rows.emplace_back(std::move(row));
    std::vector<cpp2::EdgeProp> props(1);
    props.back().type = key.edge_type;

    auto code = cpp2::ErrorCode::SUCCEEDED;
    List lst;
    do {
        auto fRpc = storageClient_->getProps(spaceId,
                                             std::move(ds),     /*DataSet*/
                                             nullptr,           /*vector<cpp2::VertexProp>*/
                                             &props,            /*vector<cpp2::EdgeProp>*/
                                             nullptr            /*expressions*/)
                                             .via(worker_.get());
        fRpc.wait();
        if (!fRpc.valid()) {
            LOG(ERROR) << "messi getProps() failed";
        }
        // StorageRpcResponse<cpp2::GetPropResponse>
        auto rpcResp = std::move(fRpc.value());
        if (!rpcResp.succeeded()) {
            LOG(ERROR) << "messi getProps() failed";
        }

        std::vector<cpp2::GetPropResponse>& getPropResps = rpcResp.responses();
        LOG(INFO) << "messi getPropResps.size()=" << getPropResps.size();
        for (cpp2::GetPropResponse& getPropResp : getPropResps) {
            code = cpp2::ErrorCode::SUCCEEDED;
            cpp2::ResponseCommon& result = getPropResp.result;
            std::vector<cpp2::PartitionResult>& failedParts = result.failed_parts;
            LOG(INFO) << "messi failedParts.size() = " << failedParts.size();
            for (cpp2::PartitionResult& res : failedParts) {
                LOG(INFO) << "messi [failedPartResult] partId: " << res.part_id
                        << ", code=" << static_cast<int>(res.code)
                        << ", leader=" << res.leader;
                code = res.code;
                if (code == cpp2::ErrorCode::E_LEADER_CHANGED) {
                    // StatusOr<LeaderMap> =
                    // auto statusOrLeaderMap = metaClient_->loadLeader();
                    // if (statusOrLeaderMap.ok()) {
                    //     auto leaderMap = statusOrLeaderMap.value();
                    // }
                    LOG(INFO) << "messi updateLeader";
                    storageClient_->updateLeader(spaceId, res.part_id, res.leader);
                }
            }
            if (failedParts.empty()) {
                nebula::DataSet* pDataSet = getPropResp.get_props();
                lst = pDataSet->rows[0];
            }
        }
    } while (code == cpp2::ErrorCode::E_LEADER_CHANGED);
    return lst;
}


std::pair<PartitionID, HostAddr>
TransactionManager::getPartIdAndLeaderHost(GraphSpaceID spaceId, const VertexID& vid) {
    auto partId = metaClient_->partId(spaceId, vid);
    auto leader = getLeader(spaceId, partId.value());
    return std::make_pair(partId.value(), leader);
}


HostAddr TransactionManager::getLeader(GraphSpaceID spaceId, PartitionID partId) {
    LOG(INFO) << folly::sformat("messi getLeader of space {}, part {} ", spaceId, partId);
    std::lock_guard<std::mutex> lk(muLeader_);
    HostAddr ret;
    auto k = std::make_pair(spaceId, partId);
    auto constIter = leaderCache_.find(k);
    if (constIter != leaderCache_.cend()) {
        ret = constIter->second;
        LOG(INFO) << "messi leader: " << ret;
        return ret;
    }

    LOG(INFO) << folly::sformat("messi no leader of space {}, part {} in cache",
                                k.first,
                                k.second);
    auto statusOrLeaderMap = metaClient_->loadLeader();
    if (!statusOrLeaderMap.ok()) {
        LOG(INFO) << "messi leader: " << ret;
        return ret;
    }
    auto leaderMap = std::move(statusOrLeaderMap).value();

    auto constIter2 = leaderMap.find(k);
    if (constIter2 == leaderMap.cend()) {
        return ret;
    }

    ret = constIter2->second;
    leaderCache_.insert(k, ret);
    return ret;
}


}  // namespace storage
}  // namespace nebula
