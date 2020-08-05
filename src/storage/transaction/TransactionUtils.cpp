/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/time/WallClock.h"
#include "storage/transaction/TransactionUtils.h"
#include "utils/NebulaKeyUtils.h"

// #include <boost/archive/text_iarchive.hpp>
// #include <boost/archive/text_oarchive.hpp>

// namespace nebula {
// namespace storage {
// namespace cpp2 {

// template<class Archive>
// void serialize(Archive & ar, cpp2::EdgeKey & k, const unsigned int /*version*/) {
//     ar & k.src;
//     ar & k.ranking;
//     ar & k.edge_type;
//     ar & k.dst;
// }

// }  // namespace cpp2
// }  // namespace storage
// }  // namespace nebula

namespace nebula {
namespace storage {

/**
 *
 * EdgeKeyUtils:
 * type(1) + partId(3) + srcId(*) + edgeType(4) + edgeRank(8) + dstId(*) + version(8)
 *
 * */

const std::string kEdgeLockTable         = "__edge_lock__";         // NOLINT

const std::string& TransactionUtils::edgeLockTable() {
    return kEdgeLockTable;
}

std::string TransactionUtils::dumpEdgeKeyHint(const cpp2::EdgeKey& key, const std::string& msg) {
    std::ostringstream oss;
    if (key.edge_type > 0) {
        oss << key.src << key.edge_type << key.ranking << key.dst << " grep " << msg;
    } else {
        oss << "-" << key.dst << key.edge_type * -1 << key.ranking << key.src << " grep " << msg;
    }

    return oss.str();
}

// std::string TransactionUtils::dumpRawEdgekey(size_t vIdLen, const std::string& msg) {
//     cpp2::EdgeKey edgeKey;

// }


std::string TransactionUtils::dumpEdgeKey(const cpp2::EdgeKey& key) {
    std::ostringstream oss;
    oss << "edgekey: src " << key.src
        << ", type=" << key.edge_type
        << ", ranking=" << key.ranking
        << ", dst=" << key.dst << ". ";
    return oss.str();
}


std::string TransactionUtils::dumpNewEdge(const cpp2::NewEdge& edge) {
    std::ostringstream oss;
    oss << dumpEdgeKey(edge.key);
    return oss.str();
}


std::string TransactionUtils::dumpTransactionReq(const cpp2::TransactionReq& req) {
    std::ostringstream oss;
    oss << "\n txn_id: " << req.txn_id
        << "\n space_id " << req.space_id
        << "\n index " << req.pos_of_chain;
    for (auto&& it : folly::enumerate(req.parts)) {
        oss << "\n parts" << it.index << ": " << *it;
    }

    for (auto&& it : folly::enumerate(req.chain)) {
        oss << "\n hosts" << it.index << ": " << *it;
    }

    for (auto&& it : folly::enumerate(req.edges)) {
        oss << "\n edges" << it.index << ": " << dumpEdgeKey(*it);
    }
    return oss.str();
}


std::string TransactionUtils::dumpAddEdgesRequest(const cpp2::AddEdgesRequest& req) {
    std::ostringstream oss;
    oss << "dumpAddEdgesRequest: "
        << ", space_id=" << req.space_id << "\n";
    for (auto& part : req.parts) {
        oss << "part=" << part.first;
        for (auto& edge : part.second) {
            oss << ", edge=" << dumpNewEdge(edge) << "\n";
        }
    }
    oss << "\nprop_names.size()=" << req.prop_names.size();
    oss << "\nprops=";
    for (auto& prop : req.prop_names) {
        oss << prop << " ";
    }
    return oss.str();
}

// std::string TransactionUtils::edgeLockKey(
//         GraphSpaceID spaceId,
//         PartitionID partId,
//         const cpp2::EdgeKey& e) {
//     return folly::sformat("s{}p{}s{}e{}r{}d{}",
//                           spaceId,
//                           partId,
//                           e.src,
//                           e.edge_type,
//                           e.ranking,
//                           e.dst);
// }

// std::string TransactionUtils::edgeLockPrefix()



// std::string TransactionUtils::edgeLockKey(GraphSpaceID spaceId,
//                                           PartitionID partId,
//                                           const cpp2::EdgeKey& k) {
//     std::ostringstream oss;
//     boost::archive::text_oarchive oa(oss);
//     oa & spaceId;
//     oa & partId;
//     oa & k;
//     return oss.str();
// }
// static
std::string TransactionUtils::edgeLockKey(size_t vIdLen,
                                          GraphSpaceID spaceId,
                                          PartitionID partId,
                                          const cpp2::EdgeKey& e) {
    return edgeLockKey(spaceId,
                       vIdLen,
                       partId,
                       e.src,
                       e.edge_type,
                       e.ranking,
                       e.dst);
}

// static
std::string TransactionUtils::edgeLockKey(size_t vIdLen,
                                          GraphSpaceID spaceId,
                                          PartitionID partId,
                                          VertexID srcId,
                                          EdgeType type,
                                          EdgeRanking rank,
                                          VertexID dstId) {
    EdgeVersion ev = 0;
    return kEdgeLockTable + std::to_string(spaceId);
           NebulaKeyUtils::edgeKey(vIdLen, partId, srcId, type, rank, dstId, ev);
}

std::string TransactionUtils::edgeLockPrefix(size_t vIdLen,
                                             GraphSpaceID spaceId,
                                             PartitionID partId,
                                             VertexID srcId,
                                             EdgeType type) {
    return kEdgeLockTable + std::to_string(spaceId);
           NebulaKeyUtils::edgePrefix(vIdLen, partId, srcId, type);
}

// std::string TransactionUtils::edgeLockKeyBoost(
//         GraphSpaceID spaceId,
//         PartitionID partId,
//         const cpp2::EdgeKey& k) {
//     std::ostringstream oss;
//     boost::archive::text_oarchive oa(oss);
//     oa & spaceId;
//     oa & partId;
//     oa & k;
//     return oss.str();
// }

// std::tuple<int, int, cpp2::EdgeKey> TransactionUtils::parseEdgeKey(const std::string& rawKey) {
//     std::stringstream iss(rawKey);
//     boost::archive::text_iarchive ia(iss);

//     std::tuple<int, int, cpp2::EdgeKey> ret;
//     ia & std::get<0>(ret);
//     ia & std::get<1>(ret);
//     ia & std::get<2>(ret);
//     return ret;
// }


void TransactionUtils::reverseEdgeKeyInPlace(cpp2::EdgeKey& key) {
    std::swap(key.src, key.dst);
    key.edge_type *= -1;
}


cpp2::NewEdge TransactionUtils::reverseEdge(const cpp2::NewEdge& edge) {
    cpp2::NewEdge ret(edge);
    std::swap(ret.key.src, ret.key.dst);
    ret.key.edge_type *= -1;
    return ret;
}


cpp2::ErrorCode TransactionUtils::to(kvstore::ResultCode rc) {
    switch (rc) {
        case kvstore::ResultCode::SUCCEEDED :
            return cpp2::ErrorCode::SUCCEEDED;
        default:
            return cpp2::ErrorCode::E_TXN_ERR_UNKNOWN;
    }
    return cpp2::ErrorCode::E_TXN_ERR_UNKNOWN;
}

int64_t TransactionUtils::getSnowFlakeUUID() {
    return time::WallClock::slowNowInMicroSec();
}

// ranking split to 3 pieces.
// e.g. ranking = 77771001
// num before last 4 digits is a magic num,
// this func will only deal with those ranking which matches
// the last 3,4 pos is a phase identifier.
// e.g. 10 means "prepare"
// and the last 2 position means operation
// e.g. 01-05 => return error
//      06-08 throw exception
//      10  sleep (for demostration conflict). etc
cpp2::ErrorCode GoalKeeper::check(int ranking, TossPhase phase) {
    LOG(INFO) << "messi check " << ranking << "@" << static_cast<int>(phase);
    if (ranking / 10000 != 7777) {
        return cpp2::ErrorCode::SUCCEEDED;
    }
    int phaseAndOp = ranking % 10000;
    if (phaseAndOp / 100 != static_cast<int>(phase)) {
        return cpp2::ErrorCode::SUCCEEDED;
    }
    return doSomethingBad(phaseAndOp % 100);
}

cpp2::ErrorCode GoalKeeper::doSomethingBad(int opCode) {
    LOG(INFO) << "messi doSomethingBad " << opCode;
    switch (opCode) {
        case 1:
            return cpp2::ErrorCode::E_TXN_ERR_UNKNOWN;
        case 2:
            throw std::runtime_error("doSomethingBad 2");
        case 10:
            std::this_thread::sleep_for(std::chrono::seconds(2));
            break;
        default:
            return cpp2::ErrorCode::E_TXN_ERR_UNKNOWN;
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

bool TransactionUtils::triggerTest(int rank, TossPhase phase) {
    if (rank < 10000) {
        return false;
    }
    rank %= 10000;
    rank /= 100;
    return rank == static_cast<int>(phase);
}

cpp2::EdgeKey TransactionUtils::toEdgekey(size_t vIdLen, folly::StringPiece rawKey) {
    cpp2::EdgeKey ret;
    ret.set_src(NebulaKeyUtils::getSrcId(vIdLen, rawKey).str());
    ret.edge_type = NebulaKeyUtils::getEdgeType(vIdLen, rawKey);
    ret.ranking = NebulaKeyUtils::getRank(vIdLen, rawKey);
    ret.set_dst(NebulaKeyUtils::getDstId(vIdLen, rawKey).str());
    return ret;
}

// void TransactionUtils::intrusiveTest(const std::string& skey,
//                                      TossPhase phase,
//                                      std::function<void()>&& f) {
//     LOG(INFO) << __func__ << " skey=" << skey << ", phase=" << static_cast<int>(phase);
//     auto tp = parseEdgeKey(skey);
//     intrusiveTest(std::get<2>(tp).ranking, phase, std::move(f));
// }

// void TransactionUtils::intrusiveTest(int rank, TossPhase phase, std::function<void()>&& f) {
//     LOG(INFO) << __func__ << " rank=" << rank << ", phase=" << static_cast<int>(phase);
//     if (triggerTest(rank, phase)) {
//         LOG(INFO) << __func__ << " rank=" << rank << ", phase=" << static_cast<int>(phase);
//         return f();
//     }
// }


}  // namespace storage
}  // namespace nebula
