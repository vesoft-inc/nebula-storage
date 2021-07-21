/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/uuid_generators.hpp>

#include "storage/transaction/ConsistUtil.h"
#include "utils/NebulaKeyUtils.h"
namespace nebula {
namespace storage {

static const std::string kPrimeTable{"__prime__"};  // NOLINT
static const std::string kDoublePrimeTable{"__prime_prime__"};  // NOLINT
static const std::string kTempRequestTable{"__temp_request__"};  // NOLINT

std::string ConsistUtil::primeTable() {
    return kPrimeTable;
}

std::string ConsistUtil::tempRequestTable() {
    return kTempRequestTable;
}

std::string ConsistUtil::doublePrimeTable() {
    return kDoublePrimeTable;
}

std::string ConsistUtil::primePrefix(PartitionID partId) {
    return kPrimeTable +NebulaKeyUtils::edgePrefix(partId);
}

std::string ConsistUtil::doublePrimePrefix(PartitionID partId) {
    return kDoublePrimeTable +NebulaKeyUtils::edgePrefix(partId);
}

std::string ConsistUtil::edgeKeyPrime(size_t vIdLen,
                                       PartitionID partId,
                                       const cpp2::EdgeKey& edgeKey) {
    return ConsistUtil::edgeKeyPrime(vIdLen,
                                      partId,
                                      edgeKey.get_src().getStr(),
                                      edgeKey.get_edge_type(),
                                      edgeKey.get_ranking(),
                                      edgeKey.get_dst().getStr());
}

std::string ConsistUtil::doublePrime(size_t vIdLen,
                                             PartitionID partId,
                                             const cpp2::EdgeKey& edgeKey) {
    return ConsistUtil::doublePrime(vIdLen,
                                            partId,
                                            edgeKey.get_src().getStr(),
                                            edgeKey.get_edge_type(),
                                            edgeKey.get_ranking(),
                                            edgeKey.get_dst().getStr());
}

std::string ConsistUtil::edgeKeyPrime(size_t vIdLen,
                                       PartitionID partId,
                                       const VertexID& src,
                                       EdgeType type,
                                       EdgeRanking rank,
                                       const VertexID& dst) {
    return kPrimeTable + NebulaKeyUtils::edgeKey(vIdLen, partId, src, type, rank, dst);
}

std::string ConsistUtil::doublePrime(size_t vIdLen,
                                             PartitionID partId,
                                             const VertexID& src,
                                             EdgeType type,
                                             EdgeRanking rank,
                                             const VertexID& dst) {
    return kDoublePrimeTable + NebulaKeyUtils::edgeKey(vIdLen, partId, src, type, rank, dst);
}

TypeOfEdgePrime ConsistUtil::parseType(folly::StringPiece val) {
    char identifier = val.str().back();
    switch (identifier) {
        case 'u':
            return TypeOfEdgePrime::UPDATE;
        case 'a':
            return TypeOfEdgePrime::INSERT;
        default:
            LOG(FATAL) << "shoule not happend, identifier is " << identifier;
            return TypeOfEdgePrime::UNKNOWN;
    }
}

cpp2::UpdateEdgeRequest ConsistUtil::parseUpdateRequest(folly::StringPiece val) {
    cpp2::UpdateEdgeRequest req;
    apache::thrift::CompactSerializer::deserialize(val, req);
    return req;
}

cpp2::AddEdgesRequest ConsistUtil::parseAddRequest(folly::StringPiece val) {
    cpp2::AddEdgesRequest req;
    apache::thrift::CompactSerializer::deserialize(val, req);
    return req;
}

std::string ConsistUtil::strUUID() {
    static boost::uuids::random_generator gen;
    return boost::uuids::to_string(gen());
}

std::string ConsistUtil::ConsistUtil::edgeKey(size_t vIdLen,
                                              PartitionID partId,
                                              const cpp2::EdgeKey& key) {
    return NebulaKeyUtils::edgeKey(vIdLen,
                                   partId,
                                   key.get_src().getStr(),
                                   *key.edge_type_ref(),
                                   *key.ranking_ref(),
                                   (*key.dst_ref()).getStr());
}

std::vector<int64_t> ConsistUtil::getMultiEdgeVers(kvstore::KVStore* store,
                                                    GraphSpaceID spaceId,
                                                    PartitionID partId,
                                                    const std::vector<std::string>& keys) {
    std::vector<int64_t> ret(keys.size());
    std::vector<std::string> _keys(keys);
    auto rc = nebula::cpp2::ErrorCode::SUCCEEDED;
    std::vector<Status> status;
    std::vector<std::string> vals;
    std::tie(rc, status) =
        store->multiGet(spaceId, partId, std::move(_keys), &vals);
    if (rc != nebula::cpp2::ErrorCode::SUCCEEDED &&
        rc != nebula::cpp2::ErrorCode::E_PARTIAL_RESULT) {
        return ret;
    }
    for (auto i = 0U; i != ret.size(); ++i) {
        ret[i] = getTimestamp(vals[i]);
    }
    return ret;
}

// return -1 if edge version not exist
int64_t ConsistUtil::getSingleEdgeVer(kvstore::KVStore* store,
                                       GraphSpaceID spaceId,
                                       PartitionID partId,
                                       const std::string& key) {
    static int64_t invalidEdgeVer = -1;
    std::string val;
    auto rc = store->get(spaceId, partId, key, &val);
    if (rc != nebula::cpp2::ErrorCode::SUCCEEDED) {
        return invalidEdgeVer;
    }
    return getTimestamp(val);
}

int64_t ConsistUtil::getTimestamp(const std::string& val) noexcept {
    return *reinterpret_cast<const int64_t*>(val.data() + (val.size() - sizeof(int64_t)));
}

cpp2::AddEdgesRequest ConsistUtil::makeDirectAddReq(const cpp2::ChainAddEdgesRequest& req) {
    cpp2::AddEdgesRequest ret;
    ret.set_space_id(req.get_space_id());
    ret.set_parts(req.get_parts());
    ret.set_prop_names(req.get_prop_names());
    ret.set_if_not_exists(req.get_if_not_exists());
    return ret;
}

cpp2::EdgeKey ConsistUtil::reverseEdgeKey(const cpp2::EdgeKey& edgeKey) {
    cpp2::EdgeKey reversedKey(edgeKey);
    std::swap(*reversedKey.src_ref(), *reversedKey.dst_ref());
    *reversedKey.edge_type_ref() = 0 - edgeKey.get_edge_type();
    return reversedKey;
}

void ConsistUtil::reverseEdgeKeyInplace(cpp2::EdgeKey& edgeKey) {
    cpp2::EdgeKey reversedKey(edgeKey);
    std::swap(*edgeKey.src_ref(), *edgeKey.dst_ref());
    *edgeKey.edge_type_ref() = 0 - edgeKey.get_edge_type();
}

std::pair<int64_t, nebula::cpp2::ErrorCode> ConsistUtil::versionOfUpdateReq(
    StorageEnv* env,
    const cpp2::UpdateEdgeRequest& req) {
    int64_t ver = -1;
    auto rc = nebula::cpp2::ErrorCode::SUCCEEDED;

    do {
        auto spaceId = req.get_space_id();
        auto stVidLen = env->metaClient_->getSpaceVidLen(spaceId);
        if (!stVidLen.ok()) {
            rc = nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND;
            break;
        }
        auto vIdLen = stVidLen.value();
        auto partId = req.get_part_id();
        auto key = ConsistUtil::edgeKey(vIdLen, partId, req.get_edge_key());
        ver = ConsistUtil::getSingleEdgeVer(env->kvstore_, spaceId, partId, key);
    } while (0);

    return std::make_pair(ver, rc);
}

}  // namespace storage
}  // namespace nebula
