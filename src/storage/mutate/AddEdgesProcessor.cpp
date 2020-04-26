/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "storage/mutate/AddEdgesProcessor.h"
#include "common/NebulaKeyUtils.h"
#include <algorithm>
#include <limits>
#include "time/WallClock.h"
#include "codec/RowWriterV2.h"

namespace nebula {
namespace storage {

void AddEdgesProcessor::process(const cpp2::AddEdgesRequest& req) {
    auto version =
        std::numeric_limits<int64_t>::max() - time::WallClock::fastNowInMicroSec();
    // Switch version to big-endian, make sure the key is in ordered.
    version = folly::Endian::big(version);

    spaceId_ = req.get_space_id();
    const auto& partEdges = req.get_parts();
    const auto& propNames = req.get_prop_names();

    CHECK_NOTNULL(env_->schemaMan_);
    auto ret = env_->schemaMan_->getSpaceVidLen(spaceId_);
    if (!ret.ok()) {
        LOG(ERROR) << "Space " << spaceId_ << " VertexId length invalid."
                   << ret.status().toString();
        cpp2::PartitionResult thriftRet;
        thriftRet.set_code(cpp2::ErrorCode::E_INVALID_SPACEVIDLEN);
        codes_.emplace_back(std::move(thriftRet));
        onFinished();
        return;
    }
    auto spaceVidLen = ret.value();
    callingNum_ = req.parts.size();

    CHECK_NOTNULL(env_->indexMan_);
    auto iRet = env_->indexMan_->getEdgeIndexes(spaceId_);
    if (iRet.ok()) {
        indexes_ = std::move(iRet).value();
    }

    CHECK_NOTNULL(env_->kvstore_);
    if (indexes_.empty()) {
        for (auto& part : partEdges) {
            auto partId = part.first;
            const auto& newEdges = part.second;

            std::vector<kvstore::KV> data;
            data.reserve(32);
            for (auto& newEdge : newEdges) {
                auto edgeKey = newEdge.key;
                VLOG(3) << "PartitionID: " << partId << ", VertexID: " << edgeKey.src
                        << ", EdgeType: " << edgeKey.edge_type << ", EdgeRanking: "
                        << edgeKey.ranking << ", VertexID: "
                        << edgeKey.dst << ", EdgeVersion: " << version;

                auto key = NebulaKeyUtils::edgeKey(spaceVidLen, partId, edgeKey.src,
                                                   edgeKey.edge_type, edgeKey.ranking,
                                                   edgeKey.dst, version);
                auto schema = env_->schemaMan_->getEdgeSchema(spaceId_, edgeKey.edge_type);
                if (!schema) {
                    LOG(ERROR) << "Space " << spaceId_ << ", Edge "
                               << edgeKey.edge_type << " invalid";
                    pushResultCode(cpp2::ErrorCode::E_EDGE_NOT_FOUND, partId);
                    onFinished();
                    return;
                }

                auto props = newEdge.get_props();
                RowWriterV2 rowWrite(schema.get());
                // If req.prop_names is not empty, use the property name in req.prop_names
                // Otherwise, use property name in schema
                if (!propNames.empty()) {
                    for (size_t i = 0; i < propNames.size(); i++) {
                        auto wRet = rowWrite.setValue(propNames[i], props[i]);
                        if (wRet != WriteResult::SUCCEEDED) {
                            LOG(ERROR) << "Add edge faild";
                            pushResultCode(cpp2::ErrorCode::E_DATA_TYPE_MISMATCH, partId);
                            onFinished();
                            return;
                        }
                    }
                } else {
                    for (size_t i = 0; i < props.size(); i++) {
                        auto wRet = rowWrite.setValue(i, props[i]);
                        if (wRet != WriteResult::SUCCEEDED) {
                            LOG(ERROR) << "Add edge faild";
                            pushResultCode(cpp2::ErrorCode::E_DATA_TYPE_MISMATCH, partId);
                            onFinished();
                            return;
                        }
                    }
                }

                auto wRet = rowWrite.finish();
                if (wRet != WriteResult::SUCCEEDED) {
                    LOG(ERROR) << "Add edge faild";
                    pushResultCode(cpp2::ErrorCode::E_DATA_TYPE_MISMATCH, partId);
                    onFinished();
                    return;
                }

                std::string encode = std::move(rowWrite).moveEncodedStr();
                data.emplace_back(std::move(key), std::move(encode));
            }
            doPut(spaceId_, partId, std::move(data));
        }
    } else {
        for (auto& part : partEdges) {
            auto partId = part.first;
            auto atomic = [version, partId, edges = std::move(part.second), this]()
                          -> folly::Optional<std::string> {
                return this->addEdges(version, partId, edges);
            };
            auto callback = [partId, this](kvstore::ResultCode code) {
                this->handleAsync(this->spaceId_, partId, code);
            };
            env_->kvstore_->asyncAtomicOp(spaceId_, partId, atomic, callback);
        }
    }
}

std::string AddEdgesProcessor::addEdges(int64_t version, PartitionID partId,
                                        const std::vector<cpp2::NewEdge>& edges) {
    UNUSED(version);
    UNUSED(partId);
    UNUSED(edges);
#if 0
    std::unique_ptr<kvstore::BatchHolder> batchHolder = std::make_unique<kvstore::BatchHolder>();

    /*
     * Define the map newIndexes to avoid inserting duplicate edge.
     * This map means :
     * map<edge_unique_key, prop_value> ,
     * -- edge_unique_key is only used as the unique key , for example:
     * insert below edges in the same request:
     *     kv(part1_src1_edgeType1_rank1_dst1 , v1)
     *     kv(part1_src1_edgeType1_rank1_dst1 , v2)
     *     kv(part1_src1_edgeType1_rank1_dst1 , v3)
     *     kv(part1_src1_edgeType1_rank1_dst1 , v4)
     *
     * Ultimately, kv(part1_src1_edgeType1_rank1_dst1 , v4) . It's just what I need.
     */
    std::map<std::string, std::string> newEdges;
    std::for_each(edges.begin(), edges.end(), [&](auto& edge) {
        auto prop = edge.get_props();
        auto type = edge.key.edge_type;
        auto srcId = edge.key.src;
        auto rank = edge.key.ranking;
        auto dstId = edge.key.dst;
        VLOG(3) << "PartitionID: " << partId << ", VertexID: " << srcId
                << ", EdgeType: " << type << ", EdgeRanking: " << rank
                << ", VertexID: " << dstId << ", EdgeVersion: " << version;
        auto key = NebulaKeyUtils::edgeKey(partId, srcId, type, rank, dstId, version);
        newEdges[key] = std::move(prop);
    });
    for (auto& e : newEdges) {
        std::string val;
        std::unique_ptr<RowReader> nReader;
        auto edgeType = NebulaKeyUtils::getEdgeType(e.first);
        for (auto& index : indexes_) {
            if (edgeType == index->get_schema_id().get_edge_type()) {
                /*
                 * step 1 , Delete old version index if exists.
                 */
                if (val.empty()) {
                    val = findObsoleteIndex(partId, e.first);
                }
                if (!val.empty()) {
                    auto reader = RowReader::getEdgePropReader(this->schemaMan_,
                                                               val,
                                                               spaceId_,
                                                               edgeType);
                    auto oi = indexKey(partId, reader.get(), e.first, index);
                    if (!oi.empty()) {
                        batchHolder->remove(std::move(oi));
                    }
                }
                /*
                 * step 2 , Insert new edge index
                 */
                if (nReader == nullptr) {
                    nReader = RowReader::getEdgePropReader(this->schemaMan_,
                                                           e.second,
                                                           spaceId_,
                                                           edgeType);
                }
                auto ni = indexKey(partId, nReader.get(), e.first, index);
                batchHolder->put(std::move(ni), "");
            }
        }
        /*
         * step 3 , Insert new vertex data
         */
        auto key = e.first;
        auto prop = e.second;
        batchHolder->put(std::move(key), std::move(prop));
    }

    return encodeBatchValue(batchHolder->getBatch());
#endif
    return std::string("");
}

#if 0
std::string AddEdgesProcessor::findObsoleteIndex(PartitionID partId,
                                                 const folly::StringPiece& rawKey) {
    auto prefix = NebulaKeyUtils::edgePrefix(partId,
                                             NebulaKeyUtils::getSrcId(rawKey),
                                             NebulaKeyUtils::getEdgeType(rawKey),
                                             NebulaKeyUtils::getRank(rawKey),
                                             NebulaKeyUtils::getDstId(rawKey));
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(this->spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Error! ret = " << static_cast<int32_t>(ret)
                   << ", spaceId " << this->spaceId_;
        return "";
    }
    if (iter && iter->valid()) {
        return iter->val().str();
    }
    return "";
}

std::string AddEdgesProcessor::indexKey(PartitionID partId,
                                        RowReader* reader,
                                        const folly::StringPiece& rawKey,
                                        std::shared_ptr<nebula::cpp2::IndexItem> index) {
    auto values = collectIndexValues(reader, index->get_fields());
    return NebulaKeyUtils::edgeIndexKey(partId,
                                        index->get_index_id(),
                                        NebulaKeyUtils::getSrcId(rawKey),
                                        NebulaKeyUtils::getRank(rawKey),
                                        NebulaKeyUtils::getDstId(rawKey),
                                        values);
}
#endif

}  // namespace storage
}  // namespace nebula
