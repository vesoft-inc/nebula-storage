/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/time/WallClock.h"
#include "utils/NebulaKeyUtils.h"
#include "utils/IndexKeyUtils.h"
#include "utils/OperationKeyUtils.h"
#include <algorithm>
#include "codec/RowWriterV2.h"
#include "storage/mutate/AddEdgesProcessor.h"

namespace nebula {
namespace storage {

void AddEdgesProcessor::process(const cpp2::AddEdgesRequest& req) {
    auto version = FLAGS_enable_multi_versions ?
        std::numeric_limits<int64_t>::max() - time::WallClock::fastNowInMicroSec() : 0L;
    // Switch version to big-endian, make sure the key is in ordered.
    version = folly::Endian::big(version);

    spaceId_ = req.get_space_id();
    const auto& partEdges = req.get_parts();
    const auto& propNames = req.get_prop_names();

    CHECK_NOTNULL(env_->schemaMan_);
    auto ret = env_->schemaMan_->getSpaceVidLen(spaceId_);
    if (!ret.ok()) {
        LOG(ERROR) << ret.status();
        for (auto& part : partEdges) {
            pushResultCode(cpp2::ErrorCode::E_INVALID_SPACEVIDLEN, part.first);
        }
        onFinished();
        return;
    }

    spaceVidLen_ = ret.value();
    callingNum_ = partEdges.size();

    CHECK_NOTNULL(env_->indexMan_);
    handleEdgeIndexes(spaceId_);

    CHECK_NOTNULL(env_->kvstore_);
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

            if (!NebulaKeyUtils::isValidVidLen(
                    spaceVidLen_, edgeKey.src.getStr(), edgeKey.dst.getStr())) {
                LOG(ERROR) << "Space " << spaceId_ << " vertex length invalid, "
                           << "space vid len: " << spaceVidLen_ << ", edge srcVid: " << edgeKey.src
                           << ", dstVid: " << edgeKey.dst;
                pushResultCode(cpp2::ErrorCode::E_INVALID_VID, partId);
                onFinished();
                return;
            }

            auto key = NebulaKeyUtils::edgeKey(spaceVidLen_,
                                               partId,
                                               edgeKey.src.getStr(),
                                               edgeKey.edge_type,
                                               edgeKey.ranking,
                                               edgeKey.dst.getStr(),
                                               version);
            auto schema = env_->schemaMan_->getEdgeSchema(spaceId_,
                                                          std::abs(edgeKey.edge_type));
            if (!schema) {
                LOG(ERROR) << "Space " << spaceId_ << ", Edge "
                           << edgeKey.edge_type << " invalid";
                pushResultCode(cpp2::ErrorCode::E_EDGE_NOT_FOUND, partId);
                onFinished();
                return;
            }

            auto props = newEdge.get_props();
            WriteResult wRet;
            auto retEnc = encodeRowVal(schema.get(), propNames, props, wRet);
            if (!retEnc.ok()) {
                LOG(ERROR) << retEnc.status();
                pushResultCode(writeResultTo(wRet, true), partId);
                onFinished();
                return;
            }

            data.emplace_back(std::move(key), std::move(retEnc.value()));
        }
        if (indexes_.empty() && edgeIndexes_.empty() && allEdgeStatIndex_ == nullptr) {
            doPut(spaceId_, partId, std::move(data));
        } else {
            auto atomic = [partId, edges = std::move(data), this]()
                          -> folly::Optional<std::string> {
                return addEdges(partId, edges);
            };

            auto callback = [partId, this](kvstore::ResultCode code) {
                handleAsync(spaceId_, partId, code);
            };
            env_->kvstore_->asyncAtomicOp(spaceId_, partId, atomic, callback);
        }
    }
}

folly::Optional<std::string>
AddEdgesProcessor::addEdges(PartitionID partId,
                            const std::vector<kvstore::KV>& edges) {
    env_->onFlyingRequest_.fetch_add(1);
    std::unique_ptr<kvstore::BatchHolder> batchHolder = std::make_unique<kvstore::BatchHolder>();
    int64_t countVal = 0;

    /*
     * Define the map newEdges to avoid inserting duplicate edge.
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
    std::unordered_map<std::string, std::string> newEdges;
    std::for_each(edges.begin(), edges.end(),
                 [&newEdges](const std::map<std::string, std::string>::value_type& e)
                 { newEdges[e.first] = e.second; });

    for (auto& e : newEdges) {
        std::string val;
        RowReaderWrapper oReader;
        RowReaderWrapper nReader;
        auto edgeType = NebulaKeyUtils::getEdgeType(spaceVidLen_, e.first);
        auto srcId = NebulaKeyUtils::getSrcId(spaceVidLen_, e.first).str();
        auto rank = NebulaKeyUtils::getRank(spaceVidLen_, e.first);
        auto dstId = NebulaKeyUtils::getDstId(spaceVidLen_, e.first).str();

        // Normal index
        for (auto& index : indexes_) {
            if (edgeType == index->get_schema_id().get_edge_type()) {
                auto indexId = index->get_index_id();
                /*
                 * Step 1 , Delete old version normal index if exists.
                 */
                if (val.empty()) {
                    auto obsIdx = findOldValue(partId, srcId, edgeType, rank, dstId);
                    if (obsIdx == folly::none) {
                        return folly::none;
                    }
                    val = std::move(obsIdx).value();
                    if (!val.empty()) {
                        // put keyExist_, key exist
                        keyExist_[std::make_tuple(srcId, edgeType, rank, dstId)] = true;
                        oReader = RowReaderWrapper::getEdgePropReader(env_->schemaMan_,
                                                                      spaceId_,
                                                                      edgeType,
                                                                      val);
                        if (oReader == nullptr) {
                            LOG(ERROR) << "Bad format row";
                            return folly::none;
                        }
                    } else {
                        // put keyExist_, key not exist
                        keyExist_[std::make_tuple(srcId, edgeType, rank, dstId)] = false;
                    }
                }

                if (!val.empty()) {
                    auto oi = normalIndexKey(partId, oReader.get(), srcId, rank, dstId, index);
                    if (!oi.empty()) {
                        // Check the index is building for the specified partition or not.
                        if (env_->checkRebuilding(spaceId_, partId, indexId)) {
                            auto deleteOpKey = OperationKeyUtils::deleteOperationKey(partId);
                            batchHolder->put(std::move(deleteOpKey), std::move(oi));
                        } else if (env_->checkIndexLocked(spaceId_, partId, indexId)) {
                            LOG(ERROR) << "The index has been locked: " << index->get_index_name();
                            return folly::none;
                        } else {
                            batchHolder->remove(std::move(oi));
                        }
                    }
                }

                /*
                 * Step 2 , Insert new edge normal index
                 */
                if (nReader == nullptr) {
                    nReader = RowReaderWrapper::getEdgePropReader(env_->schemaMan_,
                                                                  spaceId_,
                                                                  edgeType,
                                                                  e.second);
                    if (nReader == nullptr) {
                        LOG(ERROR) << "Bad format row";
                        return folly::none;
                    }
                }

                auto ni = normalIndexKey(partId, nReader.get(), srcId, rank, dstId, index);
                if (!ni.empty()) {
                    auto retRebuild = rebuildingModifyOp(spaceId_,
                                                         partId,
                                                         indexId,
                                                         ni,
                                                         batchHolder.get());
                    if (retRebuild == folly::none) {
                        return folly::none;
                    }
                }
            }
        }

        /*
         * Step 3, insert edge index data
         * To simplify, directly overwrite
         * TODO(pandasheep) handle ttl in index
         */
        auto eIndexIt = edgeTypeToIndexId_.find(edgeType);
        if (eIndexIt != edgeTypeToIndexId_.end()) {
            auto indexId = eIndexIt->second;
            auto eIndexKey = StatisticsIndexKeyUtils::edgeIndexKey(spaceVidLen_,
                                                                      partId,
                                                                      indexId,
                                                                      srcId,
                                                                      rank,
                                                                      dstId);
            if (!eIndexKey.empty()) {
                auto retRebuild = rebuildingModifyOp(spaceId_,
                                                     partId,
                                                     indexId,
                                                     eIndexKey,
                                                     batchHolder.get());
                if (retRebuild == folly::none) {
                    return folly::none;
                }
            }
        }

        /*
         * Step 4, Insert new vertex data
         */
        auto key = e.first;
        auto prop = e.second;
        batchHolder->put(std::move(key), std::move(prop));

        /*
         * Step 5, for statistics all edge count
         * Only count the number of positive edges
         */
        if (allEdgeStatIndex_ != nullptr && edgeType > 0) {
            // First check if exist in keyExist_
            auto existKey = std::make_tuple(srcId, edgeType, rank, dstId);
            auto keyIt = keyExist_.find(existKey);
            if (keyIt != keyExist_.end()) {
                if (!keyIt->second) {
                    countVal++;
                }
            } else {
                // There is no normal index
                // Find it again to see if it exists
                auto prefix = NebulaKeyUtils::edgePrefix(spaceVidLen_,
                                                         partId, srcId, edgeType, rank, dstId);
                std::unique_ptr<kvstore::KVIterator> iter;
                auto ret = env_->kvstore_->prefix(spaceId_, partId, prefix, &iter);
                if (ret != kvstore::ResultCode::SUCCEEDED) {
                    LOG(ERROR) << "Error! ret = " << static_cast<int32_t>(ret)
                               << ", spaceId " << spaceId_;
                    return folly::none;
                }
                if (iter == nullptr || !(iter->valid())) {
                    countVal++;
                }
            }
        }
    }

    /*
     * step 6, upsert statistic all edge index data
     */
    if (allEdgeStatIndex_ != nullptr) {
        auto edgeCountIndexId = allEdgeStatIndex_->get_index_id();
        if (countVal != 0) {
            std::string val;
            auto eCountIndexKey = StatisticsIndexKeyUtils::countIndexKey(partId, edgeCountIndexId);
            auto ret = env_->kvstore_->get(spaceId_, partId, eCountIndexKey, &val);
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                if (ret != kvstore::ResultCode::ERR_KEY_NOT_FOUND) {
                    LOG(ERROR) << "Get statistics index error";
                    return folly::none;
                }
                // key does not exist
            } else {
                countVal += *reinterpret_cast<const int64_t*>(val.c_str());
            }

            if (!eCountIndexKey.empty()) {
                auto newCount = std::string(reinterpret_cast<const char*>(&countVal),
                                            sizeof(int64_t));
                auto retRebuild = rebuildingModifyOp(spaceId_,
                                                     partId,
                                                     edgeCountIndexId,
                                                     eCountIndexKey,
                                                     batchHolder.get(),
                                                     newCount);
                if (retRebuild == folly::none) {
                    return folly::none;
                }
            }
        }
    }

    return encodeBatchValue(batchHolder->getBatch());
}

folly::Optional<std::string>
AddEdgesProcessor::findOldValue(PartitionID partId,
                                VertexID srcId,
                                EdgeType eType,
                                EdgeRanking rank,
                                VertexID dstId) {
    auto prefix = NebulaKeyUtils::edgePrefix(spaceVidLen_, partId, srcId, eType, rank, dstId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env_->kvstore_->prefix(spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Error! ret = " << static_cast<int32_t>(ret)
                   << ", spaceId " << spaceId_;
        return folly::none;
    }
    if (iter && iter->valid()) {
        return iter->val().str();
    }
    return std::string();
}

std::string
AddEdgesProcessor::normalIndexKey(PartitionID partId,
                                  RowReader* reader,
                                  VertexID srcId,
                                  EdgeRanking rank,
                                  VertexID dstId,
                                  std::shared_ptr<nebula::meta::cpp2::IndexItem> index) {
    std::vector<Value::Type> colsType;
    auto values = IndexKeyUtils::collectIndexValues(reader, index->get_fields(), colsType);
    if (!values.ok()) {
        return "";
    }
    return IndexKeyUtils::edgeIndexKey(spaceVidLen_, partId,
                                       index->get_index_id(),
                                       srcId,
                                       rank,
                                       dstId,
                                       values.value(), colsType);
}

}  // namespace storage
}  // namespace nebula
