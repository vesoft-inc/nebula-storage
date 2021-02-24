/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "codec/RowWriterV2.h"
#include "storage/StorageFlags.h"
#include "storage/mutate/AddEdgesAtomicProcessor.h"
#include "storage/transaction/TransactionManager.h"
#include "storage/transaction/ChainAddEdgeProcessor.h"

namespace nebula {
namespace storage {

ProcessorCounters kAddEdgesAtomicCounters;

void AddEdgesAtomicProcessor::process(const cpp2::AddEdgesRequest& req) {
    propNames_ = req.get_prop_names();
    spaceId_ = req.get_space_id();
    auto code = getSpaceVidLen(spaceId_);
    if (code != cpp2::ErrorCode::SUCCEEDED) {
        for (auto& part : req.get_parts()) {
            pushResultCode(cpp2::ErrorCode::E_INVALID_SPACEVIDLEN, part.first);
        }
        onFinished();
        return;
    }

    auto vidTypeStatus = env_->metaClient_->getSpaceVidType(spaceId_);
    if (!vidTypeStatus) {
        for (auto& part : req.get_parts()) {
            pushResultCode(cpp2::ErrorCode::E_INVALID_VID, part.first);
        }
        onFinished();
        return;
    }
    spaceVidType_ = std::move(vidTypeStatus).value();
    CHECK_LT(req.get_parts().begin()->second.front().get_key().get_edge_type(), 0);

    processInEdges(const_cast<cpp2::AddEdgesRequest&>(req));
}

void AddEdgesAtomicProcessor::processInEdges(cpp2::AddEdgesRequest& request) {
    if (request.get_parts().empty()) {
        this->onFinished();
        return;
    }
    activeSubRequest_ = request.get_parts().size();

    auto spaceId = request.get_space_id();
    auto propNames = request.get_prop_names();
    auto overwrittable = request.__isset.overwritable ? request.get_overwritable() : true;

    for (auto& part : request.parts) {
        auto cb = [&, partId = part.first](auto code) {
            if (code == cpp2::ErrorCode::E_LEADER_CHANGED) {
                handleLeaderChanged(spaceId_, partId);
            } else {
                pushResultCode(code, partId);
            }
            if (--activeSubRequest_ == 0) {
                this->onFinished();
            }
        };

        auto encoder = [&](auto& e) { return encodeEdge(e); };

        auto* chainAddEdgesProc = ChainAddEdgesProcessor::instance(std::move(cb),
                                                                   env_,
                                                                   spaceVidLen_,
                                                                   spaceId,
                                                                   part.first,
                                                                   part.second,
                                                                   propNames,
                                                                   overwrittable,
                                                                   spaceVidType_,
                                                                   std::move(encoder));

        env_->txnMan_->processChain(chainAddEdgesProc);
    }
}

std::pair<std::string, cpp2::ErrorCode> AddEdgesAtomicProcessor::encodeEdge(
    const cpp2::NewEdge& e) {
    auto edgeType = e.get_key().get_edge_type();
    auto schema = env_->schemaMan_->getEdgeSchema(spaceId_, std::abs(edgeType));
    if (!schema) {
        LOG(ERROR) << "Space " << spaceId_ << ", Edge " << edgeType << " invalid";
        return std::make_pair("", cpp2::ErrorCode::E_SPACE_NOT_FOUND);
    }

    WriteResult wRet;
    auto stVal = encodeRowVal(schema.get(), propNames_, e.get_props(), wRet);
    if (!stVal.ok()) {
        LOG(ERROR) << stVal.status();
        return std::make_pair("", cpp2::ErrorCode::E_DATA_TYPE_MISMATCH);
    }
    return std::make_pair(stVal.value(), cpp2::ErrorCode::SUCCEEDED);
}

}   // namespace storage
}   // namespace nebula
