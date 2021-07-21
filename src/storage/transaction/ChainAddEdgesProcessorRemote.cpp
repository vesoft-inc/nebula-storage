/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/mutate/AddEdgesProcessor.h"
#include "storage/transaction/ChainAddEdgesProcessorRemote.h"
#include "storage/transaction/ConsistUtil.h"
#include "storage/transaction/TransactionManager.h"

namespace nebula {
namespace storage {

void ChainAddEdgesProcessorRemote::process(const cpp2::ChainAddEdgesRequest& req) {
    auto partId = req.get_parts().begin()->first;
    auto code = nebula::cpp2::ErrorCode::SUCCEEDED;
    do {
        if (!checkTerm(req)) {
            LOG(WARNING) << "invalid term";
            code = nebula::cpp2::ErrorCode::E_OUTDATED_TERM;
            break;
        }

        if (!checkVersion(req)) {
            code = nebula::cpp2::ErrorCode::E_OUTDATED_EDGE;
            break;
        }
    } while (0);

    if (code == nebula::cpp2::ErrorCode::SUCCEEDED) {
        forwardRequest(req);
    } else {
        pushResultCode(code, partId);
    }
    onFinished();
}

bool ChainAddEdgesProcessorRemote::checkTerm(const cpp2::ChainAddEdgesRequest& req) {
    auto partId = req.get_parts().begin()->first;
    return env_->txnMan_->checkTerm(req.get_space_id(), partId, req.get_term());
}

bool ChainAddEdgesProcessorRemote::checkVersion(const cpp2::ChainAddEdgesRequest& req) {
    if (!req.edge_version_ref()) {
        return true;
    }
    auto spaceId = req.get_space_id();
    auto partId = req.get_parts().begin()->first;
    auto strEdgeKeys = getStrEdgeKeys(req);
    auto currVer = ConsistUtil::getMultiEdgeVers(env_->kvstore_, spaceId, partId, strEdgeKeys);
    auto& edge_vers = *req.edge_version_ref();
    for (auto i = 0U; i != currVer.size(); ++i) {
        if (currVer[i] > edge_vers[i]) {
            return false;
        }
    }
    return true;
}

void ChainAddEdgesProcessorRemote::forwardRequest(const cpp2::ChainAddEdgesRequest& req) {
    auto* proc = AddEdgesProcessor::instance(env_);
    proc->getFuture().thenValue([&](auto&& resp){
        this->result_ = resp.get_result();
    });
    proc->process(ConsistUtil::makeDirectAddReq(req));
}

std::vector<std::string> ChainAddEdgesProcessorRemote::getStrEdgeKeys(
    const cpp2::ChainAddEdgesRequest& req) {
    std::vector<std::string> ret;
    for (auto& edgesOfPart : req.get_parts()) {
        auto partId = edgesOfPart.first;
        for (auto& edge : edgesOfPart.second) {
            ret.emplace_back(ConsistUtil::edgeKey(spaceVidLen_, partId, edge.get_key()));
        }
    }
    return ret;
}

}  // namespace storage
}  // namespace nebula
