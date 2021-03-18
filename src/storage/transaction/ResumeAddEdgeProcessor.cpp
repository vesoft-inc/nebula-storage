/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/transaction/ResumeAddEdgeProcessor.h"

namespace nebula {
namespace storage {

ResumeAddEdgeProcessor::ResumeAddEdgeProcessor(StorageEnv* env, const std::string& val)
    : ChainAddEdgesProcessorLocal(env) {
    req_ = ConsistUtil::parseAddRequest(val);
    ChainAddEdgesProcessorLocal::prepareRequest(req_);
}

folly::SemiFuture<nebula::cpp2::ErrorCode> ResumeAddEdgeProcessor::prepareLocal() {
    if (code_ != Code::SUCCEEDED) {
        return code_;
    }
    auto spaceId = req_.get_space_id();
    std::vector<std::string> keys = sEdgeKey(req_);
    auto vers = ConsistUtil::getMultiEdgeVers(env_->kvstore_, spaceId, localPartId_, keys);
    edgeVer_ = vers.front();

    return Code::SUCCEEDED;
}

folly::SemiFuture<Code> ResumeAddEdgeProcessor::processRemote(Code code) {
    return ChainAddEdgesProcessorLocal::processRemote(code);
}

folly::SemiFuture<Code> ResumeAddEdgeProcessor::processLocal(Code code) {
    setErrorCode(code);

    if (!checkTerm(req_)) {
        LOG(WARNING) << "E_OUTDATED_TERM";
        return Code::E_OUTDATED_TERM;
    }

    if (!checkVersion(req_)) {
        LOG(WARNING) << "E_OUTDATED_EDGE";
        return Code::E_OUTDATED_EDGE;
    }

    if (code == Code::E_RPC_FAILURE) {
        kvAppend_ = ChainAddEdgesProcessorLocal::makeDoublePrime();
    }

    if (code == Code::E_RPC_FAILURE || code == Code::SUCCEEDED) {
        // if there are something wrong other than rpc failure
        // we need to keep the resume retry(by not remove those prime key)
        erasePrime();
        return ChainAddEdgesProcessorLocal::forwardToDelegateProcessor();
    }

    return code;
}

}  // namespace storage
}  // namespace nebula
