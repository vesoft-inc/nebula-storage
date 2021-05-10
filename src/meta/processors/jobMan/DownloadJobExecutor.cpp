/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/hdfs/HdfsHelper.h"
#include "meta/MetaServiceUtils.h"
#include "meta/processors/jobMan/DownloadJobExecutor.h"

namespace nebula {
namespace meta {

DownloadJobExecutor::DownloadJobExecutor(JobID jobId,
                                         kvstore::KVStore* kvstore,
                                         AdminClient* adminClient,
                                         const std::vector<std::string>& paras)
    : MetaJobExecutor(jobId, kvstore, adminClient, paras) {
        helper_ = std::make_unique<nebula::hdfs::HdfsCommandHelper>();
    }

bool DownloadJobExecutor::check() {
    if (paras_.size() != 2) {
        return false;
    }

    auto spaceRet = getSpaceIdFromName(paras_[1]);
    if (!nebula::ok(spaceRet)) {
        LOG(ERROR) << "Can't find the space: " << paras_[1];
        return false;
    }
    space_ = nebula::value(spaceRet);

    auto& url = paras_[0];
    std::string hdfsPrefix = "hdfs://";
    if (url.find(hdfsPrefix) != 0) {
        LOG(ERROR) << "URL should start with " << hdfsPrefix;
        return false;
    }

    auto u = url.substr(hdfsPrefix.size(), url.size());
    std::vector<folly::StringPiece> tokens;
    folly::split(":", u, tokens);
    if (tokens.size() == 2) {
        host_ = std::make_unique<std::string>(tokens[0]);
        int32_t position = tokens[1].find_first_of("/");
        if (position != -1) {
            try {
                port_ = folly::to<int32_t>(tokens[1].toString().substr(0, position).c_str());
            } catch (const std::exception& ex) {
                LOG(ERROR) << "URL's port parse failed: " << url;
                return false;
            }
            path_ = std::make_unique<std::string>(
                        tokens[1].toString().substr(position, tokens[1].size()));
        } else {
            LOG(ERROR) << "URL Parse Failed: " << url;
            return false;
        }
    } else {
        LOG(ERROR) << "URL Parse Failed: " << url;
        return false;
    }

    return true;
}

nebula::cpp2::ErrorCode DownloadJobExecutor::prepare() {
    auto errOrHost = getTargetHost(space_);
    if (!nebula::ok(errOrHost)) {
        LOG(ERROR) << "Can't get any host according to space";
        return nebula::error(errOrHost);
    }

    LOG(INFO) << "HDFS host: " << *host_.get()
              << " port: " << port_
              << " path: " << *path_.get();

    auto listResult = helper_->ls(*host_.get(), port_, *path_.get());
    if (!listResult.ok()) {
        LOG(ERROR) << "Dispatch SSTFile Failed";
        return nebula::cpp2::ErrorCode::E_INVALID_JOB;
    }

    taskParas_.emplace_back(*host_.get());
    taskParas_.emplace_back(folly::to<std::string>(port_));
    taskParas_.emplace_back(*path_.get());
    std::unique_ptr<kvstore::KVIterator> iter;
    auto prefix = MetaServiceUtils::partPrefix(space_);
    auto result = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (result != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Fetch Parts Failed";
        return result;
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

folly::Future<Status> DownloadJobExecutor::executeInternal(HostAddr&& address,
                                                           std::vector<PartitionID>&& parts) {
    taskParas_.resize(3);
    return adminClient_->addTask(cpp2::AdminCmd::DOWNLOAD, jobId_, taskId_++, space_,
                                 {std::move(address)}, taskParas_, std::move(parts), concurrency_);
}

nebula::cpp2::ErrorCode DownloadJobExecutor::stop() {
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace meta
}  // namespace nebula
