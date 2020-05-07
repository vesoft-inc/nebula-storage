
/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_SIMPLEDAG_H_
#define STORAGE_EXEC_SIMPLEDAG_H_

#include "base/Base.h"
#include "storage/exec/Executor.h"
#include "storage/CommonUtils.h"

namespace nebula {
namespace storage {

class StorageDAG {
public:
    folly::Future<kvstore::ResultCode> go() {
        for (size_t i = 0; i < nodes_.size(); i++) {
            auto* node = nodes_[i].get();
            std::vector<folly::Future<kvstore::ResultCode>> futures;
            if (node->dependencies_.empty()) {
                node->addDependency(&input_);
            }
            for (auto dep : node->dependencies_) {
                futures.emplace_back(dep->promise_.getFuture());
            }

            folly::collectAll(futures)
                .via(workers_[i])
                .thenTry([node] (auto&& t) {
                    CHECK(!t.hasException());
                    for (const auto& codeTry : t.value()) {
                        if (codeTry.hasException()) {
                            node->promise_.setException(std::move(codeTry.exception()));
                            return;
                        } else if (codeTry.value() != kvstore::ResultCode::SUCCEEDED) {
                            node->promise_.setValue(codeTry.value());
                            return;
                        }
                    }
                    node->execute().thenValue([node] (auto&& code) {
                        node->promise_.setValue(code);
                    }).thenError([node] (auto&& ex) {
                        LOG(ERROR) << "Exception occurs, perhaps should not reach here: "
                                << ex.what();
                        node->promise_.setException(std::move(ex));
                    });
                });
        }

        input_.promise_.setValue(kvstore::ResultCode::SUCCEEDED);
        return nodes_[outputIdx_]->promise_.getFuture();
    }

    void setOutput(size_t idx) {
        outputIdx_ = idx;
    }

    size_t addNode(std::unique_ptr<Executor> node, folly::Executor* executor = nullptr) {
        workers_.emplace_back(executor);
        nodes_.emplace_back(std::move(node));
        return nodes_.size() - 1;
    }

    Executor* getNode(size_t idx) {
        CHECK_LT(idx, nodes_.size());
        return nodes_[idx].get();
    }

private:
    Executor input_;
    size_t outputIdx_;
    std::vector<folly::Executor*> workers_;
    std::vector<std::unique_ptr<Executor>> nodes_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_SIMPLEDAG_H_
