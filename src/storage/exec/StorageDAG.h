/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_SIMPLEDAG_H_
#define STORAGE_EXEC_SIMPLEDAG_H_

#include "base/Base.h"
#include "storage/exec/RelNode.h"
#include "storage/CommonUtils.h"

namespace nebula {
namespace storage {

/*
Origined from folly::FutureDAG, not thread-safe.

The StorageDAG defines a simple dag, all you need to do is define a RelNode, add it to dag by
calling addNode, which will return the index of the RelNode in this dag. The denpendencies
between different nodes is defined by calling addDependency in RelNode.

To run the dag, call the go method, you could get the Future of finale result. Once you run the dag,
the structure can't be changed anymore.

For simplicity, StorageDAG has not detect if has cycle in it for now, user must make sure no cycle
dependency in it, otherwise the promise would never be fulfilled.
*/
class StorageDAG {
public:
    folly::Future<kvstore::ResultCode> go(PartitionID partId, const VertexID& vId) {
        // find the root nodes and leaf nodes. All root nodes depends on a dummy input node,
        // and a dummy output node depends on all leaf node.
        if (firstLoop_) {
            auto output = std::make_unique<RelNode>();
            auto input = std::make_unique<RelNode>();
            for (const auto& node : nodes_) {
                if (node->dependencies_.empty()) {
                    // add dependency of root node
                    node->addDependency(input.get());
                }
                if (!node->hasDependents_) {
                    // add dependency of output node
                    output->addDependency(node.get());
                }
            }
            outputIdx_ = addNode(std::move(output));
            inputIdx_ = addNode(std::move(input));
            firstLoop_ = false;
        }

        for (size_t i = 0; i < nodes_.size() - 1; i++) {
            auto* node = nodes_[i].get();
            std::vector<folly::Future<kvstore::ResultCode>> futures;
            for (auto dep : node->dependencies_) {
                futures.emplace_back(dep->promise_.getFuture());
            }

            folly::collectAll(futures)
                .via(workers_[i])
                .thenTry([node, partId, &vId] (auto&& t) {
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
                    node->execute(partId, vId).thenValue([node] (auto&& code) {
                        node->promise_.setValue(code);
                    }).thenError([node] (auto&& ex) {
                        LOG(ERROR) << "Exception occurs, perhaps should not reach here: "
                                    << ex.what();
                        node->promise_.setException(std::move(ex));
                    });
                });
        }

        getNode(inputIdx_)->promise_.setValue(kvstore::ResultCode::SUCCEEDED);
        return getNode(outputIdx_)->promise_.getFuture()
            .thenTry([this](auto&& t) -> folly::Future<kvstore::ResultCode> {
                CHECK(!t.hasException());
                reset();
                return t.value();
            });
    }

    size_t addNode(std::unique_ptr<RelNode> node, folly::Executor* executor = nullptr) {
        workers_.emplace_back(executor);
        nodes_.emplace_back(std::move(node));
        return nodes_.size() - 1;
    }

    RelNode* getNode(size_t idx) {
        CHECK_LT(idx, nodes_.size());
        return nodes_[idx].get();
    }

private:
    // reset all promise after dag has run
    void reset() {
        for (auto& node : nodes_) {
            node->promise_ = std::move(folly::SharedPromise<kvstore::ResultCode>());
        }
    }

    bool firstLoop_ = true;
    size_t inputIdx_;
    size_t outputIdx_;
    std::vector<folly::Executor*> workers_;
    std::vector<std::unique_ptr<RelNode>> nodes_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_SIMPLEDAG_H_
