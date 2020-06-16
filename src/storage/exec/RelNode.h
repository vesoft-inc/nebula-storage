/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_RELNODE_H_
#define STORAGE_EXEC_RELNODE_H_

#include "common/base/Base.h"
#include "common/context/ExpressionContext.h"
#include "utils/NebulaKeyUtils.h"
#include "storage/CommonUtils.h"
#include "storage/query/QueryBaseProcessor.h"
#include "storage/exec/QueryUtils.h"
#include "storage/exec/StorageIterator.h"

namespace nebula {
namespace storage {

using NullHandler = std::function<kvstore::ResultCode(const std::vector<PropContext>*)>;

using TagPropHandler = std::function<kvstore::ResultCode(TagID,
                                                         RowReader*,
                                                         const std::vector<PropContext>* props)>;

using EdgePropHandler = std::function<kvstore::ResultCode(EdgeType,
                                                          folly::StringPiece,
                                                          RowReader*,
                                                          const std::vector<PropContext>* props)>;

template<typename T> class StoragePlan;

// RelNode is shortcut for relational algebra node, each RelNode has an execute method,
// which will be invoked in dag when all its dependencies have finished
template<typename T>
class RelNode {
    friend class StoragePlan<T>;
public:
    virtual kvstore::ResultCode execute(PartitionID partId, const T& input) {
        DVLOG(1) << name_;
        for (auto* dependency : dependencies_) {
            auto ret = dependency->execute(partId, input);
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                return ret;
            }
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

    void addDependency(RelNode<T>* dep) {
        dependencies_.emplace_back(dep);
        dep->hasDependents_ = true;
    }

    RelNode() = default;

    virtual ~RelNode() = default;

    explicit RelNode(const std::string& name): name_(name) {}

    std::string name_;
    std::vector<RelNode<T>*> dependencies_;
    bool hasDependents_ = false;
};

// QueryNode is the node which would read data from kvstore, it usually generate a row in response
// or a cell in a row.
template<typename T>
class QueryNode : public RelNode<T> {
public:
    const Value& result() {
        return result_;
    }

    Value& mutableResult() {
        return result_;
    }

protected:
    kvstore::ResultCode collectEdgeProps(VertexIDSlice srcId,
                                         EdgeType edgeType,
                                         EdgeRanking edgeRank,
                                         VertexIDSlice dstId,
                                         RowReader* reader,
                                         const std::vector<PropContext>* props,
                                         nebula::List& list) {
        for (size_t i = 0; i < props->size(); i++) {
            const auto& prop = (*props)[i];
            if (prop.returned_) {
                VLOG(2) << "Collect prop " << prop.name_ << ", type " << edgeType;
                auto value = QueryUtils::readEdgeProp(srcId, edgeType, edgeRank, dstId,
                                                      reader, prop);
                list.values.emplace_back(std::move(value));
            }
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

    kvstore::ResultCode collectTagProps(TagID tagId,
                                        RowReader* reader,
                                        const std::vector<PropContext>* props,
                                        nebula::List& list,
                                        ExpressionContext* ctx) {
        for (auto& prop : *props) {
            VLOG(2) << "Collect prop " << prop.name_ << ", type " << tagId;
            if (reader != nullptr) {
                auto status = QueryUtils::readValue(reader, prop);
                if (!status.ok()) {
                    return kvstore::ResultCode::ERR_TAG_PROP_NOT_FOUND;
                }
                auto value = std::move(status.value());
                if (ctx != nullptr && prop.tagFiltered_) {
                    // todo(doodle): put data into ExpressionContxt
                }
                if (prop.returned_) {
                    list.values.emplace_back(std::move(value));
                }
            }
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

    Value result_;
};

// IterateEdgeNode is a typical volcano node, it will have a upstream node.
// It keeps moving forward the iterator by calling `next`, if you need to filter some data,
// implement the `check` just like FilterNode and HashJoinNode.
template<typename T>
class IterateEdgeNode : public QueryNode<T>, public EdgeIterator {
public:
    IterateEdgeNode() = default;

    explicit IterateEdgeNode(IterateEdgeNode* node) : upstream_(node) {}

    bool valid() const override {
        return upstream_->valid();
    }

    void next() override {
        do {
            upstream_->next();
        } while (upstream_->valid() && !check());
    }

    folly::StringPiece key() const override {
        return upstream_->key();
    }

    folly::StringPiece val() const override {
        return upstream_->val();
    }

    VertexID srcId() const override {
        return upstream_->srcId();
    }

    EdgeType edgeType() const override {
        return upstream_->edgeType();
    }

    EdgeRanking edgeRank() const override {
        return upstream_->edgeRank();
    }

    VertexID dstId() const override {
        return upstream_->dstId();
    }

    // return the edge row reader which could pass filter
    RowReader* reader() const override {
        return upstream_->reader();
    }

    // return the column index in result row, used for GetNeighbors
    virtual size_t idx() const {
        return upstream_->idx();
    }

    // return the edge props need to return
    virtual const std::vector<PropContext>* props() const {
        return upstream_->props();
    }

protected:
    // return true when the iterator points to a valid value
    virtual bool check() {
        return true;
    }

    IterateEdgeNode* upstream_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_RELNODE_H_
