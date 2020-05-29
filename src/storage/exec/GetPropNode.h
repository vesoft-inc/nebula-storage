/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_GETPROPNODE_H_
#define STORAGE_EXEC_GETPROPNODE_H_

#include "common/base/Base.h"
#include "storage/exec/TagNode.h"
#include "storage/exec/EdgeNode.h"

namespace nebula {
namespace storage {

class GetTagPropNode : public QueryNode<VertexID> {
public:
    GetTagPropNode(std::vector<TagNode*> tagNodes,
                   nebula::DataSet* result)
        : tagNodes_(std::move(tagNodes))
        , result_(result) {}

    kvstore::ResultCode execute(PartitionID partId, const VertexID& vId) override {
        auto ret = RelNode::execute(partId, vId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        nebula::Row row;
        for (auto* tagNode : tagNodes_) {
            ret = tagNode->collectTagPropsIfValid(
                [&row] (const std::vector<PropContext>* props) -> kvstore::ResultCode {
                    for (const auto& prop : *props) {
                        if (prop.returned_) {
                            row.columns.emplace_back(NullType::__NULL__);
                        }
                    }
                    return kvstore::ResultCode::SUCCEEDED;
                },
                [this, &row]
                (TagID tagId, RowReader* reader, const std::vector<PropContext>* props)
                -> kvstore::ResultCode {
                    nebula::List list;
                    auto code = collectTagProps(tagId, reader, props, list, nullptr);
                    if (code != kvstore::ResultCode::SUCCEEDED) {
                        return code;
                    }
                    for (auto& col : list.values) {
                        row.columns.emplace_back(std::move(col));
                    }
                    return kvstore::ResultCode::SUCCEEDED;
                });
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                return ret;
            }
        }
        result_->rows.emplace_back(std::move(row));
        return kvstore::ResultCode::SUCCEEDED;
    }

private:
    std::vector<TagNode*> tagNodes_;
    nebula::DataSet* result_;

    std::unique_ptr<RowReader> reader_;
};

class GetEdgePropNode : public QueryNode<cpp2::EdgeKey> {
public:
    GetEdgePropNode(std::vector<EdgeNode<cpp2::EdgeKey>*> edgeNodes,
                    size_t vIdLen,
                    nebula::DataSet* result)
        : edgeNodes_(std::move(edgeNodes))
        , vIdLen_(vIdLen)
        , result_(result) {
    }

    kvstore::ResultCode execute(PartitionID partId, const cpp2::EdgeKey& edgeKey) override {
        auto ret = RelNode::execute(partId, edgeKey);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        nebula::Row row;
        for (auto* edgeNode : edgeNodes_) {
            ret = edgeNode->collectEdgePropsIfValid(
                [&row] (const std::vector<PropContext>* props) -> kvstore::ResultCode {
                    for (const auto& prop : *props) {
                        if (prop.returned_) {
                            row.columns.emplace_back(NullType::__NULL__);
                        }
                    }
                    return kvstore::ResultCode::SUCCEEDED;
                },
                [this, &row] (EdgeType edgeType, folly::StringPiece key,
                              RowReader* reader, const std::vector<PropContext>* props)
                -> kvstore::ResultCode {
                    nebula::List list;
                    auto srcId = NebulaKeyUtils::getSrcId(vIdLen_, key);
                    auto edgeRank = NebulaKeyUtils::getRank(vIdLen_, key);
                    auto dstId = NebulaKeyUtils::getDstId(vIdLen_, key);
                    auto code = collectEdgeProps(srcId, edgeType, edgeRank, dstId,
                                                 reader, props, list);
                    if (code != kvstore::ResultCode::SUCCEEDED) {
                        return code;
                    }
                    for (auto& col : list.values) {
                        row.columns.emplace_back(std::move(col));
                    }
                    return kvstore::ResultCode::SUCCEEDED;
                });
        }
        result_->rows.emplace_back(std::move(row));
        return kvstore::ResultCode::SUCCEEDED;
    }

private:
    std::vector<EdgeNode<cpp2::EdgeKey>*> edgeNodes_;
    size_t vIdLen_;
    nebula::DataSet* result_;

    std::unique_ptr<RowReader> reader_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_GETPROPNODE_H_
