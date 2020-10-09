/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_CREATETAGINDEXPROCESSOR_H
#define META_CREATETAGINDEXPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class CreateTagIndexProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static CreateTagIndexProcessor* instance(kvstore::KVStore* kvstore) {
        return new CreateTagIndexProcessor(kvstore);
    }

    void process(const cpp2::CreateTagIndexReq& req);

private:
    explicit CreateTagIndexProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ExecResp>(kvstore) {}

    // For different type indexes, check whether the fields are legal
    cpp2::ErrorCode checkFields();

    cpp2::ErrorCode checkAndBuildIndex();

private:
    GraphSpaceID                     spaceId_;
    cpp2::IndexType                  indexType_;
    std::string                      indexName_;
    std::string                      tagName_;
    std::vector<std::string>         fieldNames_;

    TagID                            tagId_;
    std::vector<cpp2::ColumnDef>     columns_{};
};

}  // namespace meta
}  // namespace nebula

#endif  // META_CREATETAGINDEXPROCESSOR_H

