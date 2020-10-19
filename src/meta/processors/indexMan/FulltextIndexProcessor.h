/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_FULLTEXTINDEXPROCESSOR_H_
#define META_FULLTEXTINDEXPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class CreateFTIndexProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static CreateFTIndexProcessor* instance(kvstore::KVStore* kvstore) {
        return new CreateFTIndexProcessor(kvstore);
    }

    void process(const cpp2::CreateFTIndexReq& req);

private:
    explicit CreateFTIndexProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

class DropFTIndexProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static DropFTIndexProcessor* instance(kvstore::KVStore* kvstore) {
        return new DropFTIndexProcessor(kvstore);
    }

    void process(const cpp2::DropFTIndexReq& req);

private:
    explicit DropFTIndexProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

class ListFTIndicesProcessor : public BaseProcessor<cpp2::ListFTIndicesResp> {
public:
    static ListFTIndicesProcessor* instance(kvstore::KVStore* kvstore) {
        return new ListFTIndicesProcessor(kvstore);
    }

    void process(const cpp2::ListFTIndicesReq& req);

private:
    explicit ListFTIndicesProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::ListFTIndicesResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif   // META_FULLTEXTINDEXPROCESSOR_H_
