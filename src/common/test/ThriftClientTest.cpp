/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "thrift/ThriftClientManager.h"
#include "interface/gen-cpp2/StorageAdminServiceAsyncClient.h"

namespace nebula {

TEST(ThriftClientManTest, CreateDestroyTest) {
    // int i = 10;
    // while (i-- > 0) {
        auto clientsMan = std::make_unique<
                thrift::ThriftClientManager<storage::cpp2::StorageAdminServiceAsyncClient>>();
        auto* p = new folly::EventBase();
        std::unique_ptr<folly::EventBase> evb(p);
        LOG(INFO) << evb.get() << " ----> " << &evb;
        auto client = clientsMan->client(HostAddr(0, 0), evb.get());
        evb.reset();
        LOG(INFO) << "Reset evb, evb = " << evb.get() << ", p = " << p;
        CHECK(evb == nullptr);
        auto *q = new folly::EventBase();
        LOG(INFO) << "q = " << q << ", p = " << p;
        // memset(reinterpret_cast<void*>(p), 0, sizeof(folly::EventBase));
        //LOG(INFO) << "Reset client";
        //client.reset();
        LOG(INFO) << "Reset client manager";
        clientsMan.reset();
        delete q;
    // }
}

}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
