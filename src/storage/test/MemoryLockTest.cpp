/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include <gtest/gtest.h>
#include "storage/transaction/MemoryLockWrapper.h"

namespace nebula {
namespace storage {

using Lock = SimpleMemoryLock<std::string>;

class MemoryLockTest : public ::testing::Test {
protected:
    PartitionID partId_ = 0;
    VertexID vId_ = "nebula";
};

TEST_F(MemoryLockTest, SimpleTest) {
    {
        Lock lk1("1");
        EXPECT_TRUE(lk1.isLock());
        EXPECT_TRUE(lk1);

        Lock lk2("1");
        EXPECT_FALSE(lk2.isLock());
    }
    {
        auto* lk1 = new Lock("1");
        EXPECT_TRUE(*lk1);

        std::vector<std::string> keys{"1", "2", "3"};
        Lock lk2(keys);
        EXPECT_FALSE(lk2);

        delete lk1;
        Lock lk3(keys);
        EXPECT_TRUE(lk3);
    }
}

}  // namespace storage
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
