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

using LockGuard = MemoryLockGuard<std::string>;

class MemoryLockTest : public ::testing::Test {
protected:
};

TEST_F(MemoryLockTest, SimpleTest) {
    MemoryLockCore<std::string> mlock;
    {
        LockGuard lk1(&mlock, "1");
        EXPECT_TRUE(lk1);

        LockGuard lk2(&mlock, "1");
        EXPECT_FALSE(lk2);
    }
    {
        auto* lk1 = new LockGuard(&mlock, "1");
        EXPECT_TRUE(*lk1);

        std::vector<std::string> keys{"1", "2", "3"};
        LockGuard lk2(&mlock, keys);
        EXPECT_FALSE(lk2);

        delete lk1;
        LockGuard lk3(&mlock, keys);
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
