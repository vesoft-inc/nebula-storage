/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include <utility>

#include "base/Base.h"
#include <gtest/gtest.h>
#include "kvstore/wal/AtomicLogBuffer.h"

namespace nebula {
namespace wal {

TEST(AtomicLogBufferTest, ReadWriteTest) {
    auto logBuffer = AtomicLogBuffer::instance();
    for (LogID logId = 0; logId < 1000L; logId++) {
        logBuffer->push(logId, Record(0, 0, folly::stringPrintf("str_%ld", logId)));
    }
    {
        LogID from = 200;
        auto iter = logBuffer->iterator(from, 1000);
        while (iter->valid()) {
            auto* rec = iter->record();
            CHECK_NOTNULL(rec);
            ASSERT_EQ(folly::stringPrintf("str_%ld", from), rec->msg_);
            iter->next();
            from++;
        }
        EXPECT_EQ(1000, from);
    }
    {
        LogID from = 200;
        auto iter = logBuffer->iterator(from, 1500);
        while (iter->valid()) {
            auto* rec = iter->record();
            CHECK_NOTNULL(rec);
            ASSERT_EQ(folly::stringPrintf("str_%ld", from), rec->msg_);
            iter->next();
            from++;
        }
        EXPECT_EQ(1000, from);
    }
    {
        LogID from = 200;
        auto iter = logBuffer->iterator(from, 800);
        while (iter->valid()) {
            auto* rec = iter->record();
            CHECK_NOTNULL(rec);
            ASSERT_EQ(folly::stringPrintf("str_%ld", from), rec->msg_);
            iter->next();
            from++;
        }
        EXPECT_EQ(801, from);
    }
    {
        LogID from = 1200;
        auto iter = logBuffer->iterator(from, 1800);
        CHECK(!iter->valid());
    }
}

TEST(AtomicLogBufferTest, SingleWriterMultiReadersTest) {
    // The default size is 100K
    auto logBuffer = AtomicLogBuffer::instance(100 * 1024);
    std::atomic<LogID> writePoint{0};
    std::thread writer([logBuffer, &writePoint] {
        LOG(INFO) << "Begin write 1M records";
        for (LogID logId = 0; logId < 1000000L; logId++) {
            logBuffer->push(logId, Record(0, 0, folly::stringPrintf("str_%ld", logId)));
            writePoint.store(logId, std::memory_order_release);
        }
        LOG(INFO) << "Finish writer";
    });

    std::vector<std::thread> readers;
    for (int i = 0; i < 5; i++) {
        readers.emplace_back([i, logBuffer, &writePoint]{
            usleep(10);
            LOG(INFO) << "Start reader " << i;
            int times = 10000;
            int validSeek = 0;
            while (times-- > 0) {
                auto wp = writePoint.load(std::memory_order_acquire) - 1;
                auto start = folly::Random::rand32(logBuffer->firstLogId(), wp);
                auto end = start + folly::Random::rand32(1000);
                auto iter = logBuffer->iterator(start, end);
                if (!iter->valid()) {
                    continue;
                }
                validSeek++;
                size_t num = start;
                while (iter->valid()) {
                    const auto* rec = iter->record();
                    auto logId = iter->logId();
                    auto* node = iter->currNode();
                    CHECK_NOTNULL(rec);
                    auto expected = folly::stringPrintf("str_%ld", num);
                    EXPECT_EQ(num, logId);
                    EXPECT_EQ(expected.size(), rec->msg_.size())
                        << "wp " << wp << ", start " << start
                        << ", logId " << logId << ", end " << end
                        << ", curr node " << node->firstLogId_ << ", pos " << node->pos_
                        << ", curr index " << iter->currIndex()
                        << ", head lastLodId " << logBuffer->lastLogId();
                    EXPECT_EQ(expected, rec->msg_) << "expected size " << expected.size()
                                                   << ", actual size " << rec->msg_.size();
                    iter->next();
                    num++;
                }
            }
            LOG(INFO) << "End reader " << i << ", valid seek times " << validSeek;
        });
    }

    writer.join();
    for (auto& r : readers) {
        r.join();
    }
}


}  // namespace wal
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
