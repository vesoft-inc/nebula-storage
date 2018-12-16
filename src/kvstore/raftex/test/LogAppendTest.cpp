/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <folly/String.h>
#include "fs/TempDir.h"
#include "fs/FileUtils.h"
#include "thread/GenericThreadPool.h"
#include "network/NetworkUtils.h"
#include "raftex/BufferFlusher.h"
#include "raftex/RaftexService.h"
#include "raftex/test/RaftexTestBase.h"
#include "raftex/test/TestShard.h"

DECLARE_uint32(heartbeat_interval);


namespace nebula {
namespace raftex {

TEST(LogAppend, SimpleAppend) {
    fs::TempDir walRoot("/tmp/election_after_boot.XXXXXX");
    std::shared_ptr<thread::GenericThreadPool> workers;
    std::vector<std::string> wals;
    std::vector<HostAddr> allHosts;
    std::vector<std::shared_ptr<RaftexService>> services;
    std::vector<std::shared_ptr<test::TestShard>> copies;

    std::shared_ptr<test::TestShard> leader;
    setupRaft(walRoot, workers, wals, allHosts, services, copies, leader);

    // Check all hosts agree on the same leader
    checkLeadership(copies, leader);

    // Append 100 logs
    LOG(INFO) << "=====> Start appending logs";
    std::vector<std::string> msgs;
    for (int i = 1; i <= 100; ++i) {
        msgs.emplace_back(
            folly::stringPrintf("Test Log Message %03d", i));
        auto fut = leader->appendLogsAsync(0, {msgs.back()});
        ASSERT_EQ(RaftPart::AppendLogResult::SUCCEEDED,
                  std::move(fut).get());
    }
    LOG(INFO) << "<===== Finish appending logs";

    // Sleep a while to make sure the lat log has been committed on
    // followers
    sleep(FLAGS_heartbeat_interval);

    // Check every copy
    for (auto& c : copies) {
        ASSERT_EQ(100, c->getNumLogs());
    }

    LogID id = 1;
    for (int i = 0; i < 100; ++i, ++id) {
        for (auto& c : copies) {
            folly::StringPiece msg;
            ASSERT_TRUE(c->getLogMsg(id, msg));
            ASSERT_EQ(msgs[i], msg.toString());
        }
    }

    finishRaft(services, copies, workers, leader);
}


TEST(LogAppend, MultiThreadAppend) {
    fs::TempDir walRoot("/tmp/election_after_boot.XXXXXX");
    std::shared_ptr<thread::GenericThreadPool> workers;
    std::vector<std::string> wals;
    std::vector<HostAddr> allHosts;
    std::vector<std::shared_ptr<RaftexService>> services;
    std::vector<std::shared_ptr<test::TestShard>> copies;

    std::shared_ptr<test::TestShard> leader;
    setupRaft(walRoot, workers, wals, allHosts, services, copies, leader);

    // Check all hosts agree on the same leader
    checkLeadership(copies, leader);

    // Create 16 threads, each appends 100 logs
    LOG(INFO) << "=====> Start multi-thread appending logs";
    const int numThreads = 4;
    const int numLogs = 100;
    std::vector<std::thread> threads;
    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back(std::thread([i, numLogs, leader] {
            for (int j = 1; j <= numLogs; ++j) {
                do {
                    auto fut = leader->appendLogsAsync(
                        0, {folly::stringPrintf("Log %03d for t%d", j, i)});
                    if (fut.isReady() &&
                        fut.value() == RaftPart::AppendLogResult::E_BUFFER_OVERFLOW) {
                        // Buffer overflow, while a little
                        usleep(5000);
                        continue;
                    } else if (j == numLogs) {
                        // Only wait on the last log messaage
                        ASSERT_EQ(RaftPart::AppendLogResult::SUCCEEDED,
                                  std::move(fut).get());
                    }
                    break;
                } while (true);
            }
        }));
    }

    // Wait for all threads to finish
    for (auto& t : threads) {
        t.join();
    }

    LOG(INFO) << "<===== Finish multi-thread appending logs";

    // Sleep a while to make sure the lat log has been committed on
    // followers
    sleep(FLAGS_heartbeat_interval);

    // Check every copy
    for (auto& c : copies) {
        ASSERT_EQ(numThreads * numLogs, c->getNumLogs());
    }

    LogID id = 1;
    for (int i = 0; i < numThreads * numLogs; ++i, ++id) {
        folly::StringPiece msg;
        ASSERT_TRUE(leader->getLogMsg(id, msg));
        for (auto& c : copies) {
            if (c != leader) {
                folly::StringPiece log;
                ASSERT_TRUE(c->getLogMsg(id, log));
                ASSERT_EQ(msg, log);
            }
        }
    }

    finishRaft(services, copies, workers, leader);
}
}  // namespace raftex
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    using namespace nebula::raftex;
    flusher = std::make_unique<BufferFlusher>();

    return RUN_ALL_TESTS();
}


