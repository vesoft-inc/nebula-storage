/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <folly/Benchmark.h>
#include "kvstore/wal/AtomicLogBuffer.h"
#include "kvstore/wal/InMemoryLogBufferList.h"

DEFINE_bool(only_seek, false, "Only seek in read test");

#define TEST_WRTIE 1
#define TEST_READ  1

using nebula::wal::AtomicLogBuffer;
using nebula::wal::Record;
using nebula::wal::InMemoryBufferList;

void prepareData(std::shared_ptr<InMemoryBufferList> inMemoryLogBuffer,
                 int32_t len,
                 size_t total) {
    for (size_t i = 0; i < total; i++) {
        inMemoryLogBuffer->push(i, 0, 0, std::string(len, 'A'));
    }
}

void prepareData(std::shared_ptr<AtomicLogBuffer> logBuffer,
                 int32_t len,
                 size_t total) {
    for (size_t i = 0; i < total; i++) {
        logBuffer->push(i, Record(0, 0, std::string(len, 'A')));
    }
}

/*************************
 * Begining of benchmarks
 ************************/

void runInMemoryLogBufferWriteTest(size_t iters, int32_t len) {
    std::shared_ptr<InMemoryBufferList> inMemoryLogBuffer;
    std::vector<std::string> recs;
    BENCHMARK_SUSPEND {
        recs.reserve(iters);
        for (size_t i = 0; i < iters; i++) {
            recs.emplace_back(std::string(len, 'A'));
        }
        inMemoryLogBuffer = InMemoryBufferList::instance();
    }
    for (size_t i = 0; i < iters; i++) {
        inMemoryLogBuffer->push(i, 0, 0, std::move(recs[i]));
    }
    BENCHMARK_SUSPEND {
        recs.clear();
    }
}

void runAtomicLogBufferWriteTest(size_t iters, int32_t len) {
    std::shared_ptr<AtomicLogBuffer> logBuffer;
    std::vector<Record> recs;
    BENCHMARK_SUSPEND {
        recs.reserve(iters);
        for (size_t i = 0; i < iters; i++) {
            recs.emplace_back(0, 0, std::string(len, 'A'));
        }
        logBuffer = AtomicLogBuffer::instance();
    }
    for (size_t i = 0; i < iters; i++) {
        logBuffer->push(i, std::move(recs[i]));
    }
    BENCHMARK_SUSPEND {
        recs.clear();
    }
}

#if TEST_WRTIE

BENCHMARK(InMemoryLogBufferWriteShort, iters) {
    runInMemoryLogBufferWriteTest(iters, 16);
}

BENCHMARK_RELATIVE(AtomicLogBufferWriteShort, iters) {
    runAtomicLogBufferWriteTest(iters, 16);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(InMemoryLogBufferWriteMiddle, iters) {
    runInMemoryLogBufferWriteTest(iters, 128);
}

BENCHMARK_RELATIVE(AtomicLogBufferWriteMiddle, iters) {
    runAtomicLogBufferWriteTest(iters, 128);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(InMemoryLogBufferWriteLong, iters) {
    runInMemoryLogBufferWriteTest(iters, 1024);
}

BENCHMARK_RELATIVE(AtomicLogBufferWriteLong, iters) {
    runAtomicLogBufferWriteTest(iters, 1024);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(InMemoryLogBufferWriteVeryLong, iters) {
    runInMemoryLogBufferWriteTest(iters, 4096);
}

BENCHMARK_RELATIVE(AtomicLogBufferWriteVeryLong, iters) {
    runAtomicLogBufferWriteTest(iters, 4096);
}

BENCHMARK_DRAW_LINE();

#endif
/*============== Begin test for scan ===================== */
void runInMemoryLogBufferReadLatestN(int32_t total,
                                     int32_t N) {
    std::shared_ptr<InMemoryBufferList> inMemoryLogBuffer;
    BENCHMARK_SUSPEND {
        inMemoryLogBuffer = InMemoryBufferList::instance();
        prepareData(inMemoryLogBuffer, 1024, total);
    }
    auto start = total - N;
    auto end = total - 1;
    int32_t loopTimes = 1000000;
    while (loopTimes-- > 0) {
        auto iter = inMemoryLogBuffer->iterator(start, end);
        if (!FLAGS_only_seek) {
            for (;iter->valid(); ++(*iter)) {
                auto log = iter->logMsg();
                folly::doNotOptimizeAway(log);
            }
        }
    }
}

void runAtomicLogBufferReadLatestN(int32_t total,
                                   int32_t N) {
    std::shared_ptr<AtomicLogBuffer> logBuffer;
    BENCHMARK_SUSPEND {
        logBuffer = AtomicLogBuffer::instance();
        prepareData(logBuffer, 1024, total);
    }
    auto start = total - N;
    auto end = total - 1;
    int32_t loopTimes = 1000000;
    while (loopTimes-- > 0) {
        auto iter = logBuffer->iterator(start, end);
        if (!FLAGS_only_seek) {
            for (;iter->valid(); ++(*iter)) {
                auto log = iter->logMsg();
                folly::doNotOptimizeAway(log);
            }
        }
    }
}

#if TEST_READ
constexpr int32_t totalLogs = 20000;

BENCHMARK(InMemoryLogBufferReadLatest8) {
    runInMemoryLogBufferReadLatestN(totalLogs, 8);
}

BENCHMARK_RELATIVE(AtomicLogBufferReadLatest8) {
    runAtomicLogBufferReadLatestN(totalLogs, 8);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(InMemoryLogBufferReadLatest32) {
    runInMemoryLogBufferReadLatestN(totalLogs, 32);
}

BENCHMARK_RELATIVE(AtomicLogBufferReadLatest32) {
    runAtomicLogBufferReadLatestN(totalLogs, 32);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(InMemoryLogBufferReadLatest128) {
    runInMemoryLogBufferReadLatestN(totalLogs, 128);
}

BENCHMARK_RELATIVE(AtomicLogBufferReadLatest128) {
    runAtomicLogBufferReadLatestN(totalLogs, 128);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(InMemoryLogBufferReadLatest1024) {
    runInMemoryLogBufferReadLatestN(totalLogs, 1024);
}

BENCHMARK_RELATIVE(AtomicLogBufferReadLatest1024) {
    runAtomicLogBufferReadLatestN(totalLogs, 1024);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(InMemoryLogBufferReadLatest6000) {
    runInMemoryLogBufferReadLatestN(totalLogs, 6000);
}

BENCHMARK_RELATIVE(AtomicLogBufferReadLatest6000) {
    runAtomicLogBufferReadLatestN(totalLogs, 6000);
}

BENCHMARK_DRAW_LINE();

#endif


/*************************
 * End of benchmarks
 ************************/


int main(int argc, char** argv) {
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    folly::runBenchmarks();
    return 0;
}
/*
Intel(R) Xeon(R) CPU E5-2690 v2 @ 3.00GHz
-O2  kMaxLenght=64    write test
============================================================================
LogBufferBenchmark.cpprelative                            time/iter  iters/s
============================================================================
InMemoryLogBufferWriteShort                                 97.70ns   10.23M
AtomicLogBufferWriteShort                        246.98%    39.56ns   25.28M
----------------------------------------------------------------------------
InMemoryLogBufferWriteMiddle                                98.69ns   10.13M
AtomicLogBufferWriteMiddle                       213.39%    46.25ns   21.62M
----------------------------------------------------------------------------
InMemoryLogBufferWriteLong                                 106.45ns    9.39M
AtomicLogBufferWriteLong                         222.10%    47.93ns   20.86M
----------------------------------------------------------------------------
InMemoryLogBufferWriteVeryLong                             122.01ns    8.20M
AtomicLogBufferWriteVeryLong                     242.53%    50.31ns   19.88M
----------------------------------------------------------------------------
============================================================================

-O2 kMaxLenght=64 read test, repeat 'seek and scan' 1M times each iteration.
----------------------------------------------------------------------------
InMemoryLogBufferReadLatest8                               427.37ms     2.34
AtomicLogBufferReadLatest8                       362.10%   118.03ms     8.47
----------------------------------------------------------------------------
InMemoryLogBufferReadLatest32                                 1.11s  902.66m
AtomicLogBufferReadLatest32                      390.31%   283.84ms     3.52
----------------------------------------------------------------------------
InMemoryLogBufferReadLatest128                                3.79s  263.85m
AtomicLogBufferReadLatest128                     414.49%   914.40ms     1.09
----------------------------------------------------------------------------
InMemoryLogBufferReadLatest1024                              27.93s   35.80m
AtomicLogBufferReadLatest1024                    472.45%      5.91s  169.16m
----------------------------------------------------------------------------
InMemoryLogBufferReadLatest6000                             2.56min    6.52m
AtomicLogBufferReadLatest6000                    419.61%     36.57s   27.34m
----------------------------------------------------------------------------
============================================================================

*/
