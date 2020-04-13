/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <folly/Benchmark.h>
#include "kvstore/wal/AtomicLogBuffer.h"
#include "kvstore/wal/InMemoryLogBuffer.h"



namespace nebula {
namespace wal {
/**
 * Mock current buffer list used in WAL module.
 * */
struct InMemoryBufferList {
public:

    void push(LogID logId, TermID termId, ClusterID clusterId, std::string&& msg) {
        auto buffer = getLastBuffer(logId, msg.size());
        buffer->push(termId, clusterId, std::move(msg));
    }

    BufferPtr getLastBuffer(LogID id, size_t expectedToWrite) {
        std::unique_lock<std::mutex> g(buffersMutex_);
        if (!buffers_.empty()) {
            if (buffers_.back()->size() + expectedToWrite <= maxBufferSize_) {
                return buffers_.back();
            }
            // Need to rollover to a new buffer
            if (buffers_.size() == numBuffers_) {
                // Need to pop the first one
                buffers_.pop_front();
            }
            CHECK_LT(buffers_.size(), numBuffers_);
        }
        buffers_.emplace_back(std::make_shared<InMemoryLogBuffer>(id, ""));
        return buffers_.back();
    }


    BufferList  buffers_;
    std::mutex  buffersMutex_;
    size_t     maxBufferSize_{8 * 1024 * 1024};
    size_t     numBuffers_{2};
};

}   // namespace wal
}   // namespace nebula

using nebula::wal::AtomicLogBuffer;
using nebula::wal::Record;
using nebula::wal::InMemoryBufferList;


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
        inMemoryLogBuffer = std::make_shared<nebula::wal::InMemoryBufferList>();
    }
    for (size_t i = 0; i < iters; i++) {
        inMemoryLogBuffer->push(i, 0, 0, std::move(recs[i]));
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
}

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
-O2  kMaxLenght=64
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


-O2 kMaxLenght=512
============================================================================
LogBufferBenchmark.cpprelative                            time/iter  iters/s
============================================================================
InMemoryLogBufferWriteShort                                107.50ns    9.30M
AtomicLogBufferWriteShort                        254.48%    42.24ns   23.67M
----------------------------------------------------------------------------
InMemoryLogBufferWriteMiddle                               110.07ns    9.08M
AtomicLogBufferWriteMiddle                       218.95%    50.27ns   19.89M
----------------------------------------------------------------------------
InMemoryLogBufferWriteLong                                 116.82ns    8.56M
AtomicLogBufferWriteLong                         228.28%    51.18ns   19.54M
----------------------------------------------------------------------------
InMemoryLogBufferWriteVeryLong                             134.19ns    7.45M
AtomicLogBufferWriteVeryLong                     258.99%    51.81ns   19.30M
----------------------------------------------------------------------------
============================================================================

*/
