/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef WAL_ATOMICLOGBUFFER_H_
#define WAL_ATOMICLOGBUFFER_H_

#include "thrift/ThriftTypes.h"
#include <gtest/gtest_prod.h>

namespace nebula {
namespace wal {

constexpr int32_t kMaxLength = 64;

struct Record {
    Record() = default;
    Record(const Record&) = default;
    Record(Record&& record) noexcept = default;
    Record& operator=(Record&& record) noexcept = default;

    Record(ClusterID clusterId, TermID termId, std::string msg)
        : clusterId_(clusterId)
        , termId_(termId)
        , msg_(std::move(msg)) {}

    int32_t size() const {
        return sizeof(ClusterID) + sizeof(TermID) + msg_.size();
    }

    ClusterID       clusterId_;
    TermID          termId_;
    std::string     msg_;
};

struct Node {
    Node() = default;

    bool isFull() {
        return pos_.load(std::memory_order_relaxed) == kMaxLength;
    }

    bool push_back(Record&& rec) {
        if (isFull()) {
            return false;
        }
        size_ += rec.size();
        auto pos = pos_.load(std::memory_order_acquire);
        records_[pos] = std::move(rec);
        pos_.fetch_add(1, std::memory_order_release);
        return true;
    }

    Record* rec(int32_t index) {
        CHECK_GE(index, 0);
        auto pos = pos_.load(std::memory_order_acquire);
        CHECK_LE(index, pos);
        CHECK(index != kMaxLength);
        return &records_[index];
    }

    LogID lastLogId() const {
        return firstLogId_ + pos_.load(std::memory_order_relaxed);
    }

    LogID                             firstLogId_{0};
    // total size for current Node.
    int32_t                           size_{0};
    Node*                             next_{nullptr};

    /******* readers maybe access the fields below ******************/

    // We should ensure the records appended happens-before pos_ increment.
    std::array<Record, kMaxLength>    records_;
    // current valid position for the next record.
    std::atomic<int32_t>              pos_{0};
    // The filed only be accessed when the refs count donw to zero
    std::atomic<bool>                 markDeleted_{false};
    // We should ensure the records appened happens-before the prev inited.
    std::atomic<Node*>                prev_{nullptr};
};

/**
 * Wait-free log buffer for single writer, multi readers
 * When deleting the extra node, to avoid read the dangling one,
 * we just mark it to be deleted, and delete it when no readers using it.
 *
 * For write, most of time, it is o(1)
 * For seek, it is o(n), n is the number of nodes inside current list, but in most
 * cases, the seeking log is in the head Node, so it equals O(1)
 * */
class AtomicLogBuffer : public std::enable_shared_from_this<AtomicLogBuffer> {
public:
    /**
     * The iterator once created, it could just see the snapshot of current list.
     * In other words, the new records inserted during scanning are invisible.
     * */
    class Iterator {
        friend class AtomicLogBuffer;
        FRIEND_TEST(AtomicLogBufferTest, SingleWriterMultiReadersTest);
    public:
        ~Iterator() {
            logBuffer_->releaseRef();
        }

        bool valid() {
            return valid_;
        }

        void next() {
            currIndex_++;
            currLogId_++;
            if (currLogId_ > end_) {
                valid_ = false;
                currRec_ = nullptr;
                return;
            }
            // Operations after load SHOULD NOT reorder before it.
            auto pos = currNode_->pos_.load(std::memory_order_acquire);
            VLOG(3) << "currNode firstLogId = " << currNode_->firstLogId_
                    << ", currIndex = " << currIndex_
                    << ", currNode pos " << pos;
            if (currIndex_ >= pos) {
                currNode_ = currNode_->prev_.load(std::memory_order_relaxed);
                if (currNode_ == nullptr) {
                    valid_ = false;
                    currRec_ = nullptr;
                    return;
                } else {
                    currIndex_ = 0;
                }
            }
            DCHECK_NOTNULL(currNode_);
            DCHECK_LT(currIndex_, kMaxLength);
            currRec_ = currNode_->rec(currIndex_);
        }

        const Record* record() const {
            if (!valid_) {
                return nullptr;
            }
            DCHECK_NOTNULL(currRec_);
            return currRec_;
        }

        LogID logId() const {
            DCHECK(valid_);
            return currLogId_;
        }

    private:
        // Iterator could only be acquired by AtomicLogBuffer::iterator interface.
        Iterator(std::shared_ptr<AtomicLogBuffer> logBuffer, LogID start, LogID end)
            : logBuffer_(logBuffer)
            , currLogId_(start) {
            logBuffer_->addRef();
            end_ = std::min(end, logBuffer->lastLogId());
            seek(currLogId_);
        }

        void seek(LogID logId) {
            currNode_ = logBuffer_->seek(logId);
            if (currNode_ != nullptr) {
                currIndex_ = logId - currNode_->firstLogId_;
                currRec_ = currNode_->rec(currIndex_);
                valid_ = true;
            } else {
                valid_ = false;
            }
        }

        Node* currNode() const {
            return currNode_;
        }

        int32_t currIndex() const {
            return currIndex_;
        }

    private:
        std::shared_ptr<AtomicLogBuffer> logBuffer_;
        LogID                            currLogId_{0};
        LogID                            end_{0};
        Node*                            currNode_{nullptr};
        int32_t                          currIndex_{0};
        bool                             valid_{true};
        Record*                          currRec_{nullptr};
    };

    static std::shared_ptr<AtomicLogBuffer> instance(int32_t capacity = 8 * 1024 * 1024) {
        return std::shared_ptr<AtomicLogBuffer>(new AtomicLogBuffer(capacity));
    }

    ~AtomicLogBuffer() {
        auto refs = refs_.load(std::memory_order_acquire);
        CHECK(refs == 0);
        auto* curr = head_.load(std::memory_order_relaxed);
        auto* prev = curr;
        while (curr != nullptr) {
            curr = curr->next_;
            delete prev;
            prev = curr;
        }
        if (prev != nullptr) {
            delete prev;
        }
    }

    void push(LogID logId, Record&& record) {
        auto* head = head_.load(std::memory_order_relaxed);
        auto recSize = record.size();
        if (head == nullptr
                || head->isFull()
                || head->markDeleted_.load(std::memory_order_relaxed)) {
            auto* newNode = new Node();
            newNode->firstLogId_ = logId;
            newNode->next_ = head;
            size_ += recSize;
            newNode->push_back(std::move(record));
            if (head != nullptr) {
                head->prev_.store(newNode, std::memory_order_release);
            } else {
                // It is the first log.
                firstLogId_.store(logId, std::memory_order_relaxed);
            }
            head_.store(newNode, std::memory_order_relaxed);
            return;
        }
        if (size_ + recSize > capacity_ && head != nullptr) {
            auto* p = head->next_;
            int32_t sumSize = head->size_;
            int32_t firstLogIdInPrevNode = head->firstLogId_;
            while (p != nullptr) {
                if (sumSize + p->size_ > capacity_) {
                    bool expected = false;
                    if (p->markDeleted_.compare_exchange_strong(expected, true)) {
                        VLOG(3) << "Mark node " << p->firstLogId_ << " to be deleted!";
                        size_ -= p->size_;
                        firstLogId_.store(firstLogIdInPrevNode, std::memory_order_relaxed);
                    } else {
                        // The rest nodes has been mark deleted.
                        break;
                    }
                }
                sumSize += p->size_;
                firstLogIdInPrevNode = p->firstLogId_;
                p = p->next_;
            }
        }
        size_ += recSize;
        head->push_back(std::move(record));
    }

    LogID firstLogId() const {
        return firstLogId_.load(std::memory_order_relaxed);
    }

    LogID lastLogId() const {
        auto* p = head_.load(std::memory_order_relaxed);
        CHECK_NOTNULL(p);
        return p->lastLogId();
    }

    /**
     * For reset operation, users should keep it thread-safe with push operation.
     * Just mark all nodes to be deleted.
     * */
    void reset() {
        auto* p = head_.load(std::memory_order_relaxed);
        while (p != nullptr) {
            bool expected = false;
            if (!p->markDeleted_.compare_exchange_strong(expected, true)) {
                // The rest nodes has been mark deleted.
                break;
            }
            p = p->next_;
        }
        size_ = 0;
        firstLogId_  = 0;
    }

    std::unique_ptr<Iterator> iterator(LogID start, LogID end) {
        std::unique_ptr<Iterator> iter(new Iterator(shared_from_this(), start, end));
        return iter;
    }

private:
    AtomicLogBuffer(int32_t capacity)
        : capacity_(capacity) {}

    /*
     * Find the non-deleted node contains the logId.
     * */
    Node* seek(LogID logId) {
        auto* head = head_.load(std::memory_order_relaxed);
        if (head != nullptr && logId > head->lastLogId()) {
            VLOG(3) << "Bad seek, the seeking logId " << logId
                    << " is greater than the latest logId in buffer "
                    << head->lastLogId();
            return nullptr;
        }
        auto* p = head;
        if (p == nullptr) {
            return nullptr;
        }
        while (p != nullptr && !p->markDeleted_) {
            VLOG(3) << "current node firstLogId = " << p->firstLogId_
                    << ", the seeking logId = " << logId;
            if (logId >= p->firstLogId_) {
                break;
            }
            p = p->next_;
        }
        return p->markDeleted_ ? nullptr : p;
    }

    int32_t addRef() {
        return refs_.fetch_add(1, std::memory_order_consume);
    }

    void releaseRef() {
        auto readers = refs_.fetch_add(-1, std::memory_order_consume);
        VLOG(3) << "Release ref, readers = " << readers;
        if (readers == 1) {
            // It means no readers on the deleted nodes.
            auto* prev = head_.load(std::memory_order_relaxed);
            auto* curr = prev;
            while (curr != nullptr) {
                if (curr->markDeleted_.load(std::memory_order_relaxed)) {
                    VLOG(3) << "Delete node " << curr->firstLogId_;
                    auto* del = curr;
                    prev->next_ = curr->next_;
                    curr = curr->next_;
                    if (curr != nullptr) {
                        curr->prev_ = prev;
                    }
                    delete del;
                } else {
                    prev = curr;
                    curr = curr->next_;
                }
            }
        }
    }


private:
    std::atomic<Node*>           head_{nullptr};
    std::atomic_int              refs_{0};
    // current size for the buffer.
    std::atomic_int              size_{0};
    std::atomic<LogID>           firstLogId_{0};
    // The max size limit.
    int32_t                      capacity_{8 * 1024 * 1024};
};

}  // namespace wal
}  // namespace nebula

#endif  // WAL_ATOMICLOGBUFFER_H_
