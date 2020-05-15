/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "thread/GenericWorker.h"
#include "time/Duration.h"
#include "clients/storage/GraphStorageClient.h"

DEFINE_int32(threads, 2, "Total threads for perf");
DEFINE_int32(qps, 1000, "Total qps for the perf tool");
DEFINE_int32(totalReqs, 10000, "Total requests during this perf test");
DEFINE_int32(io_threads, 10, "Client io threads");
DEFINE_string(method, "getNeighbors", "method type being tested,"
                                      "such as getNeighbors, addVertices, addEdges, getVertices");
DEFINE_string(meta_server_addrs, "", "meta server address");
DEFINE_int32(min_vertex_id, 1, "The smallest vertex Id");
DEFINE_int32(max_vertex_id, 10000, "The biggest vertex Id");
DEFINE_int32(size, 1000, "The data's size per request");
DEFINE_string(space_name, "test", "Specify the space name");
DEFINE_string(tag_name, "test_tag", "Specify the tag name");
DEFINE_string(edge_name, "test_edge", "Specify the edge name");
DEFINE_bool(random_message, false, "Whether to write random message to storage service");

namespace nebula {
namespace storage {

thread_local uint32_t position = 1;

class Perf {
public:
    int run() {
        uint32_t qpsPerThread = FLAGS_qps / FLAGS_threads;
        int32_t radix = qpsPerThread / 1000;
        int32_t slotSize = sizeof(slots_) / sizeof(int32_t);
        std::fill(slots_, slots_ + slotSize, radix);

        int32_t remained = qpsPerThread % 1000 / 100;
        if (remained != 0) {
            int32_t step = slotSize / remained - 1;
            if (step == 0) {
                step = 1;
            }
            for (int32_t i = 0; i < remained; i++) {
                int32_t p = (i + i * step) % slotSize;
                if (slots_[p] == radix) {
                    slots_[p] += 1;
                } else {
                    while (slots_[++p] == (radix + 1)) {}
                    slots_[p] += 1;
                }
            }
        }

        std::stringstream stream;
        for (int32_t slot : slots_) {
            stream << slot << " ";
        }
        LOG(INFO) << "Total threads " << FLAGS_threads
                  << ", qpsPerThread " << qpsPerThread
                  << ", slots " << stream.str();
        auto metaAddrsRet = nebula::network::NetworkUtils::toHosts(FLAGS_meta_server_addrs);
        if (!metaAddrsRet.ok() || metaAddrsRet.value().empty()) {
            LOG(ERROR) << "Can't get metaServer address, status:" << metaAddrsRet.status()
                       << ", FLAGS_meta_server_addrs:" << FLAGS_meta_server_addrs;
            return EXIT_FAILURE;
        }

        std::vector<std::unique_ptr<thread::GenericWorker>> threads;
        for (int32_t i = 0; i < FLAGS_threads; i++) {
            auto t = std::make_unique<thread::GenericWorker>();
            threads.emplace_back(std::move(t));
        }
        threadPool_ = std::make_shared<folly::IOThreadPoolExecutor>(FLAGS_io_threads);

        meta::MetaClientOptions options;
        options.skipConfig_ = true;
        mClient_ = std::make_unique<meta::MetaClient>(threadPool_, metaAddrsRet.value(), options);
        CHECK(mClient_->waitForMetadReady());

        auto spaceResult = mClient_->getSpaceIdByNameFromCache(FLAGS_space_name);
        if (!spaceResult.ok()) {
            LOG(ERROR) << "Get SpaceID Failed: " << spaceResult.status();
            return EXIT_FAILURE;
        }
        spaceId_ = spaceResult.value();

        auto tagResult = mClient_->getTagIDByNameFromCache(spaceId_, FLAGS_tag_name);
        if (!tagResult.ok()) {
            LOG(ERROR) << "TagID not exist: " << tagResult.status();
            return EXIT_FAILURE;
        }
        tagId_ = tagResult.value();

        auto tagSchemaRes = mClient_->getTagSchemaFromCache(spaceId_, tagId_);
        if (!tagSchemaRes.ok()) {
            LOG(ERROR) << "TagID not exist: " << tagSchemaRes.status();
            return EXIT_FAILURE;
        }
        auto tagSchema = tagSchemaRes.value();
        for (size_t i = 0; i < tagSchema->getNumFields(); i++) {
            tagProps_[tagId_].emplace_back(tagSchema->getFieldName(i));
        }

        auto edgeResult = mClient_->getEdgeTypeByNameFromCache(spaceId_, FLAGS_edge_name);
        if (!edgeResult.ok()) {
            LOG(ERROR) << "EdgeType not exist: " << edgeResult.status();
            return EXIT_FAILURE;
        }
        edgeType_ = edgeResult.value();
        auto edgeSchemaRes = mClient_->getEdgeSchemaFromCache(spaceId_, std::abs(edgeType_));
        if (!edgeSchemaRes.ok()) {
            LOG(ERROR) << "Edge not exist: " << edgeSchemaRes.status();
            return EXIT_FAILURE;
        }
        auto edgeSchema = edgeSchemaRes.value();
        for (size_t i = 0; i < edgeSchema->getNumFields(); i++) {
            edgeProps_.emplace_back(edgeSchema->getFieldName(i));
        }

        graphStorageClient_ = std::make_unique<GraphStorageClient>(threadPool_, mClient_.get());
        time::Duration duration;
        static uint32_t interval = 1;
        for (auto& t : threads) {
            CHECK(t->start("TaskThread"));
            if (FLAGS_method == "getNeighbors") {
                t->addRepeatTask(interval, &Perf::getNeighborsTask, this);
            } else if (FLAGS_method == "addVertices") {
                t->addRepeatTask(interval, &Perf::addVerticesTask, this);
            } else if (FLAGS_method == "addEdges") {
                t->addRepeatTask(interval, &Perf::addEdgesTask, this);
            } else {
                t->addRepeatTask(interval, &Perf::getVerticesTask, this);
            }
        }

        while (finishedRequests_ < FLAGS_totalReqs) {
            if (finishedRequests_ % 1000 == 0) {
                LOG(INFO) << "Total " << FLAGS_totalReqs << ", finished " << finishedRequests_;
            }
            usleep(1000 * 30);
        }
        for (auto& t : threads) {
            t->stop();
        }
        for (auto& t : threads) {
            t->wait();
        }
        LOG(INFO) << "Total time cost " << duration.elapsedInMSec() << "ms, "
                  << "total requests " << finishedRequests_;
        return 0;
    }

private:
    std::vector<VertexID> randomVertices() {
        return {std::to_string(FLAGS_min_vertex_id
                              + folly::Random::rand32(FLAGS_max_vertex_id - FLAGS_min_vertex_id))};
    }

    std::vector<std::string> vertexProps() {
        std::vector<std::string> props;
        props.emplace_back(folly::stringPrintf("tag_%d_col_1", tagId_));
        return props;
    }

    std::vector<std::string> edgeProps() {
        std::vector<std::string> props;
        props.emplace_back("col_1");
        return props;
    }

    std::vector<Value> genData(int32_t size) {
        std::vector<Value> values;
        if (FLAGS_random_message) {
            for (int32_t index = 0; index < size;) {
                Value val(folly::to<std::string>(folly::Random::rand32(128)));
                values.emplace_back(val);
                index++;
            }
        } else {
            for (int32_t index = 0; index < size;) {
                Value val("");
                values.emplace_back(val);
                index++;
            }
        }
        return values;
    }

    std::vector<storage::cpp2::NewVertex> genVertices() {
        std::vector<storage::cpp2::NewVertex> newVertices;
        static int32_t size = sizeof(slots_) / sizeof(int32_t);
        static int vintId = FLAGS_min_vertex_id;
        position = (position + 1) % size;

        for (int32_t i = 0; i < slots_[position]; i++) {
            storage::cpp2::NewVertex v;
            v.set_id(std::to_string(vintId));
            vintId++;
            decltype(v.tags) newTags;
            storage::cpp2::NewTag newTag;
            newTag.set_tag_id(tagId_);
            auto props = genData(FLAGS_size);
            newTag.set_props(std::move(props));
            newTags.emplace_back(std::move(newTag));
            v.set_tags(std::move(newTags));
            newVertices.emplace_back(std::move(v));
        }
        return newVertices;
    }

    std::vector<storage::cpp2::NewEdge> genEdges() {
        static int32_t size = sizeof(slots_) / sizeof(int32_t);
        std::vector<storage::cpp2::NewEdge> edges;
        static int vintId = FLAGS_min_vertex_id;
        position = (position + 1) % size;
        for (int32_t i = 0; i< slots_[position]; i++) {
            storage::cpp2::NewEdge edge;
            storage::cpp2::EdgeKey eKey;
            eKey.set_src(std::to_string(vintId));
            eKey.set_edge_type(edgeType_);
            eKey.set_dst(std::to_string(vintId + 1));
            eKey.set_ranking(0);
            edge.set_key(std::move(eKey));
            auto props = genData(FLAGS_size);
            edge.set_props(std::move(props));
            edges.emplace_back(std::move(edge));
        }
        return edges;
    }

    void getNeighborsTask() {
        auto* evb = threadPool_->getEventBase();
        std::vector<std::string> colNames;
        colNames.emplace_back("_vid");
        std::vector<Row> vertices;
        for (auto& vertex : randomVertices()) {
            nebula::Row  row;
            row.columns.emplace_back(vertex);
            vertices.emplace_back(row);
        }

        std::vector<EdgeType> e(edgeType_);

        cpp2::EdgeDirection edgeDire = cpp2::EdgeDirection::BOTH;
        std::vector<cpp2::StatProp> statProps;
        std::vector<std::string> vProps = std::move(vertexProps());
        std::vector<std::string> eProps = std::move(edgeProps());
        auto f =
          graphStorageClient_->getNeighbors(spaceId_, colNames, vertices,
                                            std::move(e), edgeDire,  &statProps,
                                            &vProps, &eProps)
                .via(evb)
                .thenValue([this](auto&& resps) {
                    if (!resps.succeeded()) {
                        LOG(ERROR) << "Request failed!";
                    } else {
                        VLOG(3) << "request successed!";
                    }
                    this->finishedRequests_++;
                    VLOG(3) << "request successed!";
                })
                .thenError([](auto&&) { LOG(ERROR) << "request failed!"; });
    }

    void addVerticesTask() {
        auto* evb = threadPool_->getEventBase();
        auto f = graphStorageClient_->addVertices(spaceId_, genVertices(), tagProps_, true)
                    .via(evb).thenValue([this](auto&& resps) {
                        if (!resps.succeeded()) {
                            for (auto& entry : resps.failedParts()) {
                                LOG(ERROR) << "Request failed, part " << entry.first
                                           << ", error " << static_cast<int32_t>(entry.second);
                            }
                        } else {
                            VLOG(3) << "request successed!";
                        }
                        this->finishedRequests_++;
                     })
                     .thenError([](auto&&) {LOG(ERROR) << "Request failed!"; });
    }

    void addEdgesTask() {
        auto* evb = threadPool_->getEventBase();
        auto f = graphStorageClient_->addEdges(spaceId_, genEdges(), edgeProps_, true)
                    .via(evb).thenValue([this](auto&& resps) {
                        if (!resps.succeeded()) {
                            LOG(ERROR) << "Request failed!";
                        } else {
                            VLOG(3) << "request successed!";
                        }
                        this->finishedRequests_++;
                        VLOG(3) << "request successed!";
                     }).thenError([](auto&&) {
                        LOG(ERROR) << "Request failed!";
                     });
    }

    void getVerticesTask() {
        return;
        /*
        auto* evb = threadPool_->getEventBase();
        auto f = graphStorageClient_->getVertexProps(spaceId_, randomVertices(), randomCols())
                    .via(evb).thenValue([this](auto&& resps) {
                        if (!resps.succeeded()) {
                            LOG(ERROR) << "Request failed!";
                        } else {
                            VLOG(3) << "request successed!";
                        }
                        this->finishedRequests_++;
                        VLOG(3) << "request successed!";
                     }).thenError([](auto&&) {
                        LOG(ERROR) << "Request failed!";
                     });
        */
    }

private:
    std::atomic_long                                    finishedRequests_{0};
    std::unique_ptr<GraphStorageClient>                 graphStorageClient_;
    std::unique_ptr<meta::MetaClient>                   mClient_;
    std::shared_ptr<folly::IOThreadPoolExecutor>        threadPool_;
    GraphSpaceID                                        spaceId_;
    TagID                                               tagId_;
    EdgeType                                            edgeType_;
    std::unordered_map<TagID, std::vector<std::string>> tagProps_;
    std::vector<std::string>                            edgeProps_;
    int32_t                                             slots_[10];
};

}  // namespace storage
}  // namespace nebula

int main(int argc, char *argv[]) {
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    nebula::storage::Perf perf;
    return perf.run();
}
