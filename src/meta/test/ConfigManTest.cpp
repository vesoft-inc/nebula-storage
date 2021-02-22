/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/conf/Configuration.h"
#include "common/meta/GflagsManager.h"
#include "common/clients/meta/MetaClient.h"

#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include <rocksdb/utilities/options_util.h>
#include "meta/test/TestUtils.h"
#include "meta/processors/configMan/GetConfigProcessor.h"
#include "meta/processors/configMan/SetConfigProcessor.h"
#include "meta/processors/configMan/ListConfigsProcessor.h"
#include "meta/processors/configMan/RegConfigProcessor.h"
#include "storage/test/TestUtils.h"

DECLARE_int32(heartbeat_interval_secs);
DECLARE_string(rocksdb_db_options);
DECLARE_string(rocksdb_column_family_options);

// some gflags to register
DEFINE_int64(int64_key_immutable, 100, "test");
DEFINE_int64(int64_key, 101, "test");
DEFINE_bool(bool_key, false, "test");
DEFINE_double(double_key, 1.23, "test");
DEFINE_string(string_key, "something", "test");
DEFINE_string(nested_key, R"({"max_background_jobs":"4"})", "test");
DEFINE_string(test0, "v0", "test");
DEFINE_string(test1, "v1", "test");
DEFINE_string(test2, "v2", "test");
DEFINE_string(test3, "v3", "test");
DEFINE_string(test4, "v4", "test");

namespace nebula {
namespace meta {

cpp2::ConfigItem assignConfigItem(const cpp2::ConfigModule& module,
                                  const std::string& name,
                                  const cpp2::ConfigMode& mode,
                                  const nebula::Value& value) {
    cpp2::ConfigItem item;
    item.set_module(module);
    item.set_name(name);
    item.set_mode(mode);
    item.set_value(value);
    return item;
}

TEST(ConfigManTest, ConfigProcessorTest) {
    fs::TempDir rootPath("/tmp/ConfigProcessorTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));

    auto item1 = assignConfigItem(cpp2::ConfigModule::STORAGE, "k1",
                                  cpp2::ConfigMode::MUTABLE, "v1");

    auto item2 = assignConfigItem(cpp2::ConfigModule::STORAGE, "k2",
                                  cpp2::ConfigMode::MUTABLE, "v2");

    // set and get without register
    {
        cpp2::SetConfigReq req;
        req.set_item(item1);

        auto* processor = SetConfigProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::ConfigItem item;
        item.set_module(cpp2::ConfigModule::STORAGE);
        item.set_name("k1");
        cpp2::GetConfigReq req;
        req.set_item(std::move(item));

        auto* processor = GetConfigProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // register config item1 and item2
    {
        std::vector<cpp2::ConfigItem> items;
        items.emplace_back(item1);
        items.emplace_back(item2);
        cpp2::RegConfigReq req;
        req.set_items(items);

        auto* processor = RegConfigProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // set and get string config item1
    {
        cpp2::SetConfigReq req;
        req.set_item(item1);

        auto* processor = SetConfigProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::ConfigItem item;
        item.set_module(cpp2::ConfigModule::STORAGE);
        item.set_name("k1");
        cpp2::GetConfigReq req;
        req.set_item(item);

        auto* processor = GetConfigProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(item1, resp.get_items().front());
    }
    // get config not existed
    {
        cpp2::ConfigItem item;
        item.set_module(cpp2::ConfigModule::STORAGE);
        item.set_name("not_existed");
        cpp2::GetConfigReq req;
        req.set_item(item);

        auto* processor = GetConfigProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(0, resp.get_items().size());
    }
    // list all configs in a module
    {
        cpp2::ListConfigsReq req;
        req.set_module(cpp2::ConfigModule::STORAGE);

        auto* processor = ListConfigsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();

        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(2, resp.get_items().size());
        auto ret1 = resp.get_items().front();
        auto ret2 = resp.get_items().back();
        if (ret1.get_name() == "k1") {
            ASSERT_EQ(ret1, item1);
            ASSERT_EQ(ret2, item2);
        } else {
            ASSERT_EQ(ret1, item2);
            ASSERT_EQ(ret2, item1);
        }
    }

    // register a nested config in other module
    cpp2::ConfigItem item3;
    item3.set_module(cpp2::ConfigModule::META);
    item3.set_name("nested");
    item3.set_mode(cpp2::ConfigMode::MUTABLE);
    item3.set_value(R"({"max_background_jobs":8})");

    {
        std::vector<cpp2::ConfigItem> items;
        items.emplace_back(item3);
        cpp2::RegConfigReq req;
        req.set_items(items);

        auto* processor = RegConfigProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // update the nested config
    {
        cpp2::ConfigItem updated;
        updated.set_module(cpp2::ConfigModule::META);
        updated.set_name("nested");
        updated.set_mode(cpp2::ConfigMode::MUTABLE);
        // update from consle as format of update list
        updated.set_value(R"({"max_background_jobs":8, "level0_file_num_compaction_trigger":10})");


        cpp2::SetConfigReq req;
        req.set_item(updated);

        auto* processor = SetConfigProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // get the nested config after updated
    {
        cpp2::ConfigItem item;
        item.set_module(cpp2::ConfigModule::META);
        item.set_name("nested");
        cpp2::GetConfigReq req;
        req.set_item(item);

        auto* processor = GetConfigProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());

        // verify the updated nested config
        conf::Configuration conf;
        auto confRet = conf.parseFromString(resp.get_items().front().get_value().getStr());
        ASSERT_TRUE(confRet.ok());

        int64_t val;
        auto status = conf.fetchAsInt("max_background_jobs", val);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(val, 8);
        status = conf.fetchAsInt("level0_file_num_compaction_trigger", val);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(val, 10);
    }
    // list all configs in all module
    {
        cpp2::ListConfigsReq req;
        req.set_module(cpp2::ConfigModule::ALL);

        auto* processor = ListConfigsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();

        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(3, resp.get_items().size());
    }
}

TEST(ConfigManTest, MetaConfigTest) {
    FLAGS_heartbeat_interval_secs = 1;
    fs::TempDir rootPath("/tmp/MetaConfigTest.XXXXXX");

    auto module = cpp2::ConfigModule::STORAGE;
    auto mode = cpp2::ConfigMode::MUTABLE;
    mock::MockCluster cluster;
    cluster.startMeta(rootPath.path());
    cluster.initMetaClient();
    auto* client = cluster.metaClient_.get();
    // client->setGflagsModule(module);

    // mock some test gflags to meta
    {
        std::vector<cpp2::ConfigItem> configItems;
        auto item = assignConfigItem(module, "int64_key_immutable",
                                     cpp2::ConfigMode::IMMUTABLE, 100L);
        configItems.emplace_back(std::move(item));

        item = assignConfigItem(module, "int64_key", mode, 101L);
        configItems.emplace_back(std::move(item));

        item = assignConfigItem(module, "bool_key", mode, false);
        configItems.emplace_back(std::move(item));

        item = assignConfigItem(module, "double_key", mode, 1.23);
        configItems.emplace_back(std::move(item));

        item = assignConfigItem(module, "string_key", mode, "something");
        configItems.emplace_back(std::move(item));

        item = assignConfigItem(module, "nested_key", mode, FLAGS_nested_key);
        configItems.emplace_back(std::move(item));
        client->regConfig(configItems);
    }

    // try to set/get config not registered
    {
        std::string name = "not_existed";
        sleep(FLAGS_heartbeat_interval_secs + 1);
        // get/set without register
        auto setRet = client->setConfig(module, name, 101l).get();
        ASSERT_FALSE(setRet.ok());
        auto getRet = client->getConfig(module, name).get();
        ASSERT_FALSE(getRet.ok());
    }
    // immutable configs
    {
        std::string name = "int64_key_immutable";

        // register config as immutable and try to update
        auto setRet = client->setConfig(module, name, 101l).get();
        ASSERT_FALSE(setRet.ok());

        // get immutable config
        auto getRet = client->getConfig(module, name).get();
        ASSERT_TRUE(getRet.ok());
        auto item = getRet.value().front();
        ASSERT_EQ(item.get_value().getInt(), 100);

        sleep(FLAGS_heartbeat_interval_secs + 1);
        ASSERT_EQ(FLAGS_int64_key_immutable, 100);
    }
    // mutable config
    {
        std::string name = "int64_key";
        ASSERT_EQ(FLAGS_int64_key, 101);

        // update config
        auto setRet = client->setConfig(module, name, 102l).get();
        ASSERT_TRUE(setRet.ok());

        // get from meta server
        auto getRet = client->getConfig(module, name).get();
        ASSERT_TRUE(getRet.ok());
        auto item = getRet.value().front();
        ASSERT_EQ(item.get_value().getInt(), 102);

        // get from cache
        sleep(FLAGS_heartbeat_interval_secs + 1);
        ASSERT_EQ(FLAGS_int64_key, 102);
    }
    {
        std::string name = "bool_key";
        ASSERT_EQ(FLAGS_bool_key, false);

        // update config
        auto setRet = client->setConfig(module, name, true).get();
        ASSERT_TRUE(setRet.ok());

        // get from meta server
        auto getRet = client->getConfig(module, name).get();
        ASSERT_TRUE(getRet.ok());
        auto item = getRet.value().front();
        ASSERT_TRUE(item.get_value().getBool());

        // get from cache
        sleep(FLAGS_heartbeat_interval_secs + 1);
        ASSERT_TRUE(FLAGS_bool_key);
    }
    {
        std::string name = "double_key";
        ASSERT_EQ(FLAGS_double_key, 1.23);

        // update config
        auto setRet = client->setConfig(module, name, 3.14).get();
        ASSERT_TRUE(setRet.ok());

        // get from meta server
        auto getRet = client->getConfig(module, name).get();
        ASSERT_TRUE(getRet.ok());
        auto item = getRet.value().front();
        ASSERT_EQ(item.get_value().getFloat(), 3.14);

        // get from cache
        sleep(FLAGS_heartbeat_interval_secs + 1);
        ASSERT_EQ(FLAGS_double_key, 3.14);
    }
    {
        std::string name = "string_key";
        ASSERT_EQ(FLAGS_string_key, "something");

        // update config
        std::string newValue = "abc";
        auto setRet = client->setConfig(module, name, newValue).get();
        ASSERT_TRUE(setRet.ok());

        // get from meta server
        auto getRet = client->getConfig(module, name).get();
        ASSERT_TRUE(getRet.ok());
        auto item = getRet.value().front();
        ASSERT_EQ(item.get_value().getStr(), "abc");

        // get from cache
        sleep(FLAGS_heartbeat_interval_secs + 1);
        ASSERT_EQ(FLAGS_string_key, "abc");
    }
    {
        std::string name = "nested_key";
        ASSERT_EQ(FLAGS_nested_key, R"({"max_background_jobs":"4"})");

        // update config
        std::string newValue = R"({"max_background_jobs":"8"})";
        auto setRet = client->setConfig(module, name, newValue).get();
        ASSERT_TRUE(setRet.ok());

        // get from meta server
        auto getRet = client->getConfig(module, name).get();
        ASSERT_TRUE(getRet.ok());
        auto item = getRet.value().front();

        conf::Configuration conf;
        auto confRet = conf.parseFromString(item.get_value().getStr());
        ASSERT_TRUE(confRet.ok());
        std::string val;
        auto status = conf.fetchAsString("max_background_jobs", val);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(val, "8");

        // get from cache
        sleep(FLAGS_heartbeat_interval_secs + 1);
        confRet = conf.parseFromString(FLAGS_nested_key);
        ASSERT_TRUE(confRet.ok());
        status = conf.fetchAsString("max_background_jobs", val);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(val, "8");
    }
    {
        auto ret = client->listConfigs(module).get();
        ASSERT_TRUE(ret.ok());
        ASSERT_EQ(ret.value().size(), 6);
    }
}

TEST(ConfigManTest, MockConfigTest) {
    FLAGS_heartbeat_interval_secs = 1;
    fs::TempDir rootPath("/tmp/MockConfigTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.startMeta(rootPath.path());
    cluster.initMetaClient();

    auto module = cpp2::ConfigModule::STORAGE;
    auto mode = cpp2::ConfigMode::MUTABLE;
    auto* client = cluster.metaClient_.get();

    std::vector<cpp2::ConfigItem> configItems;
    for (int i = 0; i < 5; i++) {
        std::string name = "test" + std::to_string(i);
        std::string value = "v" + std::to_string(i);
        configItems.emplace_back(assignConfigItem(module, name, mode, value));
    }
    client->regConfig(configItems);

    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    auto consoleClient = std::make_shared<MetaClient>(threadPool,
        std::vector<HostAddr>{HostAddr("127.0.0.1", cluster.metaServer_->port_)});
    consoleClient->waitForMetadReady();
    // update in console
    for (int i = 0; i < 5; i++) {
        std::string name = "test" + std::to_string(i);
        std::string value = "updated" + std::to_string(i);
        auto setRet = consoleClient->setConfig(module, name, value).get();
        ASSERT_TRUE(setRet.ok());
    }
    // get in console
    for (int i = 0; i < 5; i++) {
        std::string name = "test" + std::to_string(i);
        std::string value = "updated" + std::to_string(i);

        auto getRet = consoleClient->getConfig(module, name).get();
        ASSERT_TRUE(getRet.ok());
        auto item = getRet.value().front();
        ASSERT_EQ(item.get_value().getStr(), value);
    }
}

TEST(ConfigManTest, RocksdbOptionsTest) {
    FLAGS_heartbeat_interval_secs = 1;
    fs::TempDir rootPath("/tmp/RocksdbOptionsTest.XXXXXX");
    auto module = cpp2::ConfigModule::STORAGE;
    auto mode = meta::cpp2::ConfigMode::MUTABLE;
    LOG(INFO) << "Create meta client...";
    mock::MockCluster cluster;
    cluster.startMeta(rootPath.path());
    auto ptr = cluster.metaKV_.get();
    auto* kv = dynamic_cast<kvstore::KVStore*>(ptr);
    meta::MetaClientOptions options;
    auto storagePort = network::NetworkUtils::getAvailablePort();
    HostAddr storageAddr{"127.0.0.1", storagePort};
    std::vector<HostAddr> hosts = {storageAddr};
    TestUtils::createSomeHosts(kv, hosts);
    TestUtils::registerHB(kv, hosts);
    options.localHost_ = storageAddr;
    options.role_ = meta::cpp2::HostRole::STORAGE;
    cluster.initMetaClient(options);
    auto* mClient = cluster.metaClient_.get();

    // mock some rocksdb gflags to meta
    {
        std::vector<cpp2::ConfigItem> configItems;
        FLAGS_rocksdb_db_options = R"({
            "max_background_jobs":"4"
        })";
        configItems.emplace_back(assignConfigItem(
            module, "rocksdb_db_options",
            mode, FLAGS_rocksdb_db_options));
        FLAGS_rocksdb_column_family_options = R"({
            "disable_auto_compactions":"false"
        })";
        configItems.emplace_back(assignConfigItem(
            module, "rocksdb_column_family_options",
            mode, FLAGS_rocksdb_column_family_options));
        mClient->regConfig(configItems);
    }

    std::string dataPath = folly::stringPrintf("%s/storage", rootPath.path());
    cluster.startStorage(storageAddr, dataPath, false);

    cpp2::SpaceDesc spaceDesc;
    spaceDesc.set_space_name("storage");
    spaceDesc.set_partition_num(9);
    spaceDesc.set_replica_factor(1);
    auto ret = mClient->createSpace(std::move(spaceDesc)).get();
    ASSERT_TRUE(ret.ok());
    auto spaceId = ret.value();
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        std::string name = "rocksdb_db_options";
        std::string updateValue = R"({
            "max_background_jobs":"10"
        })";
        // update config
        auto setRet = mClient->setConfig(module, name, updateValue).get();
        ASSERT_TRUE(setRet.ok());

        // get from meta server
        auto getRet = mClient->getConfig(module, name).get();
        ASSERT_TRUE(getRet.ok());
        auto item = getRet.value().front();

        sleep(FLAGS_heartbeat_interval_secs + 3);
        ASSERT_EQ(updateValue, item.get_value().getStr());
    }
    {
        std::string name = "rocksdb_column_family_options";
        std::string updateValue = R"({"disable_auto_compactions":true,
                                      "level0_file_num_compaction_trigger":8,
                                      "write_buffer_size":1048576})";

        // update config
        auto setRet = mClient->setConfig(module, name, updateValue).get();
        ASSERT_TRUE(setRet.ok());

        // get from meta server
        auto getRet = mClient->getConfig(module, name).get();
        ASSERT_TRUE(getRet.ok());
        auto item = getRet.value().front();

        sleep(FLAGS_heartbeat_interval_secs + 3);
        ASSERT_EQ(updateValue, item.get_value().getStr());
    }
    {
        // need to sleep a bit to take effect on rocksdb
        LOG(INFO) << "=========================================";

        std::vector<cpp2::ConfigItem> items;
        auto item1 = assignConfigItem(cpp2::ConfigModule::STORAGE,
                                      "disable_auto_compactions",
                                      cpp2::ConfigMode::MUTABLE,
                                      true);

        auto item2 = assignConfigItem(cpp2::ConfigModule::STORAGE,
                                      "level0_file_num_compaction_trigger",
                                      cpp2::ConfigMode::MUTABLE,
                                      8);

        auto item3 = assignConfigItem(cpp2::ConfigModule::STORAGE,
                                      "write_buffer_size",
                                      cpp2::ConfigMode::MUTABLE,
                                      1048576);

        items.emplace_back(std::move(item1));
        items.emplace_back(std::move(item2));
        items.emplace_back(std::move(item3));

        mClient->gflagsDeclared_ = std::move(items);
        mClient->gflagsModule_ = cpp2::ConfigModule::STORAGE;
        mClient->loadCfg();
        sleep(FLAGS_heartbeat_interval_secs + 2);
        rocksdb::DBOptions loadedDbOpt;
        std::vector<rocksdb::ColumnFamilyDescriptor> loadedCfDescs;
        auto rocksPath = folly::stringPrintf("%s/disk1/nebula/%d/data",
                                              dataPath.c_str(), spaceId);
        auto status = rocksdb::LoadLatestOptions(rocksPath, rocksdb::Env::Default(),
                                                 &loadedDbOpt, &loadedCfDescs);
        ASSERT_TRUE(status.ok());
        EXPECT_EQ(10, loadedDbOpt.max_background_jobs);
        EXPECT_EQ(true, loadedCfDescs[0].options.disable_auto_compactions);
        EXPECT_EQ(8, loadedCfDescs[0].options.level0_file_num_compaction_trigger);
        EXPECT_EQ(1048576, loadedCfDescs[0].options.write_buffer_size);
    }
    cluster.stop();
}

}  // namespace meta
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}

