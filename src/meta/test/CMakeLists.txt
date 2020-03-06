nebula_add_test(
    NAME
        meta_utils_test
    SOURCES
        MetaServiceUtilsTest.cpp
        ../MetaServiceUtils.cpp
    OBJECTS
        $<TARGET_OBJECTS:common_meta_thrift_obj>
        $<TARGET_OBJECTS:common_common_thrift_obj>
        $<TARGET_OBJECTS:common_thrift_obj>
        $<TARGET_OBJECTS:common_base_obj>
        $<TARGET_OBJECTS:common_fs_obj>
        $<TARGET_OBJECTS:common_network_obj>
        $<TARGET_OBJECTS:common_thread_obj>
        $<TARGET_OBJECTS:common_datatypes_obj>
    LIBRARIES
        ${THRIFT_LIBRARIES}
        gtest
)

nebula_add_test(
    NAME
        processor_test
    SOURCES
        ProcessorTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:meta_service_handler>
        $<TARGET_OBJECTS:kvstore_obj>
        $<TARGET_OBJECTS:wal_obj>
        $<TARGET_OBJECTS:raftex_obj>
        $<TARGET_OBJECTS:schema_obj>
        $<TARGET_OBJECTS:common_obj>
        $<TARGET_OBJECTS:common_meta_client_obj>
        $<TARGET_OBJECTS:common_stats_obj>
        $<TARGET_OBJECTS:common_meta_thrift_obj>
        $<TARGET_OBJECTS:common_storage_thrift_obj>
        $<TARGET_OBJECTS:common_common_thrift_obj>
        $<TARGET_OBJECTS:common_raftex_thrift_obj>
        $<TARGET_OBJECTS:common_base_obj>
        $<TARGET_OBJECTS:common_thrift_obj>
        $<TARGET_OBJECTS:common_thread_obj>
        $<TARGET_OBJECTS:common_time_obj>
        $<TARGET_OBJECTS:common_fs_obj>
        $<TARGET_OBJECTS:common_network_obj>
        $<TARGET_OBJECTS:common_thread_obj>
        $<TARGET_OBJECTS:common_meta_obj>
        $<TARGET_OBJECTS:common_process_obj>
        $<TARGET_OBJECTS:common_stats_obj>
        $<TARGET_OBJECTS:common_datatypes_obj>
        $<TARGET_OBJECTS:common_conf_obj>
        $<TARGET_OBJECTS:common_file_based_cluster_id_man_obj>
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)

nebula_add_test(
    NAME
        hb_processor_test
    SOURCES
        HBProcessorTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:meta_service_handler>
        $<TARGET_OBJECTS:kvstore_obj>
        $<TARGET_OBJECTS:wal_obj>
        $<TARGET_OBJECTS:raftex_obj>
        $<TARGET_OBJECTS:schema_obj>
        $<TARGET_OBJECTS:common_obj>
        $<TARGET_OBJECTS:common_meta_client_obj>
        $<TARGET_OBJECTS:common_stats_obj>
        $<TARGET_OBJECTS:common_meta_thrift_obj>
        $<TARGET_OBJECTS:common_storage_thrift_obj>
        $<TARGET_OBJECTS:common_common_thrift_obj>
        $<TARGET_OBJECTS:common_raftex_thrift_obj>
        $<TARGET_OBJECTS:common_thrift_obj>
        $<TARGET_OBJECTS:common_base_obj>
        $<TARGET_OBJECTS:common_fs_obj>
        $<TARGET_OBJECTS:common_time_obj>
        $<TARGET_OBJECTS:common_network_obj>
        $<TARGET_OBJECTS:common_thread_obj>
        $<TARGET_OBJECTS:common_meta_obj>
        $<TARGET_OBJECTS:common_stats_obj>
        $<TARGET_OBJECTS:common_datatypes_obj>
        $<TARGET_OBJECTS:common_conf_obj>
        $<TARGET_OBJECTS:common_file_based_cluster_id_man_obj>
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)

nebula_add_test(
    NAME
        meta_client_test
    SOURCES
        MetaClientTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:meta_service_handler>
        $<TARGET_OBJECTS:kvstore_obj>
        $<TARGET_OBJECTS:wal_obj>
        $<TARGET_OBJECTS:raftex_obj>
        $<TARGET_OBJECTS:meta_gflags_man_obj>
        $<TARGET_OBJECTS:schema_obj>
        $<TARGET_OBJECTS:common_obj>
        $<TARGET_OBJECTS:common_meta_client_obj>
        $<TARGET_OBJECTS:common_stats_obj>
        $<TARGET_OBJECTS:common_storage_thrift_obj>
        $<TARGET_OBJECTS:common_meta_thrift_obj>
        $<TARGET_OBJECTS:common_common_thrift_obj>
        $<TARGET_OBJECTS:common_raftex_thrift_obj>
        $<TARGET_OBJECTS:common_base_obj>
        $<TARGET_OBJECTS:common_thrift_obj>
        $<TARGET_OBJECTS:common_thread_obj>
        $<TARGET_OBJECTS:common_time_obj>
        $<TARGET_OBJECTS:common_fs_obj>
        $<TARGET_OBJECTS:common_network_obj>
        $<TARGET_OBJECTS:common_meta_obj>
        $<TARGET_OBJECTS:common_process_obj>
        $<TARGET_OBJECTS:common_stats_obj>
        $<TARGET_OBJECTS:common_datatypes_obj>
        $<TARGET_OBJECTS:common_conf_obj>
        $<TARGET_OBJECTS:common_file_based_cluster_id_man_obj>
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)

#nebula_add_test(
#    NAME
#        config_man_test
#    SOURCES
#        ConfigManTest.cpp
#    OBJECTS
#        $<TARGET_OBJECTS:meta_service_handler>
#        $<TARGET_OBJECTS:kvstore_obj>
#        $<TARGET_OBJECTS:raftex_obj>
#        $<TARGET_OBJECTS:wal_obj>
#        $<TARGET_OBJECTS:storage_service_handler>
#        $<TARGET_OBJECTS:adHocSchema_obj>
#        $<TARGET_OBJECTS:dataman_obj>
#        $<TARGET_OBJECTS:filter_obj>
#        $<TARGET_OBJECTS:meta_gflags_man_obj>
#        $<TARGET_OBJECTS:schema_obj>
#        $<TARGET_OBJECTS:common_obj>
#        $<TARGET_OBJECTS:common_meta_client_obj>
#        $<TARGET_OBJECTS:common_stats_obj>
#        $<TARGET_OBJECTS:common_meta_thrift_obj>
#        $<TARGET_OBJECTS:common_common_thrift_obj>
#        $<TARGET_OBJECTS:common_storage_client_obj>
#        $<TARGET_OBJECTS:common_storage_thrift_obj>
#        $<TARGET_OBJECTS:common_thrift_obj>
#        $<TARGET_OBJECTS:common_base_obj>
#        $<TARGET_OBJECTS:common_fs_obj>
#        $<TARGET_OBJECTS:common_network_obj>
#        $<TARGET_OBJECTS:common_thread_obj>
#        $<TARGET_OBJECTS:common_time_obj>
#        $<TARGET_OBJECTS:common_meta_obj>
#        $<TARGET_OBJECTS:common_raftex_thrift_obj>
#        $<TARGET_OBJECTS:common_stats_obj>
#        $<TARGET_OBJECTS:common_datatypes_obj>
#        $<TARGET_OBJECTS:common_conf_obj>
#        $<TARGET_OBJECTS:common_file_based_cluster_id_man_obj>
#    LIBRARIES
#        ${ROCKSDB_LIBRARIES}
#        ${THRIFT_LIBRARIES}
#        wangle
#        gtest
#)

nebula_add_test(
    NAME
        active_hosts_man_test
    SOURCES
        ActiveHostsManTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:raftex_obj>
        $<TARGET_OBJECTS:kvstore_obj>
        $<TARGET_OBJECTS:meta_service_handler>
        $<TARGET_OBJECTS:wal_obj>
        $<TARGET_OBJECTS:schema_obj>
        $<TARGET_OBJECTS:common_obj>
        $<TARGET_OBJECTS:common_base_obj>
        $<TARGET_OBJECTS:common_thread_obj>
        $<TARGET_OBJECTS:common_time_obj>
        $<TARGET_OBJECTS:common_fs_obj>
        $<TARGET_OBJECTS:common_network_obj>
        $<TARGET_OBJECTS:common_storage_thrift_obj>
        $<TARGET_OBJECTS:common_meta_thrift_obj>
        $<TARGET_OBJECTS:common_common_thrift_obj>
        $<TARGET_OBJECTS:common_meta_client_obj>
        $<TARGET_OBJECTS:common_stats_obj>
        $<TARGET_OBJECTS:common_thrift_obj>
        $<TARGET_OBJECTS:common_time_obj>
        $<TARGET_OBJECTS:common_meta_obj>
        $<TARGET_OBJECTS:common_raftex_thrift_obj>
        $<TARGET_OBJECTS:common_stats_obj>
        $<TARGET_OBJECTS:common_datatypes_obj>
        $<TARGET_OBJECTS:common_conf_obj>
        $<TARGET_OBJECTS:common_file_based_cluster_id_man_obj>
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)

nebula_add_test(
    NAME
        meta_http_status_test
    SOURCES
        MetaHttpStatusHandlerTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:meta_http_handler>
        $<TARGET_OBJECTS:wal_obj>
        $<TARGET_OBJECTS:raftex_obj>
        $<TARGET_OBJECTS:kvstore_obj>
        $<TARGET_OBJECTS:meta_service_handler>
        $<TARGET_OBJECTS:schema_obj>
        $<TARGET_OBJECTS:common_obj>
        $<TARGET_OBJECTS:common_meta_client_obj>
        $<TARGET_OBJECTS:common_stats_obj>
        $<TARGET_OBJECTS:common_storage_thrift_obj>
        $<TARGET_OBJECTS:common_meta_thrift_obj>
        $<TARGET_OBJECTS:common_common_thrift_obj>
        $<TARGET_OBJECTS:common_raftex_thrift_obj>
        $<TARGET_OBJECTS:common_thrift_obj>
        $<TARGET_OBJECTS:common_ws_obj>
        $<TARGET_OBJECTS:common_ws_common_obj>
        $<TARGET_OBJECTS:common_hdfs_helper_obj>
        $<TARGET_OBJECTS:common_http_client_obj>
        $<TARGET_OBJECTS:common_base_obj>
        $<TARGET_OBJECTS:common_fs_obj>
        $<TARGET_OBJECTS:common_time_obj>
        $<TARGET_OBJECTS:common_stats_obj>
        $<TARGET_OBJECTS:common_network_obj>
        $<TARGET_OBJECTS:common_thread_obj>
        $<TARGET_OBJECTS:common_process_obj>
        $<TARGET_OBJECTS:common_meta_obj>
        $<TARGET_OBJECTS:common_datatypes_obj>
        $<TARGET_OBJECTS:common_conf_obj>
        $<TARGET_OBJECTS:common_file_based_cluster_id_man_obj>
    LIBRARIES
        proxygenhttpserver
        proxygenlib
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)

#nebula_add_test(
#    NAME
#        meta_http_download_test
#    SOURCES
#        MetaHttpDownloadHandlerTest.cpp
#    OBJECTS
#        $<TARGET_OBJECTS:storage_http_handler>
#        $<TARGET_OBJECTS:wal_obj>
#        $<TARGET_OBJECTS:raftex_obj>
#        $<TARGET_OBJECTS:kvstore_obj>
#        $<TARGET_OBJECTS:meta_service_handler>
#        $<TARGET_OBJECTS:meta_http_handler>
#        $<TARGET_OBJECTS:schema_obj>
#        $<TARGET_OBJECTS:common_obj>
#        $<TARGET_OBJECTS:common_meta_client_obj>
#        $<TARGET_OBJECTS:common_stats_obj>
#        $<TARGET_OBJECTS:common_storage_thrift_obj>
#        $<TARGET_OBJECTS:common_meta_thrift_obj>
#        $<TARGET_OBJECTS:common_common_thrift_obj>
#        $<TARGET_OBJECTS:common_raftex_thrift_obj>
#        $<TARGET_OBJECTS:common_thrift_obj>
#        $<TARGET_OBJECTS:common_ws_obj>
#        $<TARGET_OBJECTS:common_ws_common_obj>
#        $<TARGET_OBJECTS:common_hdfs_helper_obj>
#        $<TARGET_OBJECTS:common_http_client_obj>
#        $<TARGET_OBJECTS:common_base_obj>
#        $<TARGET_OBJECTS:common_fs_obj>
#        $<TARGET_OBJECTS:common_time_obj>
#        $<TARGET_OBJECTS:common_stats_obj>
#        $<TARGET_OBJECTS:common_network_obj>
#        $<TARGET_OBJECTS:common_thread_obj>
#        $<TARGET_OBJECTS:common_process_obj>
#        $<TARGET_OBJECTS:common_meta_obj>
#        $<TARGET_OBJECTS:common_datatypes_obj>
#        $<TARGET_OBJECTS:common_conf_obj>
#        $<TARGET_OBJECTS:common_file_based_cluster_id_man_obj>
#    LIBRARIES
#        proxygenhttpserver
#        proxygenlib
#        ${ROCKSDB_LIBRARIES}
#        ${THRIFT_LIBRARIES}
#        wangle
#        gtest
#)

#nebula_add_test(
#    NAME
#        meta_http_ingest_test
#    SOURCES
#        MetaHttpIngestHandlerTest.cpp
#    OBJECTS
#        $<TARGET_OBJECTS:storage_http_handler>
#        $<TARGET_OBJECTS:meta_http_handler>
#        $<TARGET_OBJECTS:wal_obj>
#        $<TARGET_OBJECTS:kvstore_obj>
#        $<TARGET_OBJECTS:raftex_obj>
#        $<TARGET_OBJECTS:meta_service_handler>
#        $<TARGET_OBJECTS:schema_obj>
#        $<TARGET_OBJECTS:common_obj>
#        $<TARGET_OBJECTS:common_meta_client_obj>
#        $<TARGET_OBJECTS:common_stats_obj>
#        $<TARGET_OBJECTS:common_storage_thrift_obj>
#        $<TARGET_OBJECTS:common_meta_thrift_obj>
#        $<TARGET_OBJECTS:common_common_thrift_obj>
#        $<TARGET_OBJECTS:common_raftex_thrift_obj>
#        $<TARGET_OBJECTS:common_thrift_obj>
#        $<TARGET_OBJECTS:common_base_obj>
#        $<TARGET_OBJECTS:common_fs_obj>
#        $<TARGET_OBJECTS:common_time_obj>
#        $<TARGET_OBJECTS:common_stats_obj>
#        $<TARGET_OBJECTS:common_network_obj>
#        $<TARGET_OBJECTS:common_thread_obj>
#        $<TARGET_OBJECTS:common_process_obj>
#        $<TARGET_OBJECTS:common_meta_obj>
#        $<TARGET_OBJECTS:common_datatypes_obj>
#        $<TARGET_OBJECTS:common_conf_obj>
#        $<TARGET_OBJECTS:common_file_based_cluster_id_man_obj>
#    LIBRARIES
#        proxygenhttpserver
#        proxygenlib
#        ${ROCKSDB_LIBRARIES}
#        ${THRIFT_LIBRARIES}
#        wangle
#        gtest
#)

nebula_add_test(
    NAME
        balancer_test
    SOURCES
        BalancerTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:meta_service_handler>
        $<TARGET_OBJECTS:kvstore_obj>
        $<TARGET_OBJECTS:raftex_obj>
        $<TARGET_OBJECTS:wal_obj>
        $<TARGET_OBJECTS:schema_obj>
        $<TARGET_OBJECTS:common_obj>
        $<TARGET_OBJECTS:common_meta_obj>
        $<TARGET_OBJECTS:common_meta_client_obj>
        $<TARGET_OBJECTS:common_stats_obj>
        $<TARGET_OBJECTS:common_storage_thrift_obj>
        $<TARGET_OBJECTS:common_meta_thrift_obj>
        $<TARGET_OBJECTS:common_common_thrift_obj>
        $<TARGET_OBJECTS:common_raftex_thrift_obj>
        $<TARGET_OBJECTS:common_thrift_obj>
        $<TARGET_OBJECTS:common_base_obj>
        $<TARGET_OBJECTS:common_thread_obj>
        $<TARGET_OBJECTS:common_time_obj>
        $<TARGET_OBJECTS:common_fs_obj>
        $<TARGET_OBJECTS:common_network_obj>
        $<TARGET_OBJECTS:common_stats_obj>
        $<TARGET_OBJECTS:common_datatypes_obj>
        $<TARGET_OBJECTS:common_conf_obj>
        $<TARGET_OBJECTS:common_file_based_cluster_id_man_obj>
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)


#nebula_add_test(
#    NAME
#        balance_integration_test
#    SOURCES
#        BalanceIntegrationTest.cpp
#    OBJECTS
#        $<TARGET_OBJECTS:meta_service_handler>
#        $<TARGET_OBJECTS:common_storage_client_obj>
#        $<TARGET_OBJECTS:storage_service_handler>
#        $<TARGET_OBJECTS:kvstore_obj>
#        $<TARGET_OBJECTS:raftex_obj>
#        $<TARGET_OBJECTS:dataman_obj>
#        $<TARGET_OBJECTS:wal_obj>
#        $<TARGET_OBJECTS:filter_obj>
#        $<TARGET_OBJECTS:schema_obj>
#        $<TARGET_OBJECTS:common_obj>
#        $<TARGET_OBJECTS:common_meta_obj>
#        $<TARGET_OBJECTS:common_meta_client_obj>
#        $<TARGET_OBJECTS:common_meta_thrift_obj>
#        $<TARGET_OBJECTS:common_storage_thrift_obj>
#        $<TARGET_OBJECTS:common_raftex_thrift_obj>
#        $<TARGET_OBJECTS:common_common_thrift_obj>
#        $<TARGET_OBJECTS:common_thrift_obj>
#        $<TARGET_OBJECTS:common_thread_obj>
#        $<TARGET_OBJECTS:common_time_obj>
#        $<TARGET_OBJECTS:common_fs_obj>
#        $<TARGET_OBJECTS:common_network_obj>
#        $<TARGET_OBJECTS:adHocSchema_obj>
#        $<TARGET_OBJECTS:common_base_obj>
#        $<TARGET_OBJECTS:common_stats_obj>
#        $<TARGET_OBJECTS:common_datatypes_obj>
#        $<TARGET_OBJECTS:common_conf_obj>
#        $<TARGET_OBJECTS:common_file_based_cluster_id_man_obj>
#    LIBRARIES
#        ${ROCKSDB_LIBRARIES}
#        ${THRIFT_LIBRARIES}
#        wangle
#        gtest
#)


#nebula_add_test(
#    NAME
#        admin_client_test
#    SOURCES
#        AdminClientTest.cpp
#    OBJECTS
#        $<TARGET_OBJECTS:meta_service_handler>
#        $<TARGET_OBJECTS:kvstore_obj>
#        $<TARGET_OBJECTS:raftex_obj>
#        $<TARGET_OBJECTS:wal_obj>
#        $<TARGET_OBJECTS:dataman_obj>
#        $<TARGET_OBJECTS:schema_obj>
#        $<TARGET_OBJECTS:common_obj>
#        $<TARGET_OBJECTS:common_meta_obj>
#        $<TARGET_OBJECTS:common_meta_client_obj>
#        $<TARGET_OBJECTS:common_stats_obj>
#        $<TARGET_OBJECTS:common_storage_thrift_obj>
#        $<TARGET_OBJECTS:common_meta_thrift_obj>
#        $<TARGET_OBJECTS:common_common_thrift_obj>
#        $<TARGET_OBJECTS:common_raftex_thrift_obj>
#        $<TARGET_OBJECTS:common_thrift_obj>
#        $<TARGET_OBJECTS:common_base_obj>
#        $<TARGET_OBJECTS:common_thread_obj>
#        $<TARGET_OBJECTS:common_time_obj>
#        $<TARGET_OBJECTS:common_fs_obj>
#        $<TARGET_OBJECTS:common_network_obj>
#        $<TARGET_OBJECTS:common_stats_obj>
#        $<TARGET_OBJECTS:common_datatypes_obj>
#        $<TARGET_OBJECTS:common_conf_obj>
#        $<TARGET_OBJECTS:common_file_based_cluster_id_man_obj>
#    LIBRARIES
#        ${ROCKSDB_LIBRARIES}
#        ${THRIFT_LIBRARIES}
#        wangle
#        gtest
#)


nebula_add_test(
    NAME
        authentication_test
    SOURCES
        AuthProcessorTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:wal_obj>
        $<TARGET_OBJECTS:raftex_obj>
        $<TARGET_OBJECTS:meta_service_handler>
        $<TARGET_OBJECTS:kvstore_obj>
        $<TARGET_OBJECTS:schema_obj>
        $<TARGET_OBJECTS:common_obj>
        $<TARGET_OBJECTS:common_meta_client_obj>
        $<TARGET_OBJECTS:common_stats_obj>
        $<TARGET_OBJECTS:common_meta_thrift_obj>
        $<TARGET_OBJECTS:common_storage_thrift_obj>
        $<TARGET_OBJECTS:common_common_thrift_obj>
        $<TARGET_OBJECTS:common_raftex_thrift_obj>
        $<TARGET_OBJECTS:common_thrift_obj>
        $<TARGET_OBJECTS:common_base_obj>
        $<TARGET_OBJECTS:common_fs_obj>
        $<TARGET_OBJECTS:common_time_obj>
        $<TARGET_OBJECTS:common_network_obj>
        $<TARGET_OBJECTS:common_thread_obj>
        $<TARGET_OBJECTS:common_meta_obj>
        $<TARGET_OBJECTS:common_datatypes_obj>
        $<TARGET_OBJECTS:common_stats_obj>
        $<TARGET_OBJECTS:common_conf_obj>
        $<TARGET_OBJECTS:common_file_based_cluster_id_man_obj>
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)

nebula_add_test(
    NAME
        kv_based_cluster_id_man_test
    SOURCES
        KVBasedClusterIdManTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:wal_obj>
        $<TARGET_OBJECTS:raftex_obj>
        $<TARGET_OBJECTS:kvstore_obj>
        $<TARGET_OBJECTS:schema_obj>
        $<TARGET_OBJECTS:common_obj>
        $<TARGET_OBJECTS:common_meta_client_obj>
        $<TARGET_OBJECTS:common_stats_obj>
        $<TARGET_OBJECTS:common_meta_thrift_obj>
        $<TARGET_OBJECTS:common_storage_thrift_obj>
        $<TARGET_OBJECTS:common_common_thrift_obj>
        $<TARGET_OBJECTS:common_raftex_thrift_obj>
        $<TARGET_OBJECTS:common_thrift_obj>
        $<TARGET_OBJECTS:common_base_obj>
        $<TARGET_OBJECTS:common_fs_obj>
        $<TARGET_OBJECTS:common_time_obj>
        $<TARGET_OBJECTS:common_network_obj>
        $<TARGET_OBJECTS:common_thread_obj>
        $<TARGET_OBJECTS:common_meta_obj>
        $<TARGET_OBJECTS:common_conf_obj>
        $<TARGET_OBJECTS:common_datatypes_obj>
        $<TARGET_OBJECTS:common_file_based_cluster_id_man_obj>
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)

