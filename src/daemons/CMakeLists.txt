#nebula_add_executable(
#    NAME
#        nebula-storaged
#    SOURCES
#        StorageDaemon.cpp
#    OBJECTS
#        $<TARGET_OBJECTS:filter_obj>
#        $<TARGET_OBJECTS:storage_server>
#        $<TARGET_OBJECTS:storage_service_handler>
#        $<TARGET_OBJECTS:storage_http_handler>
#        $<TARGET_OBJECTS:kvstore_obj>
#        $<TARGET_OBJECTS:raftex_obj>
#        $<TARGET_OBJECTS:meta_gflags_man_obj>
#        $<TARGET_OBJECTS:wal_obj>
#        $<TARGET_OBJECTS:dataman_obj>
#        $<TARGET_OBJECTS:schema_obj>
#        $<TARGET_OBJECTS:common_obj>
#        $<TARGET_OBJECTS:common_raftex_thrift_obj>
#        $<TARGET_OBJECTS:common_hdfs_helper_obj>
#        $<TARGET_OBJECTS:common_storage_thrift_obj>
#        $<TARGET_OBJECTS:common_http_client_obj>
#        $<TARGET_OBJECTS:common_general_storage_client_obj>
#        $<TARGET_OBJECTS:common_meta_client_obj>
#        $<TARGET_OBJECTS:common_stats_obj>
#        $<TARGET_OBJECTS:common_time_obj>
#        $<TARGET_OBJECTS:common_meta_thrift_obj>
#        $<TARGET_OBJECTS:common_common_thrift_obj>
#        $<TARGET_OBJECTS:common_thrift_obj>
#        $<TARGET_OBJECTS:common_meta_obj>
#        $<TARGET_OBJECTS:common_network_obj>
#        $<TARGET_OBJECTS:common_process_obj>
#        $<TARGET_OBJECTS:common_ws_obj>
#        $<TARGET_OBJECTS:common_ws_common_obj>
#        $<TARGET_OBJECTS:common_thread_obj>
#        $<TARGET_OBJECTS:common_time_obj>
#        $<TARGET_OBJECTS:common_fs_obj>
#        $<TARGET_OBJECTS:common_stats_obj>
#        $<TARGET_OBJECTS:common_conf_obj>
#        $<TARGET_OBJECTS:common_datatypes_obj>
#        $<TARGET_OBJECTS:common_base_obj>
#    LIBRARIES
#        proxygenhttpserver
#        proxygenlib
#        ${ROCKSDB_LIBRARIES}
#        ${THRIFT_LIBRARIES}
#        wangle
#)


nebula_add_executable(
    NAME
        nebula-metad
    SOURCES
        MetaDaemon.cpp
    OBJECTS
        $<TARGET_OBJECTS:meta_service_handler>
        $<TARGET_OBJECTS:meta_http_handler>
        $<TARGET_OBJECTS:kvstore_obj>
        $<TARGET_OBJECTS:schema_obj>
        $<TARGET_OBJECTS:wal_obj>
        $<TARGET_OBJECTS:raftex_obj>
        $<TARGET_OBJECTS:kv_gflags_man_obj>
        $<TARGET_OBJECTS:common_obj>
        $<TARGET_OBJECTS:common_file_based_cluster_id_man_obj>
        $<TARGET_OBJECTS:common_storage_client_gflags_obj>
        $<TARGET_OBJECTS:common_raftex_thrift_obj>
        $<TARGET_OBJECTS:common_common_thrift_obj>
        $<TARGET_OBJECTS:common_thrift_obj>
        $<TARGET_OBJECTS:common_process_obj>
        $<TARGET_OBJECTS:common_ws_obj>
        $<TARGET_OBJECTS:common_ws_common_obj>
        $<TARGET_OBJECTS:common_hdfs_helper_obj>
        $<TARGET_OBJECTS:common_http_client_obj>
        $<TARGET_OBJECTS:common_general_storage_client_obj>
        $<TARGET_OBJECTS:common_storage_thrift_obj>
        $<TARGET_OBJECTS:common_meta_client_obj>
        $<TARGET_OBJECTS:common_stats_obj>
        $<TARGET_OBJECTS:common_time_obj>
        $<TARGET_OBJECTS:common_meta_thrift_obj>
        $<TARGET_OBJECTS:common_meta_obj>
        $<TARGET_OBJECTS:common_network_obj>
        $<TARGET_OBJECTS:common_thread_obj>
        $<TARGET_OBJECTS:common_stats_obj>
        $<TARGET_OBJECTS:common_time_obj>
        $<TARGET_OBJECTS:common_fs_obj>
        $<TARGET_OBJECTS:common_conf_obj>
        $<TARGET_OBJECTS:common_datatypes_obj>
        $<TARGET_OBJECTS:common_base_obj>
    LIBRARIES
        proxygenhttpserver
        proxygenlib
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        wangle
)

install(
    TARGETS
#        nebula-storaged
        nebula-metad
    DESTINATION
        bin
)

