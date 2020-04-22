nebula_add_test(
    NAME
        nebula_key_utils_test
    SOURCES
        NebulaKeyUtilsTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:keyutils_obj>
    LIBRARIES
        gtest
)

nebula_add_test(
    NAME
        index_key_utils_test
    SOURCES
        IndexKeyUtilsTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:keyutils_obj>
        $<TARGET_OBJECTS:common_base_obj>
        $<TARGET_OBJECTS:common_datatypes_obj>
    LIBRARIES
        gtest
)

nebula_add_test(
    NAME thrift_client__test
    SOURCES ThriftClientTest.cpp
    OBJECTS
    $<TARGET_OBJECTS:common_storage_thrift_obj>
    $<TARGET_OBJECTS:common_meta_thrift_obj>
    $<TARGET_OBJECTS:common_common_thrift_obj>
    $<TARGET_OBJECTS:common_thrift_obj>
    $<TARGET_OBJECTS:common_process_obj>
    $<TARGET_OBJECTS:common_fs_obj>
    $<TARGET_OBJECTS:common_time_obj>
    $<TARGET_OBJECTS:common_network_obj>
    $<TARGET_OBJECTS:common_thread_obj>
    $<TARGET_OBJECTS:common_conf_obj>
    $<TARGET_OBJECTS:common_datatypes_obj>
    $<TARGET_OBJECTS:common_base_obj>
    LIBRARIES
    gtest
    ${THRIFT_LIBRARIES}
)
