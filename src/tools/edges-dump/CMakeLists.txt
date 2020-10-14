nebula_add_executable(
    NAME
        edges_dump
    SOURCES
        EdgesDumpTool.cpp
    OBJECTS
        ${tools_test_deps}
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
        ${THRIFT_LIBRARIES}
        glog
        gflags
)

