set(THRIFT1 ${Fbthrift_BIN})
set(THRIFT_TEMPLATES ${Fbthrift_INCLUDE_DIR}/thrift/templates)
include(ThriftGenerate)

# Target object name : old_thrift_obj
thrift_generate("old_meta" "" ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR} "tools/metaDataUpdate/oldThrift")

add_library(
    old_meta_obj OBJECT
    MetaServiceUtilsV1.cpp
)

add_dependencies(
    old_meta_obj
    old_meta_thrift_headers
)