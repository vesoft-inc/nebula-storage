
# The base_obj is a basic library. Other libraries are not allowed to depend on it.

add_library(
    keyutils_obj OBJECT
    NebulaKeyUtils.cpp
    IndexKeyUtils.cpp
    OperationKeyUtils.cpp
)

nebula_add_subdirectory(test)
