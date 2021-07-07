# Copyright (c) 2018 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
#
# The build can be controlled by defining following variables on the
# <cmake> command line
#
#   CMAKE_C_COMPILER               -- Specify the compiler for C language
#   CMAKE_CXX_COMPILER             -- Specify the compiler for C++ language
#
#   NEBULA_THIRDPARTY_ROOT         -- Specify the root directory for third-party
#   NEBULA_OTHER_ROOT              -- Specify the root directory for user build
#                                  -- Split with ":", exp: DIR:DIR
#
#   NEBULA_COMMON_SOURCE_DIR       -- Path to nebula-common source directory
#   NEBULA_COMMON_BUILD_DIR        -- Path to nebula-common build directory
#   NEBULA_COMMON_REPO_URL         -- Git URL for the nebula-common repo
#   NEBULA_COMMON_REPO_TAG         -- Tag/branch of the nebula-common repo
#
#   ENABLE_JEMALLOC                -- Link jemalloc into all executables
#   ENABLE_TESTING                 -- Build unit test
#   ENABLE_PACK_ONE                -- Package to one or multi packages
#
cmake_minimum_required(VERSION 3.9.0)

project("Nebula Storage" C CXX)

option(ENABLE_PACK_ONE "Whether to package into one" ON)
option(ENABLE_MODULE_UPDATE "Automatically update module" OFF)

message(STATUS "ENABLE_PACK_ONE         : ${ENABLE_PACK_ONE}")
message(STATUS "ENABLE_MODULE_UPDATE    : ${ENABLE_MODULE_UPDATE}")

add_definitions(-DNEBULA_HOME=${CMAKE_SOURCE_DIR})

# Submodules
if("${NEBULA_COMMON_REPO_URL}" STREQUAL "")
    SET(NEBULA_COMMON_REPO_URL "https://github.com/vesoft-inc/nebula-common.git")
endif()

if("${NEBULA_COMMON_REPO_TAG}" STREQUAL "")
    SET(NEBULA_COMMON_REPO_TAG "master")
endif()

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)
if(NOT NEBULA_COMMON_SOURCE_DIR)
    include(FetchModule)
    nebula_fetch_module(
        NAME
            common
        URL
            ${NEBULA_COMMON_REPO_URL}
        TAG
            ${NEBULA_COMMON_REPO_TAG}
        UPDATE
            ${ENABLE_MODULE_UPDATE}
    )
    set(nebula_common_source_dir ${CMAKE_SOURCE_DIR}/modules/common)
    set(nebula_common_build_dir ${CMAKE_BINARY_DIR}/modules/common)
else()
    message(STATUS "NEBULA_COMMON_SOURCE_DIR: " ${NEBULA_COMMON_SOURCE_DIR})
    set(nebula_common_source_dir ${NEBULA_COMMON_SOURCE_DIR})
    if(NOT NEBULA_COMMON_BUILD_DIR)
        set(nebula_common_build_dir ${CMAKE_BINARY_DIR}/modules/common)
    else()
        set(nebula_common_build_dir ${NEBULA_COMMON_BUILD_DIR})
    endif()
endif()

list(APPEND CMAKE_MODULE_PATH ${nebula_common_source_dir}/cmake)
list(APPEND CMAKE_MODULE_PATH ${nebula_common_source_dir}/cmake/nebula)

include(PlatformCheck)
include(NebulaCMakeMacros)
include(GeneralCMakeOptions)
include(GeneralCMakeConfig)
include(GeneralCompilerConfig)
include(LinkerConfig)
include(CcacheConfig)
include(ThirdPartyConfig)
include(SanitizerConfig)
include(GitHooksConfig)
include(GitInfoConfig)
include(ConfigNebulaCommon)
config_nebula_common(
    SOURCE_DIR ${nebula_common_source_dir}
    BUILD_DIR ${nebula_common_build_dir}
)

# For simplicity, we make all ordinary libraries depend on the compile-time generated files,
# including the precompiled header, a.k.a Base.h.gch, and thrift headers.
macro(nebula_add_library name type)
    add_library(${name} ${type} ${ARGN})
    add_dependencies(
        ${name}
        common_project
    )
endmacro()

include_directories(AFTER ${CMAKE_SOURCE_DIR}/src)
include_directories(AFTER ${CMAKE_CURRENT_BINARY_DIR}/src)
include_directories(AFTER ${CMAKE_CURRENT_BINARY_DIR}/src/kvstore/plugins)
include_directories(AFTER ${CMAKE_CURRENT_BINARY_DIR}/src/kvstore/plugins/hbase)

nebula_add_subdirectory(src)
nebula_add_subdirectory(conf)
nebula_add_subdirectory(scripts)

add_custom_target(
    clean-modules
    COMMAND "rm" "-fr" "modules/*"
)

add_custom_target(
    clean-build
    COMMAND ${CMAKE_MAKE_PROGRAM} clean
    COMMAND "find" "." "-name" "Testing" "|" "xargs" "rm" "-fr"
)

add_custom_target(
    clean-all
    COMMAND ${CMAKE_MAKE_PROGRAM} clean
    COMMAND "find" "." "-name" "Testing" "|" "xargs" "rm" "-fr"
    DEPENDS clean-modules
)

add_custom_target(
    distclean
    COMMAND "find" "." "-name" "CMakeFiles" "|" "xargs" "rm" "-fr"
    COMMAND "find" "." "-name" "CMakeCache.txt" "|" "xargs" "rm" "-f"
    COMMAND "find" "." "-name" "cmake_install.cmake" "|" "xargs" "rm" "-f"
    COMMAND "find" "." "-name" "CTestTestfile.cmake" "|" "xargs" "rm" "-f"
    COMMAND "find" "." "-name" "CPackConfig.cmake" "|" "xargs" "rm" "-f"
    COMMAND "find" "." "-name" "CPackSourceConfig.cmake" "|" "xargs" "rm" "-f"
    COMMAND "find" "." "-name" "Makefile" "|" "xargs" "rm" "-f"
    DEPENDS clean-all
)

include(CPackage)
package(${ENABLE_PACK_ONE}
        "nebula-storage"
        "https://github.com/vesoft-inc/nebula-storage/releases"
        ${CMAKE_SOURCE_DIR}/package)
