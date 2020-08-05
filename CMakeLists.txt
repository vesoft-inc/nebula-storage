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
#   ENABLE_NATIVE                  -- Build native client
#   ENABLE_TESTING                 -- Build unit test
#   ENABLE_PACK_ONE                -- Package to one or multi packages
#
cmake_minimum_required(VERSION 3.5.0)

project("Nebula Storage" C CXX)

option(ENABLE_MODULE_UPDATE "Automatically update module" OFF)

if (ENABLE_NATIVE)
    message(STATUS "ENABLE_NATIVE is ${ENABLE_NATIVE}")
    add_compile_options(-fPIC)
endif()

add_definitions(-DNEBULA_HOME=${CMAKE_SOURCE_DIR})

# Submodules
if("${NEBULA_COMMON_REPO_URL}" STREQUAL "")
    SET(NEBULA_COMMON_REPO_URL "git@github.com:vesoft-inc-private/nebula-common.git")
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
include(GeneralCMakeOptions)
include(GeneralCMakeConfig)
include(GeneralCompilerConfig)
include(LinkerConfig)
include(CcacheConfig)
include(ThirdPartyConfig)
include(SanitizerConfig)
include(GitHooksConfig)
include(GitInfoConfig)
include(NebulaCMakeMacros)
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

# package nebula to a deb/rpm file
set(CPACK_PACKAGE_DESCRIPTION "nebula-storage")
set(CPACK_PACKAGE_CONTACT "nebula-storage")
set(CPACK_PACKAGE_VERSION ${NEBULA_BUILD_VERSION})
set(CPACK_RPM_PACKAGE_LICENSE "Apache 2.0 + Common Clause 1.0")
set(CPACK_PACKAGE_NAME nebula-storage)
set(CPACK_SET_DESTDIR TRUE)
set(CPACK_PACKAGE_RELOCATABLE FALSE)
set(CPACK_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX})

set(CPACK_DEB_COMPONENT_INSTALL ON)
set(CPACK_DEBIAN_PACKAGE_ARCHITECTURE "amd64")
set(CPACK_DEBIAN_PACKAGE_HOMEPAGE "https://github.com/vesoft-inc/nebula/releases")
set(CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA ${CMAKE_CURRENT_SOURCE_DIR}/package/postinst)

set(CPACK_RPM_SPEC_MORE_DEFINE "%define debug_package %{nil}
        %define __os_install_post %{nil}")
set(CPACK_RPM_COMPONENT_INSTALL ON)
set(CPACK_RPM_PACKAGE_ARCHITECTURE "x86_64")
set(CPACK_RPM_PACKAGE_URL "https://github.com/vesoft-inc/nebula/releases")
set(CPACK_RPM_POST_INSTALL_SCRIPT_FILE ${CMAKE_CURRENT_SOURCE_DIR}/package/postinst)
set(CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION /usr/local)

include(CPack)

include(CPackage)
package(${ENABLE_PACK_ONE} ${PACKAGE_NAME} "https://github.com/vesoft-inc/nebula-storage/releases")
