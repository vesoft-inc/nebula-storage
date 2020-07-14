#!/usr/bin/env bash
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

set -ex -o pipefail

PROJ_DIR="$(cd "$(dirname "$0")" && pwd)/.."
BUILD_DIR=$PROJ_DIR/_build
TOOLSET_DIR=/opt/vesoft/toolset/clang/9.0.0

mkdir -p $BUILD_DIR

function prepare() {
    $PROJ_DIR/ci/deploy.sh
}

function build_common() {
    pushd $PROJ_DIR/modules/common
    cmake .
    make -j8
    popd
}

function gcc_compile() {
    cd $PROJ_DIR
    cmake \
        -DCMAKE_CXX_COMPILER=$TOOLSET_DIR/bin/g++ \
        -DCMAKE_C_COMPILER=$TOOLSET_DIR/bin/gcc \
        -DCMAKE_BUILD_TYPE=Release \
        -DENABLE_TESTING=on \
        -DCMAKE_PREFIX_PATH=modules/common \
        -DNEBULA_THIRDPARTY_ROOT=/opt/vesoft/third-party/ \
        -B $BUILD_DIR
    build_common
    cmake --build $BUILD_DIR -j $(nproc)
}

function clang_compile() {
    cd $PROJ_DIR
    cmake \
        -DCMAKE_CXX_COMPILER=$TOOLSET_DIR/bin/clang++ \
        -DCMAKE_C_COMPILER=$TOOLSET_DIR/bin/clang \
        -DCMAKE_BUILD_TYPE=Debug \
        -DENABLE_ASAN=on \
        -DENABLE_TESTING=on \
        -DCMAKE_PREFIX_PATH=modules/common \
        -DNEBULA_THIRDPARTY_ROOT=/opt/vesoft/third-party \
        -B $BUILD_DIR
    build_common
    cmake --build $BUILD_DIR -j $(nproc)
}

function run_test() {
    cd $BUILD_DIR
    ctest -j $(nproc) \
          --timeout 400 \
          --output-on-failure
}

case "$1" in
    prepare)
        prepare
        ;;
    clang)
        clang_compile
        ;;
    gcc)
        gcc_compile
        ;;
    test)
        run_test
        ;;
    *)
        prepare
        gcc_compile
        run_test
        ;;
esac
