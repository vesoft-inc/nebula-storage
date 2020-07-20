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

function lint() {
    cd $PROJ_DIR
    ln -snf $PROJ_DIR/.linters/cpp/hooks/pre-commit.sh $PROJ_DIR/.linters/cpp/pre-commit.sh
    $PROJ_DIR/.linters/cpp/pre-commit.sh $(git --no-pager diff --diff-filter=d --name-only HEAD^ HEAD)
}

function build_common() {
    cmake --build $PROJ_DIR/modules/common -j$(nproc)
}

function gcc_compile() {
    cd $PROJ_DIR
    cmake \
        -DCMAKE_CXX_COMPILER=$TOOLSET_DIR/bin/g++ \
        -DCMAKE_C_COMPILER=$TOOLSET_DIR/bin/gcc \
        -DCMAKE_BUILD_TYPE=Release \
        -DENABLE_TESTING=on \
        -DCMAKE_PREFIX_PATH=modules/common \
        -DNEBULA_COMMON_REPO_URL=$NEBULA_COMMON_REPO_URL \
        -DNEBULA_THIRDPARTY_ROOT=/opt/vesoft/third-party/ \
        -B $BUILD_DIR
    build_common
    cmake --build $BUILD_DIR -j$(nproc)
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
        -DNEBULA_COMMON_REPO_URL=$NEBULA_COMMON_REPO_URL \
        -DNEBULA_THIRDPARTY_ROOT=/opt/vesoft/third-party \
        -B $BUILD_DIR
    build_common
    cmake --build $BUILD_DIR -j$(nproc)
}

function run_test() {
    cd $BUILD_DIR
    ctest -j$(nproc) \
          --timeout 400 \
          --output-on-failure
}

case "$1" in
    lint)
        lint
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
        gcc_compile
        run_test
        ;;
esac
