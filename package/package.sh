#!/usr/bin/env bash
#
# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

#
#  Package nebula as deb/rpm package
#
# introduce the args
#   -v: The version of package, the version should be match tag name, default value is the `commitId`
#   -n: Package to one or multi packages, `ON` means one package, `OFF` means multi packages, default value is `ON`
#   -s: Whether to strip the package, default value is `FALSE`
#
# usage: ./package.sh -v <version> -n <ON/OFF> -s <TRUE/FALSE> -b <BRANCH>
#

set -e

version=""
package_one=ON
strip_enable="FALSE"
usage="Usage: ${0} -v <version> -n <ON/OFF> -s <TRUE/FALSE> -b <BRANCH>"
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)/.."
enablesanitizer="OFF"
build_type="Release"
branch="master"

while getopts v:n:s:b:d opt;
do
    case $opt in
        v)
            version=$OPTARG
            ;;
        n)
            package_one=$OPTARG
            ;;
        s)
            strip_enable=$OPTARG
            ;;
        b)
            branch=$OPTARG
            ;;
        d)
            enablesanitizer="ON"
            build_type="RelWithDebInfo"
            ;;
        ?)
            echo "Invalid option, use default arguments"
            ;;
    esac
done

# version is null, get from tag name
[[ -z $version ]] && version=`git describe --exact-match --abbrev=0 --tags | sed 's/^v//'`
# version is null, use UTC date as version
[[ -z $version ]] && version=$(date -u +%Y.%m.%d)-nightly

if [[ -z $version ]]; then
    echo "version is null, exit"
    echo ${usage}
    exit -1
fi


if [[ $strip_enable != TRUE ]] && [[ $strip_enable != FALSE ]]; then
    echo "strip enable is wrong, exit"
    echo ${usage}
    exit -1
fi

echo "current version is [ $version ], strip enable is [$strip_enable], enablesanitizer is [$enablesanitizer]"

# args: <version>
function build {
    version=$1
    san=$2
    build_type=$3
    branch=$4
    build_dir=$PROJECT_DIR/build
    if [[ -d $build_dir ]]; then
        rm -rf ${build_dir}/*
    else
        mkdir ${build_dir}
    fi

    pushd ${build_dir}

    cmake \
        -DCMAKE_BUILD_TYPE=${build_type} \
        -DNEBULA_BUILD_VERSION=${version} \
        -DNEBULA_COMMON_REPO_TAG=${branch} \
        -DENABLE_ASAN=${san} \
        -DENABLE_UBSAN=${san} \
        -DCMAKE_INSTALL_PREFIX=/usr/local/nebula \
        -DENABLE_TESTING=OFF \
        -DENABLE_PACK_ONE=${package_one} \
        $PROJECT_DIR

    if !( make -j$(nproc) ); then
        echo ">>> build nebula failed <<<"
        exit -1
    fi

    popd
}

# args: <strip_enable>
function package {
    strip_enable=$1
    pushd $PROJECT_DIR/build/
    args=""
    [[ $strip_enable == TRUE ]] && args="-D CPACK_STRIP_FILES=TRUE -D CPACK_RPM_SPEC_MORE_DEFINE="

    sys_ver=""
    pType="RPM"
    if [[ -f "/etc/redhat-release" ]]; then
        sys_name=`cat /etc/redhat-release | cut -d ' ' -f1`
        if [[ ${sys_name} == "CentOS" ]]; then
            sys_ver=`cat /etc/redhat-release | tr -dc '0-9.' | cut -d \. -f1`
            sys_ver=.el${sys_ver}.x86_64
        elif [[ ${sys_name} == "Fedora" ]]; then
            sys_ver=`cat /etc/redhat-release | cut -d ' ' -f3`
            sys_ver=.fc${sys_ver}.x86_64
        fi
        pType="RPM"
    elif [[ -f "/etc/lsb-release" ]]; then
        sys_ver=`cat /etc/lsb-release | grep DISTRIB_RELEASE | cut -d "=" -f 2 | sed 's/\.//'`
        sys_ver=.ubuntu${sys_ver}.amd64
        pType="DEB"
    fi

    if !( cpack -G ${pType} --verbose $args ); then
        echo ">>> package nebula failed <<<"
        exit -1
    else
        # rename package file
        pkg_names=`ls | grep nebula | grep ${version}`
        outputDir=$PROJECT_DIR/build/cpack_output
        mkdir -p ${outputDir}
        for pkg_name in ${pkg_names[@]};
        do
            new_pkg_name=${pkg_name/\-Linux/${sys_ver}}
            mv ${pkg_name} ${outputDir}/${new_pkg_name}
            echo "####### taget package file is ${outputDir}/${new_pkg_name}"
        done
    fi

    popd
}


# The main
build $version $enablesanitizer $build_type $branch
package $strip_enable
