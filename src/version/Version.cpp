/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "version/Version.h"

#include <folly/String.h>

/* #undef NEBULA_BUILD_VERSION */

namespace nebula {
namespace storage {

std::string gitInfoSha() {
    return "09270f5";
}

std::string versionString() {
    std::string version;
#if defined(NEBULA_BUILD_VERSION)
    version = folly::stringPrintf("%s, ", "");
#endif
    version += folly::stringPrintf("Git: %s", gitInfoSha().c_str());
    version += folly::stringPrintf(", Build Time: %s %s", __DATE__, __TIME__);
    version += "\nThis source code is licensed under Apache 2.0 License,"
               " attached with Common Clause Condition 1.0.";
    return version;
}

}   // namespace storage
}   // namespace nebula
