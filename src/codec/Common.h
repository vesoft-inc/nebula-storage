/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CODEC_COMMON_H_
#define CODEC_COMMON_H_

#include "base/Base.h"

namespace nebula {

template<typename IntType>
typename std::enable_if<
    std::is_integral<
        typename std::remove_cv<
            typename std::remove_reference<IntType>::type
        >::type
    >::value,
    bool
>::type
intToBool(IntType iVal) {
    return iVal != 0;
}

inline bool strToBool(folly::StringPiece str) {
    return str == "Y" || str == "y" || str == "T" || str == "t" ||
           str == "yes" || str == "Yes" || str == "YES" ||
           str == "true" || str == "True" || str == "TRUE";
}

}  // namespace nebula
#endif  // CODEC_COMMON_H_

