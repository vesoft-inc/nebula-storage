/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/context/UpdateExpressionContext.h"

namespace nebula {
namespace storage {

const Value& UpdateExpressionContext::getVar(const std::string& var) const {
    UNUSED(var);
    return kNullValue;
}

const Value& UpdateExpressionContext::getVersionedVar(const std::string& var,
                                                      int64_t version) const {
    UNUSED(var);
    UNUSED(version);
    return kNullValue;
}

const Value& UpdateExpressionContext::getVarProp(const std::string& var,
                                                 const std::string& prop) const {
    UNUSED(var);
    UNUSED(prop);
    return kNullValue;
}

const Value& UpdateExpressionContext::getDstProp(const std::string& tagName,
                                                 const std::string& prop) const {
    UNUSED(tagName);
    UNUSED(prop);
    return kNullValue;
}

const Value& UpdateExpressionContext::getInputProp(const std::string& prop) const {
    UNUSED(prop);
    return kNullValue;
}

const Value& UpdateExpressionContext::getEdgeProp(const std::string& edgeName,
                                                  const std::string& prop) const {
    if (!edgeName.compare(edgeName_)) {
        auto iter = edgePropVals_.find(prop);
        if (iter != edgePropVals_.end()) {
            return iter->second;
        }
    }
    return kNullValue;
}

const Value& UpdateExpressionContext::getSrcProp(const std::string& tagName,
                                                 const std::string& prop) const {
    if (!tagName.compare(tagName_)) {
        auto iter = srcPropVals_.find(prop);
        if (iter == srcPropVals_.end()) {
            return iter->second;
        }
    }
    return kNullValue;
}

bool UpdateExpressionContext::setEdgeProp(const std::string& edgeName,
                                          const std::string& prop,
                                          Value val) {
    if (!edgeName_.empty() && edgeName.compare(edgeName_)) {
        return false;
    }
    if (edgeName_.empty()) {
        edgeName_ = edgeName;
    }
    edgePropVals_[prop] = val;
    return true;
}

bool UpdateExpressionContext::setSrcProp(const std::string& tagName,
                                         const std::string& prop,
                                         Value val) {
    if (!tagName_.empty() && tagName.compare(tagName_)) {
        return false;
    }
    if (tagName_.empty()) {
        tagName_ = tagName;
    }
    srcPropVals_[prop] = val;
    return true;
}

}  // namespace storage
}  // namespace nebula

