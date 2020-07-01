/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_CONTEXT_UPDATEEXPRESSIONCONTEXT_H_
#define STORAGE_CONTEXT_UPDATEEXPRESSIONCONTEXT_H_

#include "common/base/Base.h"
#include "storage/context/StorageExpressionContext.h"

namespace nebula {
namespace storage {

class UpdateExpressionContext final : public StorageExpressionContext {
public:
    explicit UpdateExpressionContext(size_t vIdLen)
        : StorageExpressionContext(vIdLen) {}

    // Get the specified property from the edge, such as edgename.prop_name
    const Value& getEdgeProp(const std::string& edgeName,
                             const std::string& prop) const override;

    // Get the specified property of tagName from the source vertex,
    // such as $^.tagName.prop_name
    const Value& getSrcProp(const std::string& tagName,
                            const std::string& prop) const override;

    // Set edge prop and value
    bool setEdgeProp(const std::string& edgeName, const std::string& prop,
                     const Value& val);

    // Set vertex tag prop and value
    bool setTagProp(const std::string& tagName, const std::string& prop,
                    const Value& value);

private:
    std::string                                 tagName_;

    // Update(upsert) vertex only handles one tag one row
    // Tag prop -> value
    std::unordered_map<std::string, Value>      tagPropVals_;

    std::string                                 edgeName_;

    // Update(upsert) edge only handles one edgetype one row
    // Edge prop -> value
    std::unordered_map<std::string, Value>      edgePropVals_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_CONTEXT_UPDATEEXPRESSIONCONTEXT_H_
