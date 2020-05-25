/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_CONTEXT_UPDATEEXPRESSIONCONTEXT_H_
#define STORAGE_CONTEXT_UPDATEEXPRESSIONCONTEXT_H_

#include "common/context/ExpressionContext.h"
#include "common/datatypes/Value.h"

namespace nebula {
namespace storage {

class UpdateExpressionContext final : public ExpressionContext {
public:
    // Get the latest version value for the given variable name, such as $a, $b
    const Value& getVar(const std::string& var) const override;

    // Get the given version value for the given variable name, such as $a, $b
    const Value& getVersionedVar(const std::string& var,
                                 int64_t version) const override;

    // Get the specified property from a variable, such as $a.prop_name
    const Value& getVarProp(const std::string& var,
                            const std::string& prop) const override;

    // Get the specified property of tagName from the destination vertex,
    // such as $$.tagName.prop_name
    const Value& getDstProp(const std::string& tagName,
                            const std::string& prop) const override;

    // Get the specified property from the input, such as $-.prop_name
    const Value& getInputProp(const std::string& prop) const override;


    // Get the specified property from the edge, such as edgename.prop_name
    const Value& getEdgeProp(const std::string& edgeName,
                             const std::string& prop) const override;

    // Get the specified property of tagName from the source vertex,
    // such as $^.tagName.prop_name
    const Value& getSrcProp(const std::string& tagName,
                            const std::string& prop) const override;


    void setVar(const std::string& var, Value val) override {
        UNUSED(var);
        UNUSED(val);
    }

    // Set edge prop and value
    bool setEdgeProp(const std::string& edgeName, const std::string& prop, Value val);

    // Set vertex tag prop and value
    bool setSrcProp(const std::string& tagName, const std::string& prop, Value val);

private:
    std::string                                 tagName_;

    // Update(upsert) vertex only handles one tag one row
    // For specified tag, prop> -> value
    std::unordered_map<std::string, Value>      srcPropVals_;

    std::string                                 edgeName_;

    // Update(upsert) edge only handles one edgetype one row, not handle source vertex props
    std::unordered_map<std::string, Value>      edgePropVals_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_CONTEXT_UPDATEEXPRESSIONCONTEXT_H_
