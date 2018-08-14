/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "dataman/RowUpdater.h"
#include "dataman/RowWriter.h"

namespace vesoft {
namespace vgraph {

using namespace folly::hash;

RowUpdater::RowUpdater(SchemaProviderIf* schema,
                       folly::StringPiece row)
        : schema_(schema)
        , reader_(row.empty() ? nullptr : new RowReader(schema_, row)) {
    CHECK(schema_);
    schemaVer_ = schema->getLatestVer();
}


std::string RowUpdater::encode() const noexcept {
    std::string encoded;
    // TODO Reserve enough space so resize will not happen
    encodeTo(encoded);

    return std::move(encoded);
}


void RowUpdater::encodeTo(std::string& encoded) const noexcept {
    RowWriter writer(schema_, schemaVer_);
    auto it = schema_->begin(schemaVer_);
    while(bool(it)) {
        switch(it->getType()->get_type()) {
            case cpp2::SupportedType::BOOL: {
                RU_OUTPUT_VALUE(bool, Bool, false);
                break;
            }
            case cpp2::SupportedType::INT: {
                RU_OUTPUT_VALUE(int64_t, Int, 0);
                break;
            }
            case cpp2::SupportedType::FLOAT: {
                RU_OUTPUT_VALUE(float, Float, (float)0.0);
                break;
            }
            case cpp2::SupportedType::DOUBLE: {
                RU_OUTPUT_VALUE(double, Double, (double)0.0);
                break;
            }
            case cpp2::SupportedType::STRING: {
                RU_OUTPUT_VALUE(folly::StringPiece, String, "");
                break;
            }
            case cpp2::SupportedType::VID: {
                RU_OUTPUT_VALUE(int64_t, Vid, 0);
                break;
            }
            default: {
                LOG(FATAL) << "Unimplemented";
            }
        }
        ++it;
    }

    return writer.encodeTo(encoded);
}


/***************************************************
 *
 * Field Updaters
 *
 **************************************************/
ResultType RowUpdater::setBool(const folly::StringPiece name,
                               bool v) noexcept {
    RU_GET_TYPE_BY_NAME()

    uint64_t hash;
    switch (type->get_type()) {
        case cpp2::SupportedType::BOOL:
            hash = SpookyHashV2::Hash64(name.begin(), name.size(), 0);
            updatedFields_[hash] = v;
            break;
        default:
            return ResultType::E_INCOMPATIBLE_TYPE;
    }

    return ResultType::SUCCEEDED;
}


ResultType RowUpdater::setFloat(const folly::StringPiece name,
                                float v) noexcept {
    RU_GET_TYPE_BY_NAME()

    uint64_t hash;
    switch (type->get_type()) {
        case cpp2::SupportedType::FLOAT:
            hash = SpookyHashV2::Hash64(name.begin(), name.size(), 0);
            updatedFields_[hash] = v;
            break;
        case cpp2::SupportedType::DOUBLE:
            hash = SpookyHashV2::Hash64(name.begin(), name.size(), 0);
            updatedFields_[hash] = (double)v;
            break;
        default:
            return ResultType::E_INCOMPATIBLE_TYPE;
    }

    return ResultType::SUCCEEDED;
}


ResultType RowUpdater::setDouble(const folly::StringPiece name,
                                 double v) noexcept {
    RU_GET_TYPE_BY_NAME()

    uint64_t hash;
    switch (type->get_type()) {
        case cpp2::SupportedType::FLOAT:
            hash = SpookyHashV2::Hash64(name.begin(), name.size(), 0);
            updatedFields_[hash] = (float)v;
            break;
        case cpp2::SupportedType::DOUBLE:
            hash = SpookyHashV2::Hash64(name.begin(), name.size(), 0);
            updatedFields_[hash] = v;
            break;
        default:
            return ResultType::E_INCOMPATIBLE_TYPE;
    }

    return ResultType::SUCCEEDED;
}


ResultType RowUpdater::setString(const folly::StringPiece name,
                                 folly::StringPiece v) noexcept {
    RU_GET_TYPE_BY_NAME()

    uint64_t hash;
    switch (type->get_type()) {
        case cpp2::SupportedType::STRING:
            hash = SpookyHashV2::Hash64(name.begin(), name.size(), 0);
            updatedFields_[hash] = std::move(v.toString());
            break;
        default:
            return ResultType::E_INCOMPATIBLE_TYPE;
    }

    return ResultType::SUCCEEDED;
}


ResultType RowUpdater::setVid(const folly::StringPiece name,
                              int64_t v) noexcept {
    RU_GET_TYPE_BY_NAME()

    uint64_t hash;
    switch (type->get_type()) {
        case cpp2::SupportedType::VID:
            hash = SpookyHashV2::Hash64(name.begin(), name.size(), 0);
            updatedFields_[hash] = v;
            break;
        default:
            return ResultType::E_INCOMPATIBLE_TYPE;
    }

    return ResultType::SUCCEEDED;
}


/***************************************************
 *
 * Field Accessors
 *
 **************************************************/
ResultType RowUpdater::getBool(const folly::StringPiece name,
                               bool& v) const noexcept {
    RU_CHECK_UPDATED_FIELDS(Bool)

    switch (it->second.which()) {
    case VALUE_TYPE_BOOL:
        v = boost::get<bool>(it->second);
        break;
    case VALUE_TYPE_INT:
        v = intToBool(boost::get<int64_t>(it->second));
        break;
    case VALUE_TYPE_STRING:
        v = strToBool(boost::get<std::string>(it->second));
        break;
    default:
        return ResultType::E_INCOMPATIBLE_TYPE;
    }

    return ResultType::SUCCEEDED;
}


ResultType RowUpdater::getFloat(const folly::StringPiece name,
                                float& v) const noexcept {
    RU_CHECK_UPDATED_FIELDS(Float)

    switch (it->second.which()) {
    case VALUE_TYPE_FLOAT:
        v = boost::get<float>(it->second);
        break;
    case VALUE_TYPE_DOUBLE:
        v = boost::get<double>(it->second);
        break;
    default:
        return ResultType::E_INCOMPATIBLE_TYPE;
    }

    return ResultType::SUCCEEDED;
}


ResultType RowUpdater::getDouble(const folly::StringPiece name,
                                 double& v) const noexcept {
    RU_CHECK_UPDATED_FIELDS(Double)

    switch (it->second.which()) {
    case VALUE_TYPE_FLOAT:
        v = boost::get<float>(it->second);
        break;
    case VALUE_TYPE_DOUBLE:
        v = boost::get<double>(it->second);
        break;
    default:
        return ResultType::E_INCOMPATIBLE_TYPE;
    }

    return ResultType::SUCCEEDED;
}


ResultType RowUpdater::getString(const folly::StringPiece name,
                                 folly::StringPiece& v) const noexcept {
    RU_CHECK_UPDATED_FIELDS(String)

    switch (it->second.which()) {
    case VALUE_TYPE_STRING:
        v = boost::get<std::string>(it->second);
        break;
    default:
        return ResultType::E_INCOMPATIBLE_TYPE;
    }

    return ResultType::SUCCEEDED;
}


ResultType RowUpdater::getVid(const folly::StringPiece name,
                              int64_t& v) const noexcept {
    RU_CHECK_UPDATED_FIELDS(Vid)

    switch (it->second.which()) {
    case VALUE_TYPE_INT:
        v = boost::get<int64_t>(it->second);
        break;
    default:
        return ResultType::E_INCOMPATIBLE_TYPE;
    }

    return ResultType::SUCCEEDED;
}

#undef CHECK_UPDATED_FIELDS

}  // namespace vgraph
}  // namespace vesoft

