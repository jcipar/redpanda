#pragma once

#include "datalake/arrow_converter_interface.h"
#include "datalake/errors.h"
#include "datalake/proto_to_arrow_interface.h"
#include "datalake/proto_to_arrow_scalar.h"
#include "datalake/proto_to_arrow_struct.h"

#include <arrow/api.h>

namespace datalake {
class schemaless_arrow_converter : public arrow_converter_interface {
public:
    schemaless_arrow_converter();

    [[nodiscard]] arrow_converter_status
    add_message(const std::string& serialized_message) override;

    [[nodiscard]] arrow_converter_status finish_batch() override;
    std::shared_ptr<arrow::Table> build_table() override;
    std::shared_ptr<arrow::Schema> build_schema() override;

private:
    arrow::Status _ok = arrow::Status::OK();
    std::shared_ptr<arrow::Field> _field_key, _field_value, _field_timestamp;
    std::shared_ptr<arrow::Schema> _schema;

    // per-batch data structures
    arrow::BinaryBuilder _key_builder;
    arrow::BinaryBuilder _value_builder;
    arrow::UInt64Builder _timestamp_builder;

    //
    arrow::ArrayVector _key_vector;
    arrow::ArrayVector _value_vector;
    arrow::ArrayVector _timestamp_vector;
};

}; // namespace datalake
