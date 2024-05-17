#include <datalake/schemaless_arrow_converter.h>

#include <string>

datalake::schemaless_arrow_converter::schemaless_arrow_converter() {
    _field_key = arrow::field("Key", arrow::binary());
    _field_value = arrow::field("Value", arrow::binary());

    // TODO: use the timestamp type? Iceberg does not support unsigned
    // integers.
    _field_timestamp = arrow::field("Timestamp", arrow::uint64());

    _schema = arrow::schema({_field_key, _field_value, _field_timestamp});
}

datalake::arrow_converter_status
datalake::schemaless_arrow_converter::add_message(
  const std::string& serialized_message) {
    if (!_ok.ok()) {
        return datalake::arrow_converter_status::internal_error;
    }

    _ok = _key_builder.Append(std::string(""));
    if (!_ok.ok()) {
        return datalake::arrow_converter_status::internal_error;
    }

    _ok = _value_builder.Append(serialized_message);
    if (!_ok.ok()) {
        return datalake::arrow_converter_status::internal_error;
    }

    _ok = _timestamp_builder.Append(0);
    if (!_ok.ok()) {
        return datalake::arrow_converter_status::internal_error;
    }

    return arrow_converter_status::ok;
}

datalake::arrow_converter_status
datalake::schemaless_arrow_converter::finish_batch() {
    if (!_ok.ok()) {
        return datalake::arrow_converter_status::internal_error;
    }

    std::shared_ptr<arrow::Array> key_array;
    std::shared_ptr<arrow::Array> value_array;
    std::shared_ptr<arrow::Array> timestamp_array;

    auto&& key_builder_result = _key_builder.Finish();
    _ok = key_builder_result.status();
    if (!_ok.ok()) {
        return datalake::arrow_converter_status::internal_error;
    } else {
        key_array = std::move(key_builder_result).ValueUnsafe();
    }

    auto&& value_builder_result = _value_builder.Finish();
    _ok = value_builder_result.status();
    if (!_ok.ok()) {
        return datalake::arrow_converter_status::internal_error;
    } else {
        value_array = std::move(value_builder_result).ValueUnsafe();
    }

    auto&& timestamp_builder_result = _timestamp_builder.Finish();
    _ok = timestamp_builder_result.status();
    if (!_ok.ok()) {
        return datalake::arrow_converter_status::internal_error;
    } else {
        timestamp_array = std::move(timestamp_builder_result).ValueUnsafe();
    }

    _key_vector.push_back(key_array);
    _value_vector.push_back(value_array);
    _timestamp_vector.push_back(timestamp_array);

    return arrow_converter_status::ok;
}

std::shared_ptr<arrow::Table>
datalake::schemaless_arrow_converter::build_table() {
    if (!_ok.ok()) {
        return nullptr;
    }
    // Create a ChunkedArray
    std::shared_ptr<arrow::ChunkedArray> key_chunks
      = std::make_shared<arrow::ChunkedArray>(_key_vector);
    std::shared_ptr<arrow::ChunkedArray> value_chunks
      = std::make_shared<arrow::ChunkedArray>(_value_vector);
    std::shared_ptr<arrow::ChunkedArray> timestamp_chunks
      = std::make_shared<arrow::ChunkedArray>(_timestamp_vector);

    // Create a table
    return arrow::Table::Make(
      _schema,
      {key_chunks, value_chunks, timestamp_chunks},
      key_chunks->length());
}

std::shared_ptr<arrow::Schema>
datalake::schemaless_arrow_converter::build_schema() {
    return _schema;
}
