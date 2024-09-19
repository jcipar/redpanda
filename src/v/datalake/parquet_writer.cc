/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/parquet_writer.h"

#include "bytes/iobuf.h"
#include "datalake/data_writer_interface.h"

#include <arrow/array/array_base.h>
#include <arrow/chunked_array.h>
#include <arrow/io/file.h>
#include <arrow/io/memory.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>

#include <iostream>
#include <memory>
#include <stdexcept>

namespace datalake {
class iobuf_output_stream : public arrow::io::OutputStream {
public:
    iobuf_output_stream() = default;

    ~iobuf_output_stream() override = default;

    //// OUTPUT STREAM METHODS ////

    // Close the stream cleanly.
    arrow::Status Close() override;

    // Return the position in this stream
    arrow::Result<int64_t> Tell() const override;

    // Return whether the stream is closed
    bool closed() const override;

    arrow::Status Write(const void* data, int64_t nbytes) override;

    // TODO: implement this to avoid copying data multiple times
    // virtual Status Write(const std::shared_ptr<Buffer>& data);

    //// METHODS SPECIFIC TO IOBUF OUTPUT STREAM ////
    iobuf take_iobuf();

    size_t current_size_bytes();

private:
    iobuf _current_iobuf;
    bool _closed = false;
    int64_t _position = 0;
};

arrow_to_iobuf::arrow_to_iobuf(std::shared_ptr<arrow::Schema> schema)
  : _schema(schema) // Hold a pointer to schema so it isn't deallocated
{
    // TODO: make the compression algorithm configurable.
    std::shared_ptr<parquet::WriterProperties> writer_props
      = parquet::WriterProperties::Builder()
          .compression(arrow::Compression::SNAPPY)
          ->build();

    // Opt to store Arrow schema for easier reads back into Arrow
    std::shared_ptr<parquet::ArrowWriterProperties> arrow_props
      = parquet::ArrowWriterProperties::Builder().store_schema()->build();

    _outfile = std::make_shared<iobuf_output_stream>();

    auto writer_result = parquet::arrow::FileWriter::Open(
      *_schema,
      arrow::default_memory_pool(),
      _outfile,
      writer_props,
      arrow_props);
    if (!writer_result.ok()) {
        throw std::runtime_error(fmt::format(
          "Failed to create Arrow writer: {}", writer_result.status()));
    }
    _writer = std::move(writer_result.ValueUnsafe());
}

void arrow_to_iobuf::add_arrow_array(std::shared_ptr<arrow::Array> data) {
    if (data->length() == 0) {
        return;
    }
    arrow::ArrayVector data_av = {data};
    std::shared_ptr<arrow::ChunkedArray> chunked_data
      = std::make_shared<arrow::ChunkedArray>(std::move(data_av));
    auto table_result = arrow::Table::FromChunkedStructArray(chunked_data);
    if (!table_result.ok()) {
        throw std::runtime_error(fmt::format(
          "Failed to create arrow table: {}", table_result.status()));
    }
    auto table = table_result.ValueUnsafe();
    auto write_result = _writer->WriteTable(*table);
    if (!write_result.ok()) {
        throw std::runtime_error(fmt::format(
          "Failed to write arrow table: {}", write_result.ToString()));
    }
}

iobuf arrow_to_iobuf::take_iobuf() { return _outfile->take_iobuf(); }

iobuf arrow_to_iobuf::close_and_take_iobuf() {
    auto status = _writer->Close();
    if (!status.ok()) {
        throw std::runtime_error(
          fmt::format("Failed to close FileWriter: {}", status.ToString()));
    }
    return take_iobuf();
}

size_t arrow_to_iobuf::current_size_bytes() {
    return _outfile->current_size_bytes();
}

arrow::Status iobuf_output_stream::Close() {
    _closed = true;
    return arrow::Status::OK();
}

arrow::Result<int64_t> iobuf_output_stream::Tell() const { return _position; };

bool iobuf_output_stream::closed() const { return _closed; };

arrow::Status iobuf_output_stream::Write(const void* data, int64_t nbytes) {
    _current_iobuf.append(reinterpret_cast<const uint8_t*>(data), nbytes);
    _position += nbytes;

    return arrow::Status::OK();
}
iobuf iobuf_output_stream::take_iobuf() { return std::move(_current_iobuf); }

size_t iobuf_output_stream::current_size_bytes() {
    return _current_iobuf.size_bytes();
}

} // namespace datalake
