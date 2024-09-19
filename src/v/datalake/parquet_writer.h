/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "datalake/data_writer_interface.h"

#include <arrow/io/file.h>
#include <arrow/io/memory.h>
#include <arrow/type.h>
#include <parquet/arrow/writer.h>

#include <filesystem>
#include <utility>

namespace arrow {
class Array;
}

namespace datalake {

class iobuf_output_stream;

class arrow_to_iobuf {
public:
    explicit arrow_to_iobuf(std::shared_ptr<arrow::Schema> schema);

    void add_arrow_array(std::shared_ptr<arrow::Array> data);
    iobuf take_iobuf();
    iobuf close_and_take_iobuf();
    size_t current_size_bytes();

private:
    std::shared_ptr<arrow::Schema> _schema;
    std::shared_ptr<iobuf_output_stream> _outfile;
    std::unique_ptr<parquet::arrow::FileWriter> _writer;
};

} // namespace datalake
