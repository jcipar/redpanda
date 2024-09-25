/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/schemaless_translator.h"
#include "iceberg/datatypes.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/partition_key.h"
#include "iceberg/values.h"

#include <cstddef>
#include <memory>

#pragma once

namespace datalake {
struct data_writer_file {
    ss::sstring file_path;
    size_t record_count = 0;
    size_t file_size_bytes = 0;
};

class data_writer {
public:
    virtual ~data_writer() = default;

    // TODO: error type?
    virtual bool add_data_struct(
      iceberg::struct_value /* data */, int64_t /* approx_size */)
      = 0;

    virtual data_writer_file finish() = 0;
};

class data_writer_factory {
public:
    virtual ~data_writer_factory() = default;

    virtual std::unique_ptr<data_writer>
      create_writer(iceberg::struct_type /* schema */) = 0;
};
} // namespace datalake
