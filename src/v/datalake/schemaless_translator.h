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

#include "bytes/iobuf.h"
#include "iceberg/datatypes.h"
#include "iceberg/values.h"

#include <cstdint>
#include <utility>

namespace datalake {

class record_translator {
public:
    virtual ~record_translator() = default;

    virtual iceberg::struct_value translate_event(
      iobuf key, iobuf value, int64_t timestamp, int64_t offset) const
      = 0;

    virtual iceberg::struct_type get_schema() const = 0;
};

class schemaless_translator : public record_translator {
public:
    iceberg::struct_value translate_event(
      iobuf key, iobuf value, int64_t timestamp, int64_t offset) const override;

    iceberg::struct_type get_schema() const override;
};
} // namespace datalake
