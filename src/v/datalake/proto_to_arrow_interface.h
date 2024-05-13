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

#include <arrow/api.h>
#include <arrow/array/array_base.h>
#include <arrow/type_fwd.h>

#include <memory>
#include <stdexcept>

namespace google::protobuf {
class Message;
}

namespace datalake::detail {

// This interface is used to convert a set of Protobuf messages to an Arrow
// Array. Proto messages are passed one-by-one along to this interface, and it
// builds internal state representing the Arrow array. Finally, the `finish`
// method returns the completed array.
class proto_to_arrow_interface {
public:
    proto_to_arrow_interface(const proto_to_arrow_interface&) = delete;
    proto_to_arrow_interface(proto_to_arrow_interface&&) = delete;
    proto_to_arrow_interface& operator=(const proto_to_arrow_interface&)
      = delete;
    proto_to_arrow_interface& operator=(proto_to_arrow_interface&&) = delete;

    proto_to_arrow_interface() = default;
    virtual ~proto_to_arrow_interface() = default;

    // Called on a struct message to parse an individual child field.
    // We expect the given child field to match the type of the column
    // represented by this converter. E.g. a proto_to_arrow_scalar<int32> would
    // expect the column referred to by field_idx to be an int32 column.
    virtual arrow::Status
    add_child_value(const google::protobuf::Message*, int field_idx)
      = 0;

    /// Return an Arrow field descriptor for this Array. Used for building
    /// A schema.
    // The Arrow API is built around shared_ptr: the creation functions return
    // shared pointers, and other expect them as arguments.
    // TODO: investigate if we can change the shared_ptr type in Arrow to use
    // ss::shared_ptr
    virtual std::shared_ptr<arrow::Field> field(const std::string& name) = 0;

    /// Return the underlying ArrayBuilder. Used when this is a child of another
    /// Builder
    virtual std::shared_ptr<arrow::ArrayBuilder> builder() = 0;

    // Methods with defaults
    virtual arrow::Status finish_batch() { return arrow::Status::OK(); }
    std::shared_ptr<arrow::ChunkedArray> finish() {
        return std::make_shared<arrow::ChunkedArray>(std::move(_values));
    }

protected:
    arrow::Status _arrow_status;
    arrow::ArrayVector _values;
};

} // namespace datalake::detail
