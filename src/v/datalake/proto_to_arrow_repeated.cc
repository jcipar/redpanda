/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/proto_to_arrow_repeated.h"

#include "datalake/logger.h"
#include "datalake/proto_to_arrow_struct.h"

#include <arrow/array/builder_base.h>
#include <arrow/array/builder_nested.h>
#include <arrow/scalar.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <google/protobuf/message.h>

#include <memory>

namespace datalake::detail {
proto_to_arrow_repeated::proto_to_arrow_repeated(
  const google::protobuf::FieldDescriptor* desc) {
    // The documentation for ListBuilder is not very detailed. From the linked
    // StackOverflow and some trial-and-error, it looks like it's used like
    // this:
    // 1. A ListBuilder is created with a shared pointer to another
    // arrow::ArrayBuilder as a child.
    // 2. *Before* adding elements to the child, call Append() on the
    // ListBuilder.
    // 3. Call Append(val) on the child builder in the normal way.
    // 4. *Do not* call Finish() onthe child builder.
    // 5. Call Finish() on the ListBuilder to get the array.
    //
    // https://stackoverflow.com/questions/78277111/is-there-a-way-to-nest-an-arrowarray-in-apache-arrow
    _child_converter = make_converter(desc, true);
    _builder = std::make_shared<arrow::ListBuilder>(
      arrow::default_memory_pool(), _child_converter->builder());
}

arrow::Status proto_to_arrow_repeated::add_child_value(
  const google::protobuf::Message* msg, int field_idx) {
    if (!_arrow_status.ok()) {
        return _arrow_status;
    }
    _arrow_status = _builder->Append();
    if (!_arrow_status.ok()) {
        return _arrow_status;
    }
    _arrow_status = _child_converter->add_child_value(msg, field_idx);

    return _arrow_status;
}

std::shared_ptr<arrow::Field>
proto_to_arrow_repeated::field(const std::string& name) {
    // FIXME: type
    std::shared_ptr<arrow::DataType> arrow_type = arrow::list(arrow::int32());
    return arrow::field(name, arrow_type);
};

std::shared_ptr<arrow::ArrayBuilder> proto_to_arrow_repeated::builder() {
    return _builder;
};

arrow::Status proto_to_arrow_repeated::finish_batch() {
    std::cerr << "finish_batch with " << _builder->length() << " items\n";

    if (!_arrow_status.ok()) {
        return _arrow_status;
    }
    auto builder_result = _builder->Finish();
    _arrow_status = builder_result.status();
    std::shared_ptr<arrow::Array> array;
    if (!_arrow_status.ok()) {
        return _arrow_status;
    }

    // Safe because we validated the status after calling `Finish`
    array = std::move(builder_result).ValueUnsafe();
    std::cerr << "adding array with " << array->length()
              << " elements to _values\n";
    _values.push_back(array);
    return _arrow_status;
}
} // namespace datalake::detail
