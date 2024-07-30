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

#include "datalake/proto_to_arrow_interface.h"

#include <arrow/array/builder_base.h>
#include <arrow/array/builder_nested.h>
#include <arrow/status.h>
#include <google/protobuf/descriptor.h>

#include <memory>

namespace datalake::detail {

class proto_to_arrow_repeated : public proto_to_arrow_interface {
public:
    proto_to_arrow_repeated(const google::protobuf::FieldDescriptor*);
    arrow::Status
    add_child_value(const google::protobuf::Message*, int) override;

    std::shared_ptr<arrow::Field> field(const std::string&) override;

    std::shared_ptr<arrow::ArrayBuilder> builder() override;

    arrow::Status finish_batch() override;

private:
    std::shared_ptr<arrow::ListBuilder> _builder;
    std::unique_ptr<proto_to_arrow_interface> _child_converter;
};

} // namespace datalake::detail
