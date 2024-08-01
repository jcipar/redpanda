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

#include "datalake/errors.h"
#include "datalake/proto_to_arrow_interface.h"
#include "datalake/proto_to_arrow_struct.h"

#include <arrow/array/builder_base.h>
#include <arrow/array/builder_nested.h>
#include <arrow/status.h>
#include <arrow/type_fwd.h>
#include <fmt/format.h>
#include <google/protobuf/descriptor.h>

#include <memory>

namespace datalake::detail {

class proto_to_arrow_map : public proto_to_arrow_interface {
public:
    proto_to_arrow_map(const google::protobuf::FieldDescriptor* field_desc) {
        auto msg_desc = field_desc->message_type();
        if (msg_desc->field_count() != 2) {
            throw datalake::initialization_error(fmt::format(
              "Initializing map with {} fields", msg_desc->field_count()));
        }
        std::cerr << "Initializing map converter!\n";

        _key_converter = make_converter(msg_desc->field(0));
        _value_converter = make_converter(msg_desc->field(1));
        _builder = std::make_shared<arrow::MapBuilder>(
          arrow::default_memory_pool(),
          _key_converter->builder(),
          _value_converter->builder());
    }

    arrow::Status add_child_value(
      const google::protobuf::Message* msg, int field_idx) override {
        if (!_arrow_status.ok()) {
            return _arrow_status;
        }
        std::cerr << "proto_to_arrow_map got message: " << msg->DebugString()
                  << std::endl;

        const auto* field_desc = msg->GetDescriptor()->field(field_idx);
        if (!field_desc->is_repeated()) {
            // TODO: error
        }
        _arrow_status = _builder->Append();
        if (!_arrow_status.ok()) {
            return _arrow_status;
        }
        for (int i = 0; i < msg->GetReflection()->FieldSize(*msg, field_desc);
             i++) {
            auto child_msg = &msg->GetReflection()->GetRepeatedMessage(
              *msg, field_desc, i);
            std::cerr << "Child " << i << ": " << child_msg->DebugString()
                      << std::endl;
            _arrow_status = _key_converter->add_child_value(child_msg, 0);
            if (!_arrow_status.ok()) {
                return _arrow_status;
            }
            _arrow_status = _value_converter->add_child_value(child_msg, 1);
            if (!_arrow_status.ok()) {
                return _arrow_status;
            }
        }

        return _arrow_status;
    }

    std::shared_ptr<arrow::Field> field(const std::string& name) override {
        const auto type = arrow::map(
          _key_converter->field("key")->type(),
          _value_converter->field("value")->type());
        return arrow::field(name, type);
    }

    std::shared_ptr<arrow::ArrayBuilder> builder() override { return _builder; }

    arrow::Status finish_batch() override {
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

private:
    std::shared_ptr<arrow::MapBuilder> _builder;
    std::unique_ptr<proto_to_arrow_interface> _key_converter;
    std::unique_ptr<proto_to_arrow_interface> _value_converter;
};

} // namespace datalake::detail
