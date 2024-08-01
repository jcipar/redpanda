/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/errors.h"
#include "datalake/protobuf_to_arrow_converter.h"
#include "datalake/tests/proto_to_arrow_test_utils.h"
#include "test_utils/test.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <gtest/gtest.h>

#include <stdexcept>
#include <string>

namespace datalake {
TEST(ArrowWriter, InvalidMessagetest) {
    using namespace datalake;
    test_data test_data;
    std::string schema = test_data.empty_schema;

    proto_to_arrow_converter converter(schema);

    std::string serialized_message{"This is not a Protobuf message!"};

    auto parsed_message = converter.parse_message(serialized_message);
    EXPECT_EQ(parsed_message, nullptr);
}

TEST(ArrowWriter, EmptyMessageTest) {
    using namespace datalake;
    test_data test_data;
    std::string schema = test_data.empty_schema;

    proto_to_arrow_converter converter(schema);

    std::string serialized_message = generate_empty_message();

    auto parsed_message = converter.parse_message(serialized_message);
    EXPECT_NE(parsed_message, nullptr);
    EXPECT_EQ(parsed_message->GetTypeName(), "datalake.proto.empty_message");
}

TEST(ArrowWriter, SimpleMessageTest) {
    using namespace datalake;
    std::string serialized_message = generate_simple_message(
      "Hello world", 12345);

    test_data test_data;
    proto_to_arrow_converter converter(test_data.simple_schema);
    auto parsed_message = converter.parse_message(serialized_message);
    EXPECT_NE(parsed_message, nullptr);
    EXPECT_EQ(parsed_message->GetTypeName(), "datalake.proto.simple_message");

    EXPECT_EQ(
      arrow_converter_status::ok, converter.add_message(serialized_message));
    {
        EXPECT_EQ(
          arrow_converter_status::ok,
          converter.add_message(generate_simple_message("I", 1)));
        EXPECT_EQ(
          arrow_converter_status::ok,
          converter.add_message(generate_simple_message("II", 2)));
        EXPECT_EQ(
          arrow_converter_status::ok,
          converter.add_message(generate_simple_message("III", 3)));
        EXPECT_EQ(
          arrow_converter_status::ok,
          converter.add_message(generate_simple_message("IV", 4)));
        EXPECT_EQ(
          arrow_converter_status::ok,
          converter.add_message(generate_simple_message("V", 5)));
    }
    EXPECT_EQ(arrow_converter_status::ok, converter.finish_batch());

    auto schema = converter.build_schema();
    auto table = converter.build_table();
    EXPECT_EQ(
      schema->field_names(),
      std::vector<std::string>(
        {"label",
         "number",
         "big_number",
         "float_number",
         "double_number",
         "true_or_false"}));
    std::vector<std::string> table_field_names;
    for (const auto& field : table->fields()) {
        table_field_names.push_back(field->name());
    }
    EXPECT_EQ(table_field_names, schema->field_names());
    std::vector<std::string> expected{
      "Hello world", "I", "II", "III", "IV", "V"};
    for (int i = 0; i < expected.size(); i++) {
        EXPECT_EQ(
          table->GetColumnByName("label")->GetScalar(i)->get()->ToString(),
          expected[i]);
    }

    expected = {"12345", "1", "2", "3", "4", "5"};
    for (int i = 0; i < expected.size(); i++) {
        EXPECT_EQ(
          table->GetColumnByName("number")->GetScalar(i)->get()->ToString(),
          expected[i]);
    }
    EXPECT_EQ(
      table->ToString(),
      "label: string\nnumber: int32\nbig_number: int64\nfloat_number: "
      "float\ndouble_number: double\ntrue_or_false: bool\n----\nlabel:\n  [\n  "
      "  [\n      \"Hello world\",\n      \"I\",\n      \"II\",\n      "
      "\"III\",\n      \"IV\",\n      \"V\"\n    ]\n  ]\nnumber:\n  [\n    [\n "
      "     12345,\n      1,\n      2,\n      3,\n      4,\n      5\n    ]\n  "
      "]\nbig_number:\n  [\n    [\n      123450,\n      10,\n      20,\n      "
      "30,\n      40,\n      50\n    ]\n  ]\nfloat_number:\n  [\n    [\n      "
      "1234.5,\n      0.1,\n      0.2,\n      0.3,\n      0.4,\n      0.5\n    "
      "]\n  ]\ndouble_number:\n  [\n    [\n      123.45,\n      0.01,\n      "
      "0.02,\n      0.03,\n      0.04,\n      0.05\n    ]\n  "
      "]\ntrue_or_false:\n  [\n    [\n      false,\n      false,\n      "
      "true,\n      false,\n      true,\n      false\n    ]\n  ]\n");
}

TEST(ArrowWriter, NestedMessageTest) {
    using namespace datalake;
    std::string serialized_message = generate_nested_message(
      "Hello world", 12345);

    test_data test_data;
    proto_to_arrow_converter converter(test_data.nested_schema);
    auto parsed_message = converter.parse_message(serialized_message);
    EXPECT_NE(parsed_message, nullptr);
    EXPECT_EQ(parsed_message->GetTypeName(), "datalake.proto.nested_message");

    EXPECT_EQ(
      arrow_converter_status::ok, converter.add_message(serialized_message));
    {
        EXPECT_EQ(
          arrow_converter_status::ok,
          converter.add_message(generate_nested_message("I", 1)));
        EXPECT_EQ(
          arrow_converter_status::ok,
          converter.add_message(generate_nested_message("II", 2)));
        EXPECT_EQ(
          arrow_converter_status::ok,
          converter.add_message(generate_nested_message("III", 3)));
        EXPECT_EQ(
          arrow_converter_status::ok,
          converter.add_message(generate_nested_message("IV", 4)));
        EXPECT_EQ(
          arrow_converter_status::ok,
          converter.add_message(generate_nested_message("V", 5)));
    }
    EXPECT_EQ(arrow_converter_status::ok, converter.finish_batch());

    auto schema = converter.build_schema();
    auto table = converter.build_table();

    EXPECT_EQ(
      schema->field_names(),
      std::vector<std::string>({"label", "number", "inner_message"}));
    std::vector<std::string> table_field_names;
    for (const auto& field : table->fields()) {
        table_field_names.push_back(field->name());
    }
    EXPECT_EQ(table_field_names, schema->field_names());
    EXPECT_EQ(
      table->ToString(),
      "label: string\nnumber: int32\ninner_message: struct<inner_label: "
      "string, inner_number: int32>\n  child 0, inner_label: string\n  child "
      "1, inner_number: int32\n----\nlabel:\n  [\n    [\n      \"Hello "
      "world\",\n      \"I\",\n      \"II\",\n      \"III\",\n      \"IV\",\n  "
      "    \"V\"\n    ]\n  ]\nnumber:\n  [\n    [\n      12345,\n      1,\n    "
      "  2,\n      3,\n      4,\n      5\n    ]\n  ]\ninner_message:\n  [\n    "
      "-- is_valid: all not null\n    -- child 0 type: string\n      [\n       "
      " \"inner: Hello world\",\n        \"inner: I\",\n        \"inner: "
      "II\",\n        \"inner: III\",\n        \"inner: IV\",\n        "
      "\"inner: V\"\n      ]\n    -- child 1 type: int32\n      [\n        "
      "-12345,\n        -1,\n        -2,\n        -3,\n        -4,\n        "
      "-5\n      ]\n  ]\n");
}

TEST(ArrowWriter, RepeatedFeildTest) {
    using namespace datalake;
    test_data test_data;
    std::string schema = test_data.repeated_schema;

    proto_to_arrow_converter converter(schema);

    std::string serialized_message = generate_repeated_field_message(
      {1, 1, 2, 3, 5, 8, 13});

    auto parsed_message = converter.parse_message(serialized_message);
    EXPECT_NE(parsed_message, nullptr);
    EXPECT_EQ(parsed_message->GetTypeName(), "datalake.proto.repeated_field");
    EXPECT_EQ(
      parsed_message->DebugString(),
      "numbers: 1\nnumbers: 1\nnumbers: 2\nnumbers: 3\nnumbers: 5\nnumbers: "
      "8\nnumbers: 13\n");

    EXPECT_EQ(
      arrow_converter_status::ok, converter.add_message(serialized_message));
    EXPECT_EQ(
      arrow_converter_status::ok,
      converter.add_message(generate_repeated_field_message({2, 4, 6, 8})));
    EXPECT_EQ(arrow_converter_status::ok, converter.finish_batch());

    auto table_schema = converter.build_schema();
    auto table = converter.build_table();
    EXPECT_EQ(
      table_schema->field_names(), std::vector<std::string>({"numbers"}));
    EXPECT_EQ(
      table->ToString(),
      "numbers: list<item: int32>\n  child 0, item: int32\n----\nnumbers:\n  "
      "[\n    [\n      [\n        1,\n        1,\n        2,\n        3,\n     "
      "   5,\n        8,\n        13\n      ],\n      [\n        2,\n        "
      "4,\n        6,\n        8\n      ]\n    ]\n  ]\n");
}

TEST(ArrowWriter, MapFieldTest) {
    using namespace datalake;
    test_data test_data;
    std::string schema = test_data.map_schema;

    proto_to_arrow_converter converter(schema);
    std::string serialized_message = generate_map_field_message({
      {"one", 1},
      {"two", 2},
      {"three", 3},
      {"five", 5},
      {"eight", 8},
    });

    std::cerr << "Map Message: \"" << serialized_message << "\"\n";
    auto parsed_message = converter.parse_message(serialized_message);

    EXPECT_NE(parsed_message, nullptr);
    EXPECT_EQ(parsed_message->GetTypeName(), "datalake.proto.map_field");
    EXPECT_EQ(
      parsed_message->DebugString(),
      "keys_values {\n  key: \"eight\"\n  value: 8\n}\nkeys_values {\n  key: "
      "\"five\"\n  value: 5\n}\nkeys_values {\n  key: \"one\"\n  value: "
      "1\n}\nkeys_values {\n  key: \"three\"\n  value: 3\n}\nkeys_values {\n  "
      "key: \"two\"\n  value: 2\n}\n");

    EXPECT_EQ(
      arrow_converter_status::ok, converter.add_message(serialized_message));
    EXPECT_EQ(arrow_converter_status::ok, converter.finish_batch());

    auto table_schema = converter.build_schema();
    auto table = converter.build_table();
    EXPECT_EQ(
      table_schema->field_names(), std::vector<std::string>({"keys_values"}));
    EXPECT_EQ(
      table->ToString(),
      "keys_values: map<string, int32>\n  child 0, entries: struct<key: string "
      "not null, value: int32> not null\n      child 0, key: string not null\n "
      "     child 1, value: int32\n----\nkeys_values:\n  [\n    [\n      "
      "keys:\n      [\n        \"eight\",\n        \"three\",\n        "
      "\"two\",\n        \"five\",\n        \"one\"\n      ]\n      values:\n  "
      "    [\n        8,\n        3,\n        2,\n        5,\n        1\n      "
      "]\n    ]\n  ]\n");
}

} // namespace datalake
