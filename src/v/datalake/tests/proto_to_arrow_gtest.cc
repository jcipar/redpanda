#include "datalake/errors.h"
#include "datalake/protobuf_to_arrow_converter.h"
#include "datalake/schemaless_arrow_converter.h"
#include "datalake/tests/proto_to_arrow_test_utils.h"
#include "test_utils/test.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <gtest/gtest.h>

#include <stdexcept>
#include <string>

namespace datalake {
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
}

TEST(ArrowWriter, SchemalessTest) {
    using namespace datalake;
    std::string serialized_message = generate_simple_message(
      "Hello world", 12345);

    test_data test_data;
    schemaless_arrow_converter converter;

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

    ASSERT_NE(schema, nullptr);
    ASSERT_NE(table, nullptr);

    EXPECT_EQ(
      schema->field_names(),
      std::vector<std::string>({"Key", "Value", "Timestamp"}));
    std::vector<std::string> table_field_names;
    for (const auto& field : table->fields()) {
        table_field_names.push_back(field->name());
    }
    EXPECT_EQ(table_field_names, schema->field_names());
}

} // namespace datalake
