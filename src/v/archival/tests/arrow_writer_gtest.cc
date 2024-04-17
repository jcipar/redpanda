#include "archival/protobuf_to_arrow.h"
#include "archival/tests/arrow_writer_test_utils.h"
#include "test_utils/test.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <gtest/gtest.h>

#include <stdexcept>

TEST(ArrowWriter, EmptyMessageTest) {
    using namespace datalake;
    test_data test_data;
    std::string schema = test_data.schema;
    std::string message_name = "empty_message";
    std::cerr << "Testing with " << message_name << " from schema: \n"
              << schema << std::endl;

    proto_to_arrow_converter converter(schema, message_name);

    std::string serialized_message = generate_empty_message();
    std::cerr << "Created serialized message \"" << serialized_message << "\""
              << std::endl;

    auto parsed_message = converter.parse_message(serialized_message);
    EXPECT_NE(parsed_message, nullptr);
    EXPECT_EQ(parsed_message->GetTypeName(), "datalake.proto.empty_message");

    // google::protobuf::ShutdownProtobufLibrary();
}

TEST(ArrowWriter, SimpleMessageTest) {
    using namespace datalake;
    std::string serialized_message = generate_simple_message(
      "Hello world", 12345);
    std::cerr << "*** Serialized message \"" << serialized_message << "\"\n";

    test_data test_data;
    proto_to_arrow_converter converter(
      test_data.schema, test_data.test_message_name);
    EXPECT_EQ(converter._arrays.size(), 2);
    auto parsed_message = converter.parse_message(serialized_message);
    EXPECT_NE(parsed_message, nullptr);
    EXPECT_EQ(parsed_message->GetTypeName(), "datalake.proto.simple_message");

    converter.add_message(serialized_message);
    {
        converter.add_message(generate_simple_message("I", 1));
        converter.add_message(generate_simple_message("II", 2));
        converter.add_message(generate_simple_message("III", 3));
        converter.add_message(generate_simple_message("IV", 4));
        converter.add_message(generate_simple_message("V", 5));
    }
    converter.finish_batch();
    // std::cout << converter._arrays[0]->finish()->ToString();
    // std::cout << converter._arrays[1]->finish()->ToString();
    std::cout << "Schema: \n"
              << converter.build_schema()->ToString() << std::endl
              << std::endl;

    std::cout << "Table: \n"
              << converter.build_table()->ToString() << std::endl;
    // std::cout << converter._arrays[3]->finish();

    // EXPECT_FALSE(true);
    // google::protobuf::ShutdownProtobufLibrary();
}

TEST(ArrowWriter, NestedMessageTest) {
    using namespace datalake;
    std::string serialized_message = generate_nested_message(
      "Hello world", 12345);
    std::cerr << "*** Serialized message \"" << serialized_message << "\"\n";

    test_data test_data;
    proto_to_arrow_converter converter(test_data.schema, "nested_message");
    EXPECT_EQ(converter._arrays.size(), 3);
    auto parsed_message = converter.parse_message(serialized_message);
    EXPECT_NE(parsed_message, nullptr);
    EXPECT_EQ(parsed_message->GetTypeName(), "datalake.proto.nested_message");

    converter.add_message(serialized_message);
    {
        converter.add_message(generate_nested_message("I", 1));
        converter.add_message(generate_nested_message("II", 2));
        converter.add_message(generate_nested_message("III", 3));
        converter.add_message(generate_nested_message("IV", 4));
        converter.add_message(generate_nested_message("V", 5));
    }
    converter.finish_batch();
    // std::cout << converter._arrays[0]->finish()->ToString();
    // std::cout << converter._arrays[1]->finish()->ToString();
    std::cout << "Schema: \n"
              << converter.build_schema()->ToString() << std::endl
              << std::endl;

    std::cout << "Table: \n"
              << converter.build_table()->ToString() << std::endl;
    // std::cout << converter._arrays[3]->finish();

    // EXPECT_FALSE(true);
    // google::protobuf::ShutdownProtobufLibrary();
}
