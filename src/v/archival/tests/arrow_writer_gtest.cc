#include "archival/protobuf_to_arrow.h"
#include "test_utils/test.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <gtest/gtest.h>

#include <stdexcept>

std::string test_schema = R"schema(
syntax = "proto2";
package datalake.proto;

message simple_message {
  optional string label = 1;
  optional int32 number = 3;
}

message empty_message {
}

message nested_message {
  message inner_message_t {
    optional string label = 1;
    repeated int32 indices = 2;
  }
  optional string words = 1;
  optional int32 pos_num = 2;
  optional int32 neg_num = 3;
  optional string nothing_here = 4;
  optional inner_message_t inner_message = 5;
  optional string high_id = 1010;
}
)schema";

std::string generate_message_generic(
  const std::string& message_type,
  const std::function<void(google::protobuf::Message*)>& populate_message) {
    google::protobuf::FileDescriptorProto file_descriptor_proto;
    google::protobuf::compiler::Parser parser;
    google::protobuf::io::ArrayInputStream proto_input_stream(
      test_schema.c_str(), test_schema.size());
    google::protobuf::io::Tokenizer tokenizer(&proto_input_stream, nullptr);

    if (!parser.Parse(&tokenizer, &file_descriptor_proto)) {
        exit(-1);
    }

    if (!file_descriptor_proto.has_name()) {
        file_descriptor_proto.set_name("proto_file");
    }

    // Build a descriptor pool
    google::protobuf::DescriptorPool pool;
    const google::protobuf::FileDescriptor* file_desc = pool.BuildFile(
      file_descriptor_proto);
    assert(file_desc != nullptr);

    // Get the message descriptor
    const google::protobuf::Descriptor* message_desc
      = file_desc->FindMessageTypeByName(message_type);
    assert(message_desc != nullptr);

    // Parse the actual message
    google::protobuf::DynamicMessageFactory factory;
    const google::protobuf::Message* prototype_msg = factory.GetPrototype(
      message_desc);
    assert(prototype_msg != nullptr);

    google::protobuf::Message* mutable_msg = prototype_msg->New();
    assert(mutable_msg != nullptr);
    populate_message(mutable_msg);

    std::string ret = mutable_msg->SerializeAsString();
    delete mutable_msg;
    return ret;
}

std::string generate_empty_message() {
    return generate_message_generic(
      "empty_message", [](google::protobuf::Message* message) {});
}

std::string generate_simple_message(const std::string& label, int32_t number) {
    return generate_message_generic(
      "simple_message", [&](google::protobuf::Message* message) {
          std::cerr << "*** Populating message of type "
                    << message->GetTypeName() << " with "
                    << message->GetDescriptor()->field_count() << " fields"
                    << std::endl;

          auto reflection = message->GetReflection();
          // Have to use field indices here because
          // message->GetReflections()->ListFields() only returns fields that
          // are actually present in the message;
          for (int field_idx = 0;
               field_idx < message->GetDescriptor()->field_count();
               field_idx++) {
              auto field_desc = message->GetDescriptor()->field(field_idx);
              std::cerr << "Filling in field " << field_desc->name()
                        << std::endl;
              if (field_desc->name() == "label") {
                  reflection->SetString(message, field_desc, label);
              } else if (field_desc->name() == "number") {
                  reflection->SetInt32(message, field_desc, number);
              }
          }
      });
}

TEST(ArrowWriter, EmptyMessageTest) {
    using namespace datalake;
    std::string serialized_message = generate_empty_message();

    proto_to_arrow_converter converter(test_schema, "empty_message");
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

    proto_to_arrow_converter converter(test_schema, "simple_message");
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
