#pragma once

// #include "bytes/iobuf.h"
// #include "bytes/iostream.h"
#include "cloud_storage/types.h"
#include "pandaproxy/schema_registry/types.h"
// #include "datalake/protobuf_to_arrow.h"
// #include "datalake/schema_registry_interface.h"
// #include "model/fundamental.h"
// #include "model/record.h"

#include "wasm/schema_registry.h"

#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/util/file.hh>

#include <string>

namespace datalake {

struct schema_info {
    std::string key_schema;
    std::string key_message_name;
};

class schema_registry_interface {
public:
    virtual ~schema_registry_interface() = default;
    virtual ss::future<schema_info> get_raw_topic_schema(std::string topic) = 0;
};

class dummy_schema_registry : public schema_registry_interface {
public:
    ss::future<schema_info>
    get_raw_topic_schema(std::string /* topic */) override {
        co_return schema_info{
          .key_schema = R"schema(
          syntax = "proto2";
          package datalake.proto;

          message simple_message {
            optional string label = 1;
            optional int32 number = 3;
          }

          message value_message {
            optional string Topic = 1;
            optional string Sentiment = 2;
            optional uint64 TweetId = 3;
            optional string TweetText = 4;
            optional string TweetDate = 5;
          }
          )schema",
          // .key_schema = value_schema_string,
          .key_message_name = "value_message",
        };
        // co_return R"schema(
        //   syntax = "proto2";
        //   package datalake.proto;

        //   message simple_message {
        //     optional string label = 1;
        //     optional int32 number = 3;
        //   }

        //   message value_message {
        //     optional string Topic = 1;
        //     optional string Sentiment = 2;
        //     optional uint64 TweetId = 3;
        //     optional string TweetText = 4;
        //     optional string TweetDate = 5;
        //   }
        //   )schema";
    }
};

class schema_registry_reader : public schema_registry_interface {
public:
    explicit schema_registry_reader() {}

    void set_schema_registry(
      pandaproxy::schema_registry::api* pandaproxy_schema_registry) {
        _pandaproxy_schema_registry = pandaproxy_schema_registry;
        if (!pandaproxy_schema_registry) {
            std::cerr << "jcipar initializing with null ppsr" << std::endl;
        } else {
            std::cerr << "jcipar initializing schema registry reader"
                      << std::endl;
        }
        _schema_registry = wasm::schema_registry::make_default(
          pandaproxy_schema_registry);
        _initialized = true;
    }

    ss::future<schema_info>
    get_raw_topic_schema(std::string topic_name) override {
        schema_info default_return_value = {
          .key_schema = R"schema(
          syntax = "proto2";
          package datalake.proto;

          message simple_message {
            optional string label = 1;
            optional int32 number = 3;
          }

          message twitter_record {
            optional string Topic = 1;
            optional string Sentiment = 2;
            optional uint64 TweetId = 3;
            optional string TweetText = 4;
            optional string TweetDate = 5;
          }
          )schema",
          // .key_schema = value_schema_string,
          .key_message_name = "twitter_record",
        };

        if (!_initialized) {
            std::cerr << "jcipar returning empty schema" << std::endl;
            co_return default_return_value;
        }
        // auto schema_registry = wasm::schema_registry::make_default(
        //   _pandaproxy_schema_registry);
        pandaproxy::schema_registry::subject value_subject{
          std::string(topic_name) + "-value"};
        std::cerr << "jcipar getting schema for subject " << value_subject
                  << std::endl;

        try {
            auto value_schema = co_await _schema_registry->get_subject_schema(
              value_subject, std::nullopt);

            std::cerr << "jcipar got subject schema " << std::endl;

            if (
              value_schema.schema.type()
              != pandaproxy::schema_registry::schema_type::protobuf) {
                co_return default_return_value;
            }

            std::cerr << "jcipar it's a protobuf schema\n";

            std::string value_schema_string = value_schema.schema.def().raw()();

            std::cerr << "jcipar raw value schema: " << value_schema_string
                      << std::endl;

            std::cerr << "jcipar got schema value from registry:\n"
                      << value_schema_string << std::endl;
            co_return schema_info{
              .key_schema = value_schema_string,
              .key_message_name = "twitter_record",
            };
        } catch (...) {
            co_return default_return_value;
        }
    }

    bool _initialized = false;
    std::unique_ptr<wasm::schema_registry> _schema_registry;
    pandaproxy::schema_registry::api* _pandaproxy_schema_registry;
};

} // namespace datalake
