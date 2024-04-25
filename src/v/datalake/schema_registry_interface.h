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
          .key_message_name = "value_message",
        };
    }
};

class schema_registry_reader : public schema_registry_interface {
public:
    explicit schema_registry_reader() {}

    void set_schema_registry(
      pandaproxy::schema_registry::api* pandaproxy_schema_registry) {
        _pandaproxy_schema_registry = pandaproxy_schema_registry;
        _schema_registry = wasm::schema_registry::make_default(
          pandaproxy_schema_registry);
        _initialized = true;
    }

    ss::future<schema_info>
    get_raw_topic_schema(std::string topic_name) override {
        schema_info default_return_value = {
          .key_schema = "",
          .key_message_name = "",
        };

        if (!_initialized) {
            co_return default_return_value;
        }

        pandaproxy::schema_registry::subject value_subject{
          std::string(topic_name) + "-value"};

        try {
            auto value_schema = co_await _schema_registry->get_subject_schema(
              value_subject, std::nullopt);

            if (
              value_schema.schema.type()
              != pandaproxy::schema_registry::schema_type::protobuf) {
                co_return default_return_value;
            }
            std::string value_schema_string = value_schema.schema.def().raw()();

            co_return schema_info{
              .key_schema = value_schema_string,
              .key_message_name = "",
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
