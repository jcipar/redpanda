#pragma once
#include <arrow/api.h>
#include <arrow/array/builder_base.h>
#include <arrow/array/builder_nested.h>
#include <arrow/chunked_array.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/type_traits.h>
#include <google/protobuf/arena.h>
#include <google/protobuf/compiler/parser.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/io/tokenizer.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/message.h>
#include <google/protobuf/stubs/common.h>
#include <google/protobuf/unknown_field_set.h>

#include <memory>
#include <stdexcept>

namespace datalake {

namespace proto_to_arrow_impl {
class proto_to_array {
public:
    virtual ~proto_to_array() {}

    // Pure virtual methods
    virtual arrow::Status
    add_value(const google::protobuf::Message*, int field_idx)
      = 0;

    /// Return an Arrow field descriptor for this Array. Used for building
    /// A schema.
    virtual std::shared_ptr<arrow::Field> field(const std::string& name) = 0;

    /// Return the underlying ArrayBuilder. Used when this is a child of another
    /// Builder
    virtual std::shared_ptr<arrow::ArrayBuilder> builder() = 0;

    // Methods with defaults
    virtual arrow::Status finish_batch() { return arrow::Status::OK(); }
    std::shared_ptr<arrow::ChunkedArray> finish() {
        return std::make_shared<arrow::ChunkedArray>(_values);
    }

protected:
    arrow::Status _arrow_status;
    arrow::ArrayVector _values;
};

template<typename ArrowType>
class proto_to_array_scalar : public proto_to_array {
    using BuilderType = arrow::TypeTraits<ArrowType>::BuilderType;

public:
    proto_to_array_scalar()
      : _builder(std::make_shared<BuilderType>()) {}

    arrow::Status
    add_value(const google::protobuf::Message* msg, int field_idx) override {
        if (!_arrow_status.ok()) {
            return _arrow_status;
        }
        do_add<ArrowType>(msg, field_idx);
        return _arrow_status;
    }

    arrow::Status finish_batch() override {
        if (!_arrow_status.ok()) {
            return _arrow_status;
        }
        auto&& builder_result = _builder->Finish();
        _arrow_status = builder_result.status();
        std::shared_ptr<arrow::Array> array;
        if (!_arrow_status.ok()) {
            return _arrow_status;
        }

        // Safe because we validated the status after calling `Finish`
        array = std::move(builder_result).ValueUnsafe();
        _values.push_back(array);
        return _arrow_status;
    }

    std::shared_ptr<arrow::Field> field(const std::string& name) override {
        return arrow::field(
          name, arrow::TypeTraits<ArrowType>::type_singleton());
    }

    std::shared_ptr<arrow::ArrayBuilder> builder() override { return _builder; }

private:
    template<typename T>
    void
    do_add(const google::protobuf::Message* /* msg */, int /* field_idx */) {
        throw std::runtime_error("Not implemented!");
    }

    // Signed integer types
    template<>
    void do_add<arrow::Int32Type>(
      const google::protobuf::Message* msg, int field_idx) {
        auto desc = msg->GetDescriptor()->field(field_idx);
        _arrow_status = _builder->Append(
          msg->GetReflection()->GetInt32(*msg, desc));
    }

    template<>
    void do_add<arrow::Int64Type>(
      const google::protobuf::Message* msg, int field_idx) {
        auto desc = msg->GetDescriptor()->field(field_idx);
        _arrow_status = _builder->Append(
          msg->GetReflection()->GetInt64(*msg, desc));
    }

    // Unsigned Integer Types
    // FIXME: Iceberg doesn't support unsigned integer types. I'm including
    // these to use for Tweet Ids for a demo, but we should not actually include
    // them.
    template<>
    void do_add<arrow::UInt32Type>(
      const google::protobuf::Message* msg, int field_idx) {
        auto desc = msg->GetDescriptor()->field(field_idx);
        _arrow_status = _builder->Append(
          msg->GetReflection()->GetUInt32(*msg, desc));
    }

    template<>
    void do_add<arrow::UInt64Type>(
      const google::protobuf::Message* msg, int field_idx) {
        auto desc = msg->GetDescriptor()->field(field_idx);
        _arrow_status = _builder->Append(
          msg->GetReflection()->GetUInt64(*msg, desc));
    }

    // String
    template<>
    void do_add<arrow::StringType>(
      const google::protobuf::Message* msg, int field_idx) {
        auto desc = msg->GetDescriptor()->field(field_idx);
        _arrow_status = _builder->Append(
          msg->GetReflection()->GetString(*msg, desc));
    }

    std::shared_ptr<BuilderType> _builder;
};

class proto_to_arrow_struct : public proto_to_array {
public:
    explicit proto_to_arrow_struct(
      const google::protobuf::Descriptor* message_descriptor) {
        using namespace proto_to_arrow_impl;
        namespace pb = google::protobuf;

        // Set up child arrays
        for (int field_idx = 0; field_idx < message_descriptor->field_count();
             field_idx++) {
            auto field_desc = message_descriptor->field(field_idx);
            if (field_desc->cpp_type() == pb::FieldDescriptor::CPPTYPE_INT32) {
                _child_arrays.push_back(
                  std::make_unique<proto_to_array_scalar<arrow::Int32Type>>());
            } else if (
              field_desc->cpp_type() == pb::FieldDescriptor::CPPTYPE_INT64) {
                _child_arrays.push_back(
                  std::make_unique<proto_to_array_scalar<arrow::Int64Type>>());
            } else if (
              field_desc->cpp_type() == pb::FieldDescriptor::CPPTYPE_UINT32) {
                _child_arrays.push_back(
                  std::make_unique<proto_to_array_scalar<arrow::UInt32Type>>());
            } else if (
              field_desc->cpp_type() == pb::FieldDescriptor::CPPTYPE_UINT64) {
                _child_arrays.push_back(
                  std::make_unique<proto_to_array_scalar<arrow::UInt64Type>>());
            } else if (
              field_desc->cpp_type() == pb::FieldDescriptor::CPPTYPE_STRING) {
                _child_arrays.push_back(
                  std::make_unique<proto_to_array_scalar<arrow::StringType>>());

            } else if (
              field_desc->cpp_type() == pb::FieldDescriptor::CPPTYPE_MESSAGE) {
                auto field_message_descriptor = field_desc->message_type();
                assert(field_message_descriptor != nullptr);
                _child_arrays.push_back(std::make_unique<proto_to_arrow_struct>(
                  field_message_descriptor));
            } else {
                throw std::runtime_error(
                  std::string("Unknown type: ") + field_desc->cpp_type_name());
            }
        }
        // Make Arrow data types

        // This could be combined into a single loop with the one above, but
        // this seems more readable to me
        arrow::FieldVector fields;
        for (int field_idx = 0; field_idx < message_descriptor->field_count();
             field_idx++) {
            auto field_desc = message_descriptor->field(field_idx);
            auto field_name = field_desc->name();
            fields.push_back(_child_arrays[field_idx]->field(field_name));
        }
        _arrow_data_type = arrow::struct_(fields);

        // Make builder
        std::vector<std::shared_ptr<arrow::ArrayBuilder>> child_builders;
        // This could also be collapsed into the above loop
        for (auto& child : _child_arrays) {
            child_builders.push_back(child->builder());
        }
        _builder = std::make_shared<arrow::StructBuilder>(
          _arrow_data_type, arrow::default_memory_pool(), child_builders);
    }

    arrow::Status
    add_value(const google::protobuf::Message* msg, int field_idx) override {
        if (!_arrow_status.ok()) {
            return _arrow_status;
        }
        auto desc = msg->GetDescriptor()->field(field_idx);
        auto child_message = &msg->GetReflection()->GetMessage(*msg, desc);
        for (size_t child_idx = 0; child_idx < _child_arrays.size();
             child_idx++) {
            _arrow_status = _child_arrays[child_idx]->add_value(
              child_message, int(child_idx));
            if (!_arrow_status.ok()) {
                break;
            }
        }
        _arrow_status = _builder->Append();

        return _arrow_status;
    }

    arrow::Status finish_batch() override {
        if (!_arrow_status.ok()) {
            return _arrow_status;
        }

        auto&& builder_result = _builder->Finish();
        _arrow_status = builder_result.status();
        std::shared_ptr<arrow::Array> array;
        if (!_arrow_status.ok()) {
            return _arrow_status;
        }

        // Safe because we validated the status after calling `Finish`
        array = std::move(builder_result).ValueUnsafe();
        _values.push_back(array);

        return _arrow_status;
    }

    std::shared_ptr<arrow::Field> field(const std::string& name) override {
        return arrow::field(name, _arrow_data_type);
    }
    std::shared_ptr<arrow::ArrayBuilder> builder() override { return _builder; }

private:
    std::vector<std::unique_ptr<proto_to_array>> _child_arrays;
    std::shared_ptr<arrow::DataType> _arrow_data_type;
    std::shared_ptr<arrow::StructBuilder> _builder;
};

} // namespace proto_to_arrow_impl

class proto_to_arrow_converter {
public:
    proto_to_arrow_converter(std::string schema, std::string message_type)
      : _message_type(message_type) {
        initialize_protobuf_schema(schema);
        initialize_arrow_arrays();
    }

    void add_message(const std::string& serialized_message) {
        std::unique_ptr<google::protobuf::Message> message = parse_message(
          serialized_message);
        if (message == nullptr) {
            // FIXME: Silently ignoring unparseable messages seems bad.
            return;
        }
        add_message_parsed(std::move(message));
        // assert(_builder->Append().ok()); // FIXME: check this, don't assert
    }

    void finish_batch() {
        for (auto& [field_idx, array] : _arrays) {
            assert(array->finish_batch().ok());
        }
    }

    std::shared_ptr<arrow::Table> build_table() {
        // TODO: if there is still data in the builders, call finish_batch and
        // log an error that the caller should have called it.
        std::vector<std::shared_ptr<arrow::ChunkedArray>> data_arrays;
        for (auto& [field_idx, array] : _arrays) {
            data_arrays.push_back(array->finish());
        }
        // FIXME: This will fail if we don't have any columns!
        auto table = arrow::Table::Make(
          build_schema(), data_arrays, data_arrays[0]->length());
        return table;
    }

    std::vector<std::shared_ptr<arrow::Field>> build_field_vec() {
        const google::protobuf::Descriptor* message_desc
          = _file_desc->FindMessageTypeByName(_message_type);
        assert(message_desc != nullptr);

        std::vector<std::shared_ptr<arrow::Field>> field_vec;
        for (auto& [field_idx, array] : _arrays) {
            auto field = message_desc->field(field_idx);
            field_vec.push_back(array->field(field->name()));
        }
        return field_vec;
    }

    std::shared_ptr<arrow::Schema> build_schema() {
        return arrow::schema(build_field_vec());
    }

    //// PRIVATE ////

    void
    add_message_parsed(std::unique_ptr<google::protobuf::Message> message) {
        // TODO(jcipar): Allocating and deallocating the field descriptor array
        // for every message is probably a bad idea.
        auto reflection = message->GetReflection();
        std::vector<const google::protobuf::FieldDescriptor*> field_descriptors;
        reflection->ListFields(*message, &field_descriptors);
        for (auto& [field_idx, array] : _arrays) {
            // TODO: handle this error
            assert(array->add_value(message.get(), field_idx).ok());
        }
    }

    void initialize_protobuf_schema(const std::string& schema) {
        google::protobuf::io::ArrayInputStream proto_input_stream(
          schema.c_str(), schema.size());
        google::protobuf::io::Tokenizer tokenizer(&proto_input_stream, nullptr);

        google::protobuf::compiler::Parser parser;
        if (!parser.Parse(&tokenizer, &_file_descriptor_proto)) {
            // TODO: custom exception type, or does something exist in wasm or
            // schema registry already?
            throw std::runtime_error("Could not parse protobuf schema");
        }

        if (!_file_descriptor_proto.has_name()) {
            _file_descriptor_proto.set_name("test_message");
        }

        _file_desc = _protobuf_descriptor_pool.BuildFile(
          _file_descriptor_proto);
        if (_file_desc == nullptr) {
            throw std::runtime_error("Could not build descriptor pool");
        }
    }

    void initialize_arrow_arrays() {
        using namespace proto_to_arrow_impl;
        namespace pb = google::protobuf;
        const pb::Descriptor* message_desc = _file_desc->FindMessageTypeByName(
          _message_type);
        assert(message_desc != nullptr);

        for (int field_idx = 0; field_idx < message_desc->field_count();
             field_idx++) {
            auto desc = message_desc->field(field_idx);

            if (desc->cpp_type() == pb::FieldDescriptor::CPPTYPE_INT32) {
                _arrays[field_idx]
                  = std::make_unique<proto_to_array_scalar<arrow::Int32Type>>();
            } else if (desc->cpp_type() == pb::FieldDescriptor::CPPTYPE_INT64) {
                _arrays[field_idx]
                  = std::make_unique<proto_to_array_scalar<arrow::Int64Type>>();
            } else if (
              desc->cpp_type() == pb::FieldDescriptor::CPPTYPE_UINT32) {
                _arrays[field_idx] = std::make_unique<
                  proto_to_array_scalar<arrow::UInt32Type>>();
            } else if (
              desc->cpp_type() == pb::FieldDescriptor::CPPTYPE_UINT64) {
                _arrays[field_idx] = std::make_unique<
                  proto_to_array_scalar<arrow::UInt64Type>>();
            } else if (
              desc->cpp_type() == pb::FieldDescriptor::CPPTYPE_STRING) {
                _arrays[field_idx] = std::make_unique<
                  proto_to_array_scalar<arrow::StringType>>();
            } else if (
              desc->cpp_type() == pb::FieldDescriptor::CPPTYPE_MESSAGE) {
                auto field_message_descriptor = desc->message_type();
                assert(field_message_descriptor != nullptr);
                _arrays[field_idx] = std::make_unique<proto_to_arrow_struct>(
                  field_message_descriptor);
            } else {
                throw std::runtime_error(
                  std::string("Unknown type: ") + desc->cpp_type_name());
            }
        }
        // std::vector<std::shared_ptr<arrow::ArrayBuilder>> child_builders;
        // for (auto& [field_idx, array] : _arrays) {
        //     child_builders.push_back(array->builder());
        // }

        // _builder = std::make_unique<arrow::StructBuilder>(
        //   arrow::struct_(build_field_vec()),
        //   arrow::default_memory_pool(),
        //   child_builders);
    }

    /// Parse the message to a protobuf message.
    /// Return nullptr on error.
    std::unique_ptr<google::protobuf::Message>
    parse_message(const std::string& message) {
        // TODO: How much of this can be moved to initialization code to avoid
        // reallocating objects?

        // Get the message descriptor
        const google::protobuf::Descriptor* message_desc
          = _file_desc->FindMessageTypeByName(_message_type);
        assert(message_desc != nullptr);

        const google::protobuf::Message* prototype_msg = _factory.GetPrototype(
          message_desc);
        assert(prototype_msg != nullptr);

        google::protobuf::Message* mutable_msg = prototype_msg->New();
        assert(mutable_msg != nullptr);

        if (!mutable_msg->ParseFromString(message)) {
            return nullptr;
        }
        return std::unique_ptr<google::protobuf::Message>(mutable_msg);
    }

    // Proto to array converters. Map represents field_id->proto_to_array
    std::map<int, std::unique_ptr<proto_to_arrow_impl::proto_to_array>> _arrays;

private:
    const std::string _message_type;

    // std::unique_ptr<arrow::StructBuilder> _builder;

    // Protobuf parsing
    // TODO: Figure out which of these need to remain live after the constructor
    // builds them
    google::protobuf::DescriptorPool _protobuf_descriptor_pool;
    google::protobuf::FileDescriptorProto _file_descriptor_proto;
    google::protobuf::DynamicMessageFactory _factory;
    const google::protobuf::FileDescriptor* _file_desc;
};

inline std::shared_ptr<arrow::ChunkedArray>
table_to_chunked_struct_array(const std::shared_ptr<arrow::Table> table) {
    if (table->columns().size() == 0) {
        return nullptr;
    }
    int chunk_count = table->columns()[0]->num_chunks();

    // Build data type & child builders
    arrow::FieldVector fields;
    auto column_names = table->ColumnNames();
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> child_builders;
    for (const auto& name : column_names) {
        auto column = table->GetColumnByName(name);
        auto type = column->type();
        fields.push_back(arrow::field(name, type));

        // make builder
        auto unique_builder_result = arrow::MakeBuilder(type);
        if (!unique_builder_result.ok()) {
            return nullptr;
        }
        std::shared_ptr<arrow::ArrayBuilder> builder
          = std::shared_ptr<arrow::ArrayBuilder>(
            std::move(unique_builder_result.ValueUnsafe()));
        child_builders.push_back(builder);
    }
    std::shared_ptr<arrow::DataType> struct_type = arrow::struct_(fields);

    // Make builder
    auto struct_builder = arrow::StructBuilder(
      struct_type, arrow::default_memory_pool(), child_builders);

    // Iterate over chunks, rows, and columns
    arrow::ArrayVector result_vector;
    for (int chunk_num = 0; chunk_num < chunk_count; chunk_num++) {
        int64_t row_count = table->columns()[0]->chunk(chunk_num)->length();
        for (int64_t row_num = 0; row_num < row_count; row_num++) {
            for (int column_num = 0; column_num < table->num_columns();
                 column_num++) {
                auto column = table->column(column_num);
                auto chunk = column->chunk(chunk_num);
                auto scalar_result = chunk->GetScalar(row_num);
                if (!scalar_result.ok()) {
                    return nullptr;
                }
                if (!child_builders[column_num]
                       ->AppendScalar(*scalar_result.ValueUnsafe())
                       .ok()) {
                    return nullptr;
                }
            }
            if (!struct_builder.Append().ok()) {
                return nullptr;
            }
        }

        // Finish the chunk
        auto struct_result = struct_builder.Finish();
        if (!struct_result.ok()) {
            return nullptr;
        }
        result_vector.push_back(struct_result.ValueUnsafe());
    }

    // Make the chunked array
    auto result_array = std::make_shared<arrow::ChunkedArray>(
      result_vector, struct_type);

    return result_array;
}

} // namespace datalake
