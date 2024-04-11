#include <arrow/api.h>
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
    virtual arrow::Status add_value(google::protobuf::Message*, int field_idx)
      = 0;
    virtual arrow::Status finish_batch() { return arrow::Status::OK(); }
    std::shared_ptr<arrow::ChunkedArray> finish() {
        return std::make_shared<arrow::ChunkedArray>(_values);
    }

    virtual std::shared_ptr<arrow::Field> field(const std::string& name) = 0;

protected:
    arrow::Status _arrow_status;
    arrow::ArrayVector _values;
};

template<typename ArrowType>
class proto_to_array_template : public proto_to_array {
public:
    arrow::Status
    add_value(google::protobuf::Message* msg, int field_idx) override {
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
        auto&& builder_result = _builder.Finish();
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

private:
    template<typename T>
    void do_add(google::protobuf::Message* /* msg */, int /* field_idx */) {
        throw std::runtime_error("Not implemented!");
    }

    template<>
    void
    do_add<arrow::Int32Type>(google::protobuf::Message* msg, int field_idx) {
        auto desc = msg->GetDescriptor()->field(field_idx);
        _arrow_status = _builder.Append(
          msg->GetReflection()->GetInt32(*msg, desc));
    }

    template<>
    void
    do_add<arrow::StringType>(google::protobuf::Message* msg, int field_idx) {
        auto desc = msg->GetDescriptor()->field(field_idx);
        _arrow_status = _builder.Append(
          msg->GetReflection()->GetString(*msg, desc));
    }

    arrow::TypeTraits<ArrowType>::BuilderType _builder;
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
        std::cerr << "*** Parsing message: \"" << serialized_message << "\""
                  << std::endl;
        add_message_parsed(std::move(message));
    }

    void finish_batch() {
        for (auto& [field_idx, array] : _arrays) {
            assert(array->finish_batch().ok());
        }
    }

    std::shared_ptr<arrow::Table> build_table() {
        std::vector<std::shared_ptr<arrow::ChunkedArray>> data_arrays;
        for (auto& [field_idx, array] : _arrays) {
            data_arrays.push_back(array->finish());
        }
        // FIXME: This will fail if we don't have any columns!
        return arrow::Table::Make(
          build_schema(), data_arrays, data_arrays[0]->length());
    }

    std::shared_ptr<arrow::Schema> build_schema() {
        const google::protobuf::Descriptor* message_desc
          = _file_desc->FindMessageTypeByName(_message_type);
        assert(message_desc != nullptr);

        std::vector<std::shared_ptr<arrow::Field>> field_vec;
        for (auto& [field_idx, array] : _arrays) {
            auto field = message_desc->field(field_idx);
            field_vec.push_back(array->field(field->name()));
        }
        return arrow::schema(field_vec);
    }

    //// PRIVATE ////

    void
    add_message_parsed(std::unique_ptr<google::protobuf::Message> message) {
        // TODO(jcipar): Allocating and deallocating the field descriptor array
        // for every message is probably a bad idea.
        auto reflection = message->GetReflection();
        std::vector<const google::protobuf::FieldDescriptor*> field_descriptors;
        reflection->ListFields(*message, &field_descriptors);
        std::cerr << "*** READING FIELDS" << std::endl;
        for (auto& [field_idx, array] : _arrays) {
            std::cerr << "*** Adding field index " << field_idx << std::endl;
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
                _arrays[field_idx] = std::make_unique<
                  proto_to_array_template<arrow::Int32Type>>();
            } else if (
              desc->cpp_type() == pb::FieldDescriptor::CPPTYPE_STRING) {
                _arrays[field_idx] = std::make_unique<
                  proto_to_array_template<arrow::StringType>>();
            } else {
                throw std::runtime_error(
                  std::string("Unknown type: ") + desc->cpp_type_name());
            }
        }
    }

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
            throw std::runtime_error("Could not parse message");
        }
        return std::unique_ptr<google::protobuf::Message>(mutable_msg);
    }

    // Proto to array converters. Map represents field_id->proto_to_array
    std::map<int, std::unique_ptr<proto_to_arrow_impl::proto_to_array>> _arrays;

private:
    const std::string _message_type;

    // Protobuf parsing
    // TODO: Figure out which of these need to remain live after the constructor
    // builds them
    google::protobuf::DescriptorPool _protobuf_descriptor_pool;
    google::protobuf::FileDescriptorProto _file_descriptor_proto;
    google::protobuf::DynamicMessageFactory _factory;
    const google::protobuf::FileDescriptor* _file_desc;
};

} // namespace datalake
