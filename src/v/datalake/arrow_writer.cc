#include "bytes/iobuf.h"
#include "bytes/iostream.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/types.h"
#include "cluster/partition.h"
#include "datalake/protobuf_to_arrow.h"
#include "model/fundamental.h"
#include "model/record.h"

#include <seastar/core/loop.hh>
#include <seastar/util/file.hh>

#include <arrow/array/builder_binary.h>
#include <arrow/scalar.h>
#include <arrow/type_fwd.h>
#include <arrow/visitor.h>
#include <datalake/arrow_writer.h>
// #include <datalake/schema_registry.h>

#include <filesystem>
#include <memory>
#include <optional>
#include <string_view>

ss::future<datalake::arrow_writing_consumer::schema_info>
get_schema(model::topic /* topic */) {
    // std::string_view topic_name = model::topic_view(topic);
    // pandaproxy::schema_registry::subject value_subject{
    //   std::string(topic_name) + "-value"};

    // std::unique_ptr<wasm::schema_registry> schema_registry
    //   = wasm::schema_registry::make_default(nullptr);

    // auto value_schema = co_await schema_registry->get_subject_schema(
    //   value_subject, std::nullopt);

    // assert(
    //   value_schema.schema.type()
    //   == pandaproxy::schema_registry::schema_type::protobuf);

    // std::string value_schema_string = value_schema.schema.def().raw()();

    co_return datalake::arrow_writing_consumer::schema_info{
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
}

ss::future<bool> datalake::write_parquet(
  model::topic topic,
  const std::filesystem::path inner_path,
  ss::shared_ptr<storage::log> log,
  model::offset starting_offset,
  model::offset ending_offset) {
    storage::log_reader_config reader_cfg(
      starting_offset,
      ending_offset,
      0,
      10ll * 1024ll * 1024ll
        * 1024ll, // FIXME(jcipar): 10 GiB is probably too much.
      ss::default_priority_class(),
      model::record_batch_type::raft_data,
      std::nullopt,
      std::nullopt);

    auto reader = co_await log->make_reader(reader_cfg);

    std::string_view topic_name = model::topic_view(
      log->config().ntp().tp.topic);

    std::filesystem::path path = std::filesystem::path("/tmp/parquet_files")
                                 / topic_name / inner_path;

    auto schema = co_await get_schema(topic);

    arrow_writing_consumer consumer(schema);
    std::cerr << "*** Created consumer" << std::endl;
    std::shared_ptr<arrow::Table> table = co_await reader.consume(
      std::move(consumer), model::no_timeout);
    std::cerr << "*** Consumed log" << std::endl;
    if (table == nullptr) {
        co_return false;
    }

    // FIXME: Creating and destroying sharded_thread_workers is supposed
    // to be rare. The docs for the class suggest doing creating it once
    // during application startup.
    ssx::sharded_thread_worker thread_worker;
    co_await thread_worker.start({.name = "parquet"});
    auto result = co_await thread_worker.submit(
      [table, path]() -> arrow::Status {
          return write_table_to_parquet(table, path);
      });
    co_await thread_worker.stop();
    std::cerr << "*** Wrote parquet" << std::endl;
    co_return result.ok();
}

arrow::Status datalake::write_table_to_parquet(
  std::shared_ptr<arrow::Table> table, std::filesystem::path path) {
    std::filesystem::create_directories(path.parent_path());
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open(path));
    PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(
      *table, arrow::default_memory_pool(), outfile));
    PARQUET_THROW_NOT_OK(outfile->Close());
    return arrow::Status::OK();
}

bool datalake::is_datalake_topic(cluster::partition& partition) {
    std::string_view topic = model::topic_view(
      partition.log()->config().ntp().tp.topic);

    return topic.starts_with("experimental_datalake_");
}

ss::future<cloud_storage::upload_result> datalake::put_parquet_file(
  const cloud_storage_clients::bucket_name& bucket,
  const std::string_view topic_name,
  const std::filesystem::path& inner_path,
  cloud_storage::remote& remote,
  retry_chain_node& rtc,
  retry_chain_logger& rtclog) {
    std::filesystem::path local_path
      = std::filesystem::path("/tmp/parquet_files") / topic_name / inner_path;

    std::filesystem::path remote_path = std::filesystem::path(
                                          "experimental/parquet_files")
                                        / topic_name / inner_path;

    bool exists = co_await ss::file_exists(local_path.string());
    if (!exists) {
        vlog(rtclog.error, "Local Parquet file does not exist: {}", local_path);
        co_return cloud_storage::upload_result::failed;
    }

    iobuf file_buf;
    ss::output_stream<char> iobuf_ostream = make_iobuf_ref_output_stream(
      file_buf);

    try {
        co_await ss::util::with_file_input_stream(
          local_path,
          [&iobuf_ostream](
            ss::input_stream<char>& file_istream) -> ss::future<> {
              return ss::copy<char>(file_istream, iobuf_ostream);
          });
        co_await iobuf_ostream.close();

        auto ret = co_await remote.upload_object(
          {.transfer_details
           = {.bucket = bucket, .key = cloud_storage_clients::object_key(remote_path), .parent_rtc = rtc},
           .type = cloud_storage::upload_type::object,
           .payload = std::move(file_buf)});
        co_return ret;
    } catch (...) {
        vlog(
          rtclog.error,
          "Failed to upload parquet file: {} to {}",
          local_path,
          remote_path);
        co_return cloud_storage::upload_result::failed;
    }
}

ss::future<ss::stop_iteration>
datalake::arrow_writing_consumer::operator()(model::record_batch batch) {
    arrow::StringBuilder key_builder;
    arrow::BinaryBuilder value_builder;
    arrow::UInt64Builder timestamp_builder;
    arrow::UInt64Builder offset_builder;
    if (batch.compressed()) {
        _compressed_batches++;

        // FIXME: calling internal method of storage module seems like a red
        // flag.
        batch = storage::internal::maybe_decompress_batch_sync(batch);
    } else {
        _uncompressed_batches++;
    }
    batch.for_each_record([this,
                           &batch,
                           &key_builder,
                           &value_builder,
                           &timestamp_builder,
                           &offset_builder](model::record&& record) {
        std::string key;
        std::string value;
        key = iobuf_to_string(record.key());

        if (record.has_value()) {
            value = iobuf_to_string(record.value());
        }

        _ok = key_builder.Append(key);
        if (!_ok.ok()) {
            return ss::stop_iteration::yes;
        }
        _structured_value_converter->add_message(value);

        _ok = value_builder.Append(value);
        if (!_ok.ok()) {
            return ss::stop_iteration::yes;
        }

        _ok = timestamp_builder.Append(
          batch.header().first_timestamp.value() + record.timestamp_delta());
        if (!_ok.ok()) {
            return ss::stop_iteration::yes;
        }

        // FIXME(jcipar): this is not the correct way to compute offsets. The
        // new log reader config has a `translate_offsets` options that
        // automatically does the translation. After rebasing, use that.
        _ok = offset_builder.Append(
          int64_t(batch.header().base_offset) + record.offset_delta());
        if (!_ok.ok()) {
            return ss::stop_iteration::yes;
        }

        _rows++;
        return ss::stop_iteration::no;
    });
    if (!_ok.ok()) {
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::yes);
    }

    std::shared_ptr<arrow::Array> key_array;
    std::shared_ptr<arrow::Array> value_array;
    std::shared_ptr<arrow::Array> offset_array;
    std::shared_ptr<arrow::Array> timestamp_array;

    auto&& key_builder_result = key_builder.Finish();
    // Arrow ASSIGN_OR_RAISE macro doesn't actually raise, it returns a
    // not-ok value. Expanding the macro and editing ensures we get the
    // correct return type.
    _ok = key_builder_result.status();
    if (!_ok.ok()) {
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::yes);
    } else {
        key_array = std::move(key_builder_result).ValueUnsafe();
    }

    auto&& value_builder_result = value_builder.Finish();
    _ok = value_builder_result.status();
    if (!_ok.ok()) {
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::yes);
    } else {
        value_array = std::move(value_builder_result).ValueUnsafe();
    }

    auto&& offset_builder_result = offset_builder.Finish();
    _ok = offset_builder_result.status();
    if (!_ok.ok()) {
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::yes);
    } else {
        offset_array = std::move(offset_builder_result).ValueUnsafe();
    }

    auto&& timestamp_builder_result = timestamp_builder.Finish();
    _ok = timestamp_builder_result.status();
    if (!_ok.ok()) {
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::yes);
    } else {
        timestamp_array = std::move(timestamp_builder_result).ValueUnsafe();
    }

    _key_vector.push_back(key_array);
    _value_vector.push_back(value_array);
    _offset_vector.push_back(offset_array);
    _timestamp_vector.push_back(timestamp_array);

    _structured_value_converter->finish_batch();

    return ss::make_ready_future<ss::stop_iteration>(ss::stop_iteration::no);
}

template<typename ArrowType>
class get_arrow_value : public arrow::ScalarVisitor {};

std::shared_ptr<arrow::Table> datalake::arrow_writing_consumer::get_table() {
    if (!_ok.ok()) {
        return nullptr;
    }

    // Create a ChunkedArray
    std::shared_ptr<arrow::ChunkedArray> key_chunks
      = std::make_shared<arrow::ChunkedArray>(_key_vector);
    std::shared_ptr<arrow::ChunkedArray> value_chunks
      = std::make_shared<arrow::ChunkedArray>(_value_vector);
    std::shared_ptr<arrow::ChunkedArray> timestamp_chunks
      = std::make_shared<arrow::ChunkedArray>(_timestamp_vector);
    std::shared_ptr<arrow::ChunkedArray> offset_chunks
      = std::make_shared<arrow::ChunkedArray>(_offset_vector);

    auto structured_value_table = _structured_value_converter->build_table();
    std::shared_ptr<arrow::ChunkedArray> structured_value_chunks
      = datalake::table_to_chunked_struct_array(structured_value_table);

    auto schema = arrow::schema({
      arrow::field("Key", key_chunks->type()),
      arrow::field("Value", value_chunks->type()),
      arrow::field("Timestamp", timestamp_chunks->type()),
      arrow::field("Offset", offset_chunks->type()),
      arrow::field("StructuredValue", structured_value_chunks->type()),
    });

    // Create a table
    return arrow::Table::Make(
      schema,
      {key_chunks,
       value_chunks,
       timestamp_chunks,
       offset_chunks,
       structured_value_chunks},
      key_chunks->length());
}

ss::future<std::shared_ptr<arrow::Table>>
datalake::arrow_writing_consumer::end_of_stream() {
    if (!_ok.ok()) {
        co_return nullptr;
    }
    if (_key_vector.size() == 0 || _rows == 0) {
        // FIXME: use a different return type for this.
        // See the note in ntp_archiver_service::do_upload_segment when
        // calling write_parquet.
        _ok = arrow::Status::UnknownError("No Data");
        co_return nullptr;
    }
    co_return this->get_table();
}

uint32_t datalake::arrow_writing_consumer::iobuf_to_uint32(const iobuf& buf) {
    auto kbegin = iobuf::byte_iterator(buf.cbegin(), buf.cend());
    auto kend = iobuf::byte_iterator(buf.cend(), buf.cend());
    std::vector<uint8_t> key_bytes;
    while (kbegin != kend) {
        key_bytes.push_back(*kbegin);
        ++kbegin;
    }
    return *reinterpret_cast<const uint32_t*>(key_bytes.data());
}

std::string
datalake::arrow_writing_consumer::iobuf_to_string(const iobuf& buf) {
    auto vbegin = iobuf::byte_iterator(buf.cbegin(), buf.cend());
    auto vend = iobuf::byte_iterator(buf.cend(), buf.cend());
    std::string value;
    // Byte iterators don't work with the string constructor.
    while (vbegin != vend) {
        value += *vbegin;
        ++vbegin;
    }
    return value;
}

datalake::arrow_writing_consumer::arrow_writing_consumer(schema_info schema) {
    // For now these could be local variables in end_of_stream, but in
    // the future we will have the constructor take a schema argument.
    //
    // FIXME: should these be binary columns to avoid issues when we can't
    // encode as utf8? The Parquet library will happily output binary in
    // these columns, and a reader will get an exception trying to read the
    // file.
    _field_key = arrow::field("Key", arrow::utf8());

    _field_value = arrow::field("Value", arrow::binary());
    _field_timestamp = arrow::field(
      "Timestamp", arrow::uint64()); // FIXME: timestamp type?
    _field_offset = arrow::field("Offset", arrow::uint64());

    _structured_value_converter = std::make_shared<proto_to_arrow_converter>(
      schema.key_schema, schema.key_message_name);

    _field_structured_key = arrow::field(
      "StructuredKey",
      arrow::struct_(_structured_value_converter->build_field_vec()));

    _schema = arrow::schema(
      {_field_key, _field_value, _field_timestamp, _field_offset});
}
