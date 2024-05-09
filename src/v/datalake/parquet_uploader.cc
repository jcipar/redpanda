#include "datalake/parquet_uploader.h"

#include "bytes/iostream.h"
#include "cloud_storage/remote.h"

#include <seastar/util/file.hh>

#include <arrow/scalar.h>
#include <arrow/type_fwd.h>
#include <arrow/visitor.h>

ss::future<bool> datalake::parquet_uploader::upload_parquet(
  std::filesystem::path path,
  archival::upload_candidate candidate,
  const cloud_storage_clients::bucket_name& bucket_name,
  cloud_storage::remote& remote,
  retry_chain_node& rtc,
  retry_chain_logger& logger) {
    std::string_view topic_name = model::topic_view(
      _log->config().ntp().tp.topic);

    bool write_success = co_await write_parquet_locally(
      path, candidate.starting_offset, candidate.final_offset);

    if (!write_success) {
        vlog(
          logger.debug,
          "Writing datalake topic to parquet failed {}.",
          model::topic_view(_log->config().ntp().tp.topic));
        co_return false;
    }

    cloud_storage::upload_result ret = co_await put_parquet_file(
      bucket_name,
      topic_name,
      std::filesystem::path(path),
      remote,
      rtc,
      logger);

    if (ret == cloud_storage::upload_result::success) {
        vlog(
          logger.debug,
          "Uploaded datalake topic {} successfully.",
          model::topic_view(_log->config().ntp().tp.topic));
    } else {
        vlog(
          logger.warn,
          "Uploading datalake topic {} failed: {}",
          model::topic_view(_log->config().ntp().tp.topic),
          ret);
        co_return false;
    }

    co_return true;
}

ss::future<bool> datalake::parquet_uploader::write_parquet_locally(
  const std::filesystem::path inner_path,
  model::offset starting_offset,
  model::offset ending_offset) {
    storage::log_reader_config reader_cfg(
      starting_offset,
      ending_offset,
      0,
      // The constructor that doesn't take max_bytes parameter uses the numeric
      // limit:
      std::numeric_limits<size_t>::max(),
      ss::default_priority_class(),
      model::record_batch_type::raft_data,
      std::nullopt,
      std::nullopt);

    auto reader = co_await _log->make_reader(reader_cfg);

    std::string_view topic_name = model::topic_view(
      _log->config().ntp().tp.topic);

    // TODO: Is this a good path? should it be configurable?
    std::filesystem::path path = std::filesystem::path("/tmp/parquet_files")
                                 / topic_name / inner_path;
    auto schema = co_await _schema_registry->get_raw_topic_schema(
      std::string(topic_name));
    arrow_writing_consumer consumer(path, schema.schema);
    auto status = co_await reader.consume(
      std::move(consumer), model::no_timeout);
    co_return status.ok();
}

ss::future<cloud_storage::upload_result>
datalake::parquet_uploader::put_parquet_file(
  const cloud_storage_clients::bucket_name& bucket,
  const std::string_view topic_name,
  const std::filesystem::path& inner_path,
  cloud_storage::remote& remote,
  retry_chain_node& rtc,
  retry_chain_logger& rtclog) {
    // TODO: function to construct local and remote paths
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
        // TODO: this upload code reads the entire file into an iobuf.

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
