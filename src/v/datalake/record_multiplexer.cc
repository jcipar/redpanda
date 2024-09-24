/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/record_multiplexer.h"

#include "datalake/data_writer_interface.h"
#include "datalake/schemaless_translator.h"
#include "iceberg/datatypes.h"
#include "iceberg/partition.h"
#include "iceberg/partition_key.h"
#include "iceberg/struct_accessor.h"
#include "iceberg/transform.h"
#include "iceberg/values.h"
#include "model/record.h"
#include "storage/parser_utils.h"

namespace datalake {
record_multiplexer::record_multiplexer(
  std::unique_ptr<data_writer_factory> writer_factory)
  : _writer_factory{std::move(writer_factory)} {}

ss::future<ss::stop_iteration>
record_multiplexer::operator()(model::record_batch batch) {
    if (batch.compressed()) {
        batch = co_await storage::internal::decompress_batch(std::move(batch));
    }
    batch.for_each_record([&batch, this](model::record&& record) {
        iobuf key = record.release_key();
        iobuf val = record.release_value();
        // *1000: Redpanda timestamps are milliseconds. Iceberg uses
        // microseconds.
        int64_t timestamp = (batch.header().first_timestamp.value()
                             + record.timestamp_delta())
                            * 1000;
        int64_t offset = static_cast<int64_t>(batch.base_offset())
                         + record.offset_delta();
        int64_t estimated_size = key.size_bytes() + val.size_bytes() + 16;

        // Translate the record
        auto& translator = get_translator();
        iceberg::struct_value data = std::visit(
          [&key, &val, timestamp, offset](schemaless_translator& tr) {
              return tr.translate_event(
                std::move(key), std::move(val), timestamp, offset);
          },
          translator);
        auto schema = std::visit(
          [](schemaless_translator& tr) { return tr.get_schema(); },
          translator);

        // Send it to the writer
        auto& writer = get_writer(get_partition(schema, data));
        writer.add_data_struct(std::move(data), estimated_size);
    });
    co_return ss::stop_iteration::no;
}

ss::future<chunked_vector<data_writer_result>>
record_multiplexer::end_of_stream() {
    chunked_vector<data_writer_result> ret;

    for (auto& [partition, writer] : _writers) {
        data_writer_result res = writer->finish();
        ret.push_back(res);
    }
    co_return ret;
}

record_multiplexer::translator& record_multiplexer::get_translator() {
    if (!_translators.contains(0)) {
        _translators.emplace(0, schemaless_translator());
    }
    return _translators.at(0);
}

data_writer&
record_multiplexer::get_writer(const iceberg::partition_key& partition) {
    if (!_writers.contains(partition)) {
        auto schema = std::visit(
          [](schemaless_translator& tr) { return tr.get_schema(); },
          get_translator());
        _writers[partition.copy()] = _writer_factory->create_writer(
          std::move(schema));
    }
    return *_writers.at(partition);
}

// Currently only supports partitioning by hour on the column
// "redpanda_timestamp" (2)
iceberg::partition_key record_multiplexer::get_partition(
  const iceberg::struct_type& schema, const iceberg::struct_value& data) {
    auto accessors = iceberg::struct_accessor::from_struct_type(schema);
    iceberg::partition_spec partition_spec;
    partition_spec.spec_id = iceberg::partition_spec::id_t{0};
    partition_spec.fields.emplace_back(iceberg::partition_field{
      .source_id = iceberg::nested_field::id_t{2},      // redpanda_timestamp
      .field_id = iceberg::partition_field::id_t{1000}, // IDK???
      .name = "hour",
      .transform = iceberg::hour_transform{}}

    );
    // std::cerr << "Partition for " << data << "\n";
    auto partition = iceberg::partition_key::create(
      data, accessors, partition_spec);
    // std::cerr << "Partition: " << partition.val << "\n";
    return partition;
}

} // namespace datalake
