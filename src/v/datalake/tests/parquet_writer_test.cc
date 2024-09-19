/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/arrow_translator.h"
#include "datalake/parquet_writer.h"
#include "datalake/tests/test_data.h"
#include "iceberg/tests/value_generator.h"
#include "utils/file_io.h"

#include <arrow/array.h>
#include <arrow/io/interfaces.h>
#include <arrow/io/memory.h>
#include <arrow/table.h>
#include <arrow/type_fwd.h>
#include <gtest/gtest.h>
#include <parquet/arrow/reader.h>
#include <parquet/type_fwd.h>

TEST(ParquetWriter, DoesNothing) {
    datalake::arrow_translator schema_translator(
      test_schema(iceberg::field_required::no));

    iobuf full_result;

    for (int i = 0; i < 5; i++) {
        auto data = iceberg::tests::make_value(
          iceberg::tests::value_spec{
            .forced_fixed_val = iobuf::from("Hello world")},
          test_schema(iceberg::field_required::no));
        schema_translator.add_data(std::move(data));
    }

    std::shared_ptr<arrow::Array> result = schema_translator.take_chunk();
    ASSERT_NE(result, nullptr);

    datalake::arrow_to_iobuf writer(schema_translator.build_arrow_schema());

    writer.add_arrow_array(result);
    iobuf serialized = writer.take_iobuf();
    // EXPECT_NEAR(serialized.size_bytes(), 3300, 200);
    full_result.append_fragments(std::move(serialized));

    for (int i = 0; i < 10; i++) {
        writer.add_arrow_array(result);
        serialized = writer.take_iobuf();
        // Sizes are not consistent between writes, but should be about
        // right.
        EXPECT_NEAR(serialized.size_bytes(), 3300, 200);
        full_result.append_fragments(std::move(serialized));
    }

    // The last write is also long. This is probably Parquet footer information.
    serialized = writer.close_and_take_iobuf();
    // EXPECT_NEAR(serialized.size_bytes(), 22881, 1000);
    full_result.append_fragments(std::move(serialized));
    std::cerr << "Full result size is " << full_result.size_bytes()
              << std::endl;

    // EXPECT_NEAR(full_result.size_bytes(), 60000, 1000);

    // Check that the data is a valid parquet file. Convert the iobuf to a
    // single buffer then import that into an arrow::io::BufferReader
    auto vbegin = iobuf::byte_iterator(
      full_result.cbegin(), full_result.cend());
    auto vend = iobuf::byte_iterator(full_result.cend(), full_result.cend());
    std::string full_result_string;
    // Byte iterators don't work with the string constructor.
    while (vbegin != vend) {
        full_result_string += *vbegin;
        ++vbegin;
    }
    auto reader = std::make_shared<arrow::io::BufferReader>(full_result_string);

    // Open Parquet file reader
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    ASSERT_TRUE(parquet::arrow::OpenFile(
                  reader, arrow::default_memory_pool(), &arrow_reader)
                  .ok());

    // Read entire file as a single Arrow table
    std::shared_ptr<arrow::Table> table;
    ASSERT_TRUE(arrow_reader->ReadTable(&table).ok());

    EXPECT_EQ(table->num_rows(), 11 * 5);
    EXPECT_EQ(table->num_columns(), 17);
}
