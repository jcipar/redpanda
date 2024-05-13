find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)
rp_test(
    UNIT_TEST
    GTEST
    BINARY_NAME gtest_proto_to_arrow
    SOURCES
      proto_to_arrow_gtest.cc
    LIBRARIES
      Arrow::arrow_shared
      Parquet::parquet_shared
      v::datalake
      v::gtest_main
    LABELS datalake
    ARGS "-- -c 1"
  )
  