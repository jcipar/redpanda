find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)
find_package(Protobuf REQUIRED)
rp_test(
  FIXTURE_TEST
 BINARY_NAME parquet_writer
 SOURCES tests/arrow_writer_test.cc
 LIBRARIES v::seastar_testing_main v::storage_test_utils v::model_test_utils
  v::cluster
   Arrow::arrow_shared
   Parquet::parquet_shared
  #  protobuf::libprotobuf
 ARGS "-- -c 1"
 LABELS storage
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME gtest_arrow
  SOURCES
    tests/arrow_writer_gtest.cc
  LIBRARIES
    v::raft
    v::raft_fixture
    v::cluster
    v::cloud_roles
    v::http_test_utils
    v::gtest_main
    Arrow::arrow_shared
    Parquet::parquet_shared
    protobuf::libprotobuf
  LABELS archival
  ARGS "-- -c 1"
)