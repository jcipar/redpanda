add_subdirectory(tests)
# add_subdirectory(parser)

find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)
find_package(Protobuf REQUIRED)

v_cc_library(
  NAME datalake
  SRCS
  arrow_writer.cc
  protobuf_to_arrow.cc
#   schema_registry.cc
  DEPS
    # wasmtime
    # v::wasm_parser
    v::storage
    v::model
    v::cloud_storage
    v::raft


    # v::pandaproxy_schema_registry
    Seastar::seastar
    Arrow::arrow_shared
    Parquet::parquet_shared
    protobuf::libprotobuf
)
