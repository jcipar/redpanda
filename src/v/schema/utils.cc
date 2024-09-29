/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "schema/utils.h"

#include "bytes/iobuf_parser.h"
#include "pandaproxy/schema_registry/types.h"

#include <optional>
#include <stdexcept>

namespace schema {

std::optional<pandaproxy::schema_registry::schema_id>
parse_schema_id(const iobuf& buf) {
    if (buf.size_bytes() < sizeof(uint8_t) + sizeof(uint32_t)) {
        // A record with a schema id must have at least 5 bytes:
        // 1 byte magic + 4 bytes id
        return std::nullopt;
    }
    iobuf_const_parser parser(buf);

    auto magic = parser.consume_type<uint8_t>();
    if (magic != 0) {
        return std::nullopt;
    }
    auto schema_id = parser.consume_type<int32_t>();
    return pandaproxy::schema_registry::schema_id{schema_id};
}

} // namespace schema
