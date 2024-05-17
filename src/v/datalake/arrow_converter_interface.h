#pragma once

#include <arrow/api.h>
#include <datalake/errors.h>

#include <memory>
#include <string>

namespace datalake {
class arrow_converter_interface {
public:
    virtual ~arrow_converter_interface() = default;

    [[nodiscard]] virtual arrow_converter_status
    add_message(const std::string& serialized_message)
      = 0;

    [[nodiscard]] virtual arrow_converter_status finish_batch() = 0;
    std::shared_ptr<arrow::Table> virtual build_table() = 0;
    std::shared_ptr<arrow::Schema> virtual build_schema() = 0;
};
}; // namespace datalake
