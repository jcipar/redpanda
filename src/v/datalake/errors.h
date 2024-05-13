#pragma once

#include <stdexcept>
namespace datalake {

enum class arrow_converter_status {
    ok,

    // User errors
    parse_error,

    // System Errors
    internal_error,
};

class initialization_error : public std::runtime_error {
public:
    explicit initialization_error(const std::string& what_arg)
      : std::runtime_error(what_arg) {}
};

} // namespace datalake
