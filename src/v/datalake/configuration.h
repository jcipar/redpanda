#pragma once

#include "cluster/partition.h"

#include <optional>
#include <string_view>

namespace datalake {
class topic_config {
public:
    // Return config, or nullopt if datalake is disabled for this topic.
    static std::optional<topic_config>
    get_config(config::configuration& conf, cluster::partition& partition) {
        if (!conf.experimental_enable_datalake) {
            std::cerr << "jcipar datalake extension is disabled\n";
            return std::nullopt;
        }

        auto topic_config = partition.get_topic_config();
        std::optional<bool> datalake_topic
          = topic_config.has_value()
            && topic_config->get().properties.experimental_datalake_topic;
        if (!datalake_topic.value_or(false)) {
            std::cerr << "jcipar not a datalake topic\n";
            return std::nullopt;
        }

        return datalake::topic_config();
    }
};
}; // namespace datalake
