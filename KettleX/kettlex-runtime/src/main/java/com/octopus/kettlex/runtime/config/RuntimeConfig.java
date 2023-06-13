package com.octopus.kettlex.runtime.config;

import static com.octopus.kettlex.core.steps.config.ConfigurationTag.TaskRuntimeConfigTag.CHANNEL_CAPACITY;
import static com.octopus.kettlex.core.steps.config.ConfigurationTag.TaskRuntimeConfigTag.PARAMS;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.octopus.kettlex.core.utils.YamlUtil;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class RuntimeConfig {

  private Integer channelCapcacity;
  private Map<String, String> params;

  public void loadYaml(String yaml) {
    loadYaml(YamlUtil.toYamlNode(yaml).orElse(null));
  }

  public void loadYaml(JsonNode yamlNode) {
    if (yamlNode == null || yamlNode.isNull()) {
      return;
    }
    this.channelCapcacity = yamlNode.findPath(CHANNEL_CAPACITY).asInt(10000);
    JsonNode jsonNode = yamlNode.findPath(PARAMS);
    this.params =
        (jsonNode == null || jsonNode.isNull() || jsonNode.isEmpty())
            ? null
            : YamlUtil.fromYaml(
                    yamlNode.findPath(PARAMS).toString(),
                    new TypeReference<Map<String, String>>() {})
                .orElse(null);
  }
}
