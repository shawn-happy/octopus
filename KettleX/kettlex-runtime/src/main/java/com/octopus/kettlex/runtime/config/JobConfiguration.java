package com.octopus.kettlex.runtime.config;

import static com.octopus.kettlex.core.steps.config.ConfigurationTag.TaskConfigurationTag.TASK_DESCRIPTION;
import static com.octopus.kettlex.core.steps.config.ConfigurationTag.TaskConfigurationTag.TASK_ID;
import static com.octopus.kettlex.core.steps.config.ConfigurationTag.TaskConfigurationTag.TASK_NAME;
import static com.octopus.kettlex.core.steps.config.ConfigurationTag.TaskConfigurationTag.TASK_READER_CONFIGS;
import static com.octopus.kettlex.core.steps.config.ConfigurationTag.TaskConfigurationTag.TASK_RUNTIME_CONFIG;
import static com.octopus.kettlex.core.steps.config.ConfigurationTag.TaskConfigurationTag.TASK_TRANSFORMATION_CONFIGS;
import static com.octopus.kettlex.core.steps.config.ConfigurationTag.TaskConfigurationTag.TASK_VERSION;
import static com.octopus.kettlex.core.steps.config.ConfigurationTag.TaskConfigurationTag.TASK_WRITER_CONFIGS;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.octopus.kettlex.core.utils.YamlUtil;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class JobConfiguration {

  private String taskId;
  private String taskName;
  private Version version;
  private String description;

  private List<Map<String, Object>> readers;
  private List<Map<String, Object>> transforms;
  private List<Map<String, Object>> writers;
  private RuntimeConfig runtimeConfig;

  public void loadYaml(JsonNode jsonNode) {
    //    JsonNode jsonNode =
    //        YamlUtil.toYamlNode(yaml)
    //            .orElseThrow(
    //                () ->
    //                    new KettleXParseException(
    //                        String.format("parse task configuration error, config: [%s]", yaml)));
    this.taskId = jsonNode.findPath(TASK_ID).asText();
    this.taskName = jsonNode.findPath(TASK_NAME).asText();
    this.version = Version.of(jsonNode.findPath(TASK_VERSION).asText());
    this.description = jsonNode.findPath(TASK_DESCRIPTION).asText();

    JsonNode readerConfigsNode = jsonNode.findPath(TASK_READER_CONFIGS);
    this.readers =
        (readerConfigsNode == null || readerConfigsNode.isNull() || readerConfigsNode.isEmpty())
            ? null
            : YamlUtil.fromYaml(
                    readerConfigsNode.toString(), new TypeReference<List<Map<String, Object>>>() {})
                .orElse(null);

    JsonNode transformationConfigsNode = jsonNode.findPath(TASK_TRANSFORMATION_CONFIGS);
    this.transforms =
        (transformationConfigsNode == null
                || transformationConfigsNode.isNull()
                || transformationConfigsNode.isEmpty())
            ? null
            : YamlUtil.fromYaml(
                    transformationConfigsNode.toString(),
                    new TypeReference<List<Map<String, Object>>>() {})
                .orElse(null);

    JsonNode writerConfigsNode = jsonNode.findPath(TASK_WRITER_CONFIGS);
    this.writers =
        (writerConfigsNode == null || writerConfigsNode.isNull() || writerConfigsNode.isEmpty())
            ? null
            : YamlUtil.fromYaml(
                    writerConfigsNode.toString(), new TypeReference<List<Map<String, Object>>>() {})
                .orElse(null);

    this.runtimeConfig = loadRuntimeConfig(jsonNode.findPath(TASK_RUNTIME_CONFIG));
  }

  private RuntimeConfig loadRuntimeConfig(JsonNode runtimeConfigNode) {
    RuntimeConfig runtimeConfig = new RuntimeConfig();
    runtimeConfig.loadYaml(runtimeConfigNode);
    return runtimeConfig;
  }
}
