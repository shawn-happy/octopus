package com.octopus.kettlex.steps;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.octopus.kettlex.core.exception.KettleXException;
import com.octopus.kettlex.core.steps.config.WriteMode;
import com.octopus.kettlex.core.steps.config.WriterConfig;
import com.octopus.kettlex.core.utils.YamlUtil;
import com.octopus.kettlex.steps.LogMessageConfig.LogMessageOptions;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LogMessageConfig implements WriterConfig<LogMessageOptions> {

  private static final String STEP_TYPE = "log-message";

  private String id;
  private String name;
  private String input;
  @Default private String type = STEP_TYPE;
  private WriteMode writeMode;
  private LogMessageOptions options;

  @Override
  public void loadYaml(JsonNode jsonNode) {
    if (jsonNode == null || jsonNode.isNull()) {
      return;
    }
    LogMessageConfig logMessageConfig =
        YamlUtil.fromYaml(jsonNode.asText(), new TypeReference<LogMessageConfig>() {}).orElse(null);
    if (!Objects.isNull(logMessageConfig)) {
      String type = logMessageConfig.getType();
      if (!STEP_TYPE.equals(type)) {
        throw new KettleXException(
            String.format("the step type [%s] is not [%s]", type, STEP_TYPE));
      }
      this.id = logMessageConfig.id;
      this.name = logMessageConfig.name;
      this.input = logMessageConfig.input;
      this.type = STEP_TYPE;
      this.writeMode = logMessageConfig.writeMode;
      this.options = logMessageConfig.options;
    }
  }

  @Getter
  @Builder
  @NoArgsConstructor
  public static class LogMessageOptions implements WriterOptions {

    @Override
    public void verify() {}

    @Override
    public void loadYaml(JsonNode jsonNode) {
      if (jsonNode == null || jsonNode.isNull()) {
        return;
      }
    }
  }
}
