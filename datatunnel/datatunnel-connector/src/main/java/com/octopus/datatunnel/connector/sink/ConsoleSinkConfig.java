package com.octopus.datatunnel.connector.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.octopus.datatunnel.connector.sink.ConsoleSinkConfig.ConsoleSinkOptions;
import com.octopus.operators.engine.config.CheckResult;
import com.octopus.operators.engine.config.step.AbstractSinkConfig;
import com.octopus.operators.engine.config.step.StepOptions;
import java.util.List;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
public class ConsoleSinkConfig extends AbstractSinkConfig<ConsoleSinkOptions> {
  @Default private final String type = "console";
  private ConsoleSinkOptions options;

  @Override
  protected ConsoleSinkOptions loadOptions(JsonNode options) {
    return null;
  }

  @Getter
  @Builder
  @NoArgsConstructor
  public static class ConsoleSinkOptions implements StepOptions {

    @Override
    public StepOptions loadYaml(String yaml) {
      return null;
    }

    @Override
    public List<CheckResult> check() {
      return null;
    }
  }
}
