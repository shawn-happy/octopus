package com.octopus.datatunnel.connector.sink;

import com.octopus.datatunnel.connector.sink.ConsoleSinkConfig.ConsoleSinkOptions;
import com.octopus.operators.engine.config.step.SinkConfig;
import com.octopus.operators.engine.config.step.StepConfig;
import com.octopus.operators.engine.config.step.StepOptions;
import com.octopus.operators.engine.config.step.WriteMode;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
public class ConsoleSinkConfig implements SinkConfig<ConsoleSinkOptions> {
  @Default private final String type = "console";
  private String id;
  private String name;
  private String sourceTable;
  private String description;
  private ConsoleSinkOptions options;
  private String input;
  private Integer parallelism;
  @Default private WriteMode writeMode = WriteMode.APPEND;

  @Override
  public StepConfig<ConsoleSinkOptions> loadYaml(String yaml) {
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
  }
}
