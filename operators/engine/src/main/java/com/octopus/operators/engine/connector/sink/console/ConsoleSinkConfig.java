package com.octopus.operators.engine.connector.sink.console;

import com.octopus.operators.engine.config.sink.SinkConfig;
import com.octopus.operators.engine.config.sink.SinkOptions;
import com.octopus.operators.engine.config.sink.SinkType;
import com.octopus.operators.engine.config.sink.WriteMode;
import com.octopus.operators.engine.connector.sink.console.ConsoleSinkConfig.ConsoleSinkOptions;
import com.octopus.operators.engine.util.JsonUtils;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConsoleSinkConfig implements SinkConfig<ConsoleSinkOptions> {
  private String name;
  @Default private SinkType type = SinkType.CONSOLE;
  private ConsoleSinkOptions options;
  private String input;
  private Integer parallelism;
  private WriteMode writeMode = WriteMode.APPEND;

  public static class ConsoleSinkOptions implements SinkOptions {

    @Override
    public ConsoleSinkOptions toOptions(String json) {
      return JsonUtils.fromJson(json, ConsoleSinkOptions.class).orElseThrow();
    }

    @Override
    public String toJson() {
      return null;
    }
  }
}
