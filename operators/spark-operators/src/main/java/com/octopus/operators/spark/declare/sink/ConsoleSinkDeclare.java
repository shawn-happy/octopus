package com.octopus.operators.spark.declare.sink;

import com.octopus.operators.spark.declare.common.SinkType;
import com.octopus.operators.spark.declare.common.WriteMode;
import com.octopus.operators.spark.declare.sink.ConsoleSinkDeclare.ConsoleSinkOptions;
import java.util.Collections;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class ConsoleSinkDeclare implements SinkDeclare<ConsoleSinkOptions> {

  @Builder.Default private final SinkType type = SinkType.csv;
  private ConsoleSinkOptions options;
  private String name;
  private String input;
  @Builder.Default private WriteMode writeMode = WriteMode.append;

  @Builder
  @Getter
  @NoArgsConstructor
  public static class ConsoleSinkOptions implements SinkOptions {

    @Override
    public Map<String, String> getOptions() {
      return Collections.emptyMap();
    }

    @Override
    public void verify() {}
  }
}
