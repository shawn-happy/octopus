package com.octopus.operators.flink.declare.sink;

import com.octopus.operators.flink.declare.common.SinkType;
import com.octopus.operators.flink.declare.common.WriteMode;
import com.octopus.operators.flink.declare.sink.ConsoleSinkDeclare.ConsoleSinkOptions;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.logging.log4j.Level;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class ConsoleSinkDeclare implements SinkDeclare<ConsoleSinkOptions> {

  private ConsoleSinkOptions options;
  private String name;
  private String input;
  private SinkType type;
  @Default private WriteMode writeMode = WriteMode.append;

  @Builder
  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class ConsoleSinkOptions implements SinkOptions {
    @Default private Level level = Level.INFO;
  }
}
