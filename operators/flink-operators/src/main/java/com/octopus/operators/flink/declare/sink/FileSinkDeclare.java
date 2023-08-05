package com.octopus.operators.flink.declare.sink;

import com.octopus.operators.flink.declare.common.WriteMode;
import com.octopus.operators.flink.declare.sink.FileSinkDeclare.FileSinkOptions;
import lombok.AllArgsConstructor;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public abstract class FileSinkDeclare<P extends FileSinkOptions> implements SinkDeclare<P> {

  private String name;
  private String input;
  @Default private WriteMode writeMode = WriteMode.append;

  @SuperBuilder
  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  public abstract static class FileSinkOptions implements SinkOptions {
    private String path;
  }
}
