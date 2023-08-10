package com.octopus.operators.flink.declare.sink;

import com.octopus.operators.flink.declare.common.SinkType;
import com.octopus.operators.flink.declare.common.WriteMode;
import com.octopus.operators.flink.declare.sink.CSVSinkDeclare.CSVSinkOptions;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class CSVSinkDeclare implements SinkDeclare<CSVSinkOptions> {

  @Default private SinkType type = SinkType.csv;
  private CSVSinkOptions options;
  private String name;
  private String input;
  @Default private WriteMode writeMode = WriteMode.append;

  @Builder
  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class CSVSinkOptions implements SinkOptions {
    private String path;
    @Default private Boolean header = false;
    @Default private String dateFormat = "yyyy-MM-dd";
    @Default private String dateTimeFormat = "yyyy-MM-dd HH:mm:ss.SSS";
  }
}
