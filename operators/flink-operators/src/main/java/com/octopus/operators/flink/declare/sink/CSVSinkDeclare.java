package com.octopus.operators.flink.declare.sink;

import com.octopus.operators.flink.declare.common.SinkType;
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
public class CSVSinkDeclare extends FileSinkDeclare<CSVSinkOptions> {

  @Default private SinkType type = SinkType.csv;
  private CSVSinkOptions options;

  @Builder
  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class CSVSinkOptions extends FileSinkOptions {
    @Default private Boolean header = false;
    @Default private String dateFormat = "yyyy-MM-dd";
    @Default private String dateTimeFormat = "yyyy-MM-dd HH:mm:ss.SSS";
  }
}
