package com.octopus.operators.flink.declare.source;

import com.octopus.operators.flink.declare.source.CSVSourceDeclare.CSVSourceOptions;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class CSVSourceDeclare extends FileSourceDeclare<CSVSourceOptions> {

  private CSVSourceOptions options;

  @Builder
  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class CSVSourceOptions extends FileSourceOptions {
    @Default private boolean header = true;
    @Default private boolean ignoreError = false;
    @Default private String dateFormat = "yyyy-MM-dd";
    @Default private String dateTimeFormat = "yyyy-MM-dd HH:mm:ss.SSS";
  }
}
