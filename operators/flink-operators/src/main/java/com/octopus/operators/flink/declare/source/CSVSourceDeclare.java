package com.octopus.operators.flink.declare.source;

import com.octopus.operators.flink.declare.common.ColumnDesc;
import com.octopus.operators.flink.declare.common.SourceType;
import com.octopus.operators.flink.declare.source.CSVSourceDeclare.CSVSourceOptions;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class CSVSourceDeclare implements SourceDeclare<CSVSourceOptions> {

  private CSVSourceOptions options;
  private SourceType type;
  private Integer repartition;
  private String output;
  private String name;

  @Builder
  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class CSVSourceOptions implements SourceOptions {
    private String[] paths;
    private List<ColumnDesc> schemas;
    @Default private boolean header = true;
    @Default private boolean ignoreError = false;
    @Default private String dateFormat = "yyyy-MM-dd";
    @Default private String dateTimeFormat = "yyyy-MM-dd HH:mm:ss.SSS";
  }
}
