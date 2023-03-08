package com.shawn.octopus.spark.operators.common.declare.sink;

import com.shawn.octopus.spark.operators.common.SupportedSinkType;
import com.shawn.octopus.spark.operators.common.WriteMode;
import com.shawn.octopus.spark.operators.common.declare.sink.CSVSinkDeclare.CSVSinkOptions;
import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class CSVSinkDeclare implements SinkDeclare<CSVSinkOptions> {

  @Default private final SupportedSinkType type = SupportedSinkType.csv;
  private CSVSinkOptions options;

  @Builder
  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class CSVSinkOptions implements SinkOptions {

    @Default private WriteMode writeMode = WriteMode.append;
    private String path;
    private String input;
    @Default private Boolean hasHeader = false;

    @Default private String encoding = "UTF-8";
    @Default private String dateFormat = "yyyy-MM-dd";
    @Default private String dateTimeFormat = "yyyy-MM-dd HH:mm:ss.SSS";

    @Override
    public Map<String, String> getOptions() {
      Map<String, String> options = new HashMap<>();
      options.put("header", String.valueOf(hasHeader));
      options.put("encoding", encoding);
      options.put("dateFormat", dateFormat);
      options.put("timestampFormat", dateTimeFormat);
      return options;
    }

    @Override
    public void verify() {
      if (StringUtils.isBlank(input)) {
        throw new IllegalArgumentException("input can not be null or empty");
      }
      if (StringUtils.isBlank(path)) {
        throw new IllegalArgumentException("csv sink operator must have file path");
      }
    }
  }
}
