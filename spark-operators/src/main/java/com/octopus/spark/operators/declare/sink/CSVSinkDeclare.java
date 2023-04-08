package com.octopus.spark.operators.declare.sink;

import com.octopus.spark.operators.declare.common.SupportedSinkType;
import com.octopus.spark.operators.declare.common.WriteMode;
import com.octopus.spark.operators.declare.sink.CSVSinkDeclare.CSVSinkOptions;
import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.shaded.com.google.common.base.Verify;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class CSVSinkDeclare implements SinkDeclare<CSVSinkOptions> {

  @Default private final SupportedSinkType type = SupportedSinkType.csv;
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
      Verify.verify(StringUtils.isBlank(path), "csv sink operator must have file path");
    }
  }
}
