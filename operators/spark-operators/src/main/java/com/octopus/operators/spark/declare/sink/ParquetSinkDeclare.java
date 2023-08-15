package com.octopus.operators.spark.declare.sink;

import com.octopus.operators.spark.declare.common.CompressionType;
import com.octopus.operators.spark.declare.common.SinkType;
import com.octopus.operators.spark.declare.common.WriteMode;
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
public class ParquetSinkDeclare implements SinkDeclare<ParquetSinkDeclare.ParquetSinkOptions> {

  @Default private final SinkType type = SinkType.parquet;
  private String name;
  private String input;
  private ParquetSinkOptions options;
  @Default private WriteMode writeMode = WriteMode.replace;

  @Builder
  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class ParquetSinkOptions implements SinkOptions {

    private String path;
    @Default private CompressionType compression = CompressionType.snappy;

    @Override
    public Map<String, String> getOptions() {
      Map<String, String> options = new HashMap<>();
      options.put("compression", compression.name());
      return options;
    }

    @Override
    public void verify() {
      Verify.verify(StringUtils.isNotBlank(path), "parquet sink operator must have file path");
    }
  }
}
