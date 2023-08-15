package com.octopus.operators.spark.declare.source;

import com.octopus.operators.spark.declare.common.SourceType;
import com.octopus.operators.spark.declare.source.ParquetSourceDeclare.ParquetSourceOptions;
import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.shaded.com.google.common.base.Verify;
import org.jetbrains.annotations.NotNull;

@Builder
@Getter
@NoArgsConstructor(force = true)
@AllArgsConstructor
public class ParquetSourceDeclare implements SourceDeclare<ParquetSourceOptions> {
  @Default private SourceType type = SourceType.parquet;
  private ParquetSourceOptions options;
  @NotNull private String name;
  @NotNull private String output;
  private Integer repartition;

  @Builder
  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class ParquetSourceOptions implements SourceOptions {
    private String[] paths;

    @Override
    public Map<String, String> getOptions() {
      return new HashMap<>();
    }

    @Override
    public void verify() {
      Verify.verify(ArrayUtils.isNotEmpty(paths), "paths can not be empty or null");
    }
  }
}
