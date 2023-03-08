package com.shawn.octopus.spark.operators.common.declare.transform;

import com.shawn.octopus.spark.operators.common.SupportedTransformType;
import com.shawn.octopus.spark.operators.common.declare.transform.SparkSQLTransformDeclare.SparkSQLTransformOptions;
import java.util.Collections;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class SparkSQLTransformDeclare implements TransformDeclare<SparkSQLTransformOptions> {

  private SupportedTransformType type = SupportedTransformType.sparkSQL;
  private SparkSQLTransformOptions options;

  @Builder
  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class SparkSQLTransformOptions implements TransformOptions {
    @Setter private Map<String, String> input;
    private String output;
    private Integer repartition;
    private String sql;

    @Override
    public Map<String, String> getOptions() {
      return Collections.emptyMap();
    }

    @Override
    public void verify() {
      if (MapUtils.isEmpty(input)) {
        throw new IllegalArgumentException("input can not be empty or null in transform operators");
      }
      if (StringUtils.isEmpty(output)) {
        throw new IllegalArgumentException(
            "output can not be empty or null in transform operators");
      }
      if (StringUtils.isEmpty(sql)) {
        throw new IllegalArgumentException(
            "sql can not be empty or null in sparkSQL transform operators");
      }
    }
  }
}
