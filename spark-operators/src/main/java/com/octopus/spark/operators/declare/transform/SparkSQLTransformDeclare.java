package com.octopus.spark.operators.declare.transform;

import com.octopus.spark.operators.declare.common.TransformType;
import com.octopus.spark.operators.declare.transform.SparkSQLTransformDeclare.SparkSQLTransformOptions;
import java.util.Collections;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.shaded.com.google.common.base.Verify;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class SparkSQLTransformDeclare implements TransformDeclare<SparkSQLTransformOptions> {

  @Default private TransformType type = TransformType.sparkSQL;
  private SparkSQLTransformOptions options;
  private String name;
  @Setter private Map<String, String> input;
  private String output;
  private Integer repartition;

  @Builder
  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class SparkSQLTransformOptions implements TransformOptions {
    private String sql;

    @Override
    public Map<String, String> getOptions() {
      return Collections.emptyMap();
    }

    @Override
    public void verify() {
      Verify.verify(StringUtils.isEmpty(sql), "sql can not be empty or null in sparkSQL transform");
    }
  }
}
