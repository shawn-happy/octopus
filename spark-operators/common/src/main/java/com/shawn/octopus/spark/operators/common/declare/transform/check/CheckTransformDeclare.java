package com.shawn.octopus.spark.operators.common.declare.transform.check;

import com.shawn.octopus.spark.operators.common.SupportedTransformType;
import com.shawn.octopus.spark.operators.common.declare.transform.TransformDeclare;
import com.shawn.octopus.spark.operators.common.declare.transform.TransformOptions;
import com.shawn.octopus.spark.operators.common.declare.transform.check.CheckTransformDeclare.CheckTransformOptions;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class CheckTransformDeclare implements TransformDeclare<CheckTransformOptions> {

  @Default private final SupportedTransformType type = SupportedTransformType.check;
  private CheckTransformOptions options;
  private String name;

  @Builder
  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class CheckTransformOptions implements TransformOptions {
    private Map<String, String> input;
    private String output;
    private Integer repartition;
    private String expression;
    private CheckLevel level;

    @Override
    public Map<String, String> getOptions() {
      return null;
    }

    @Override
    public void verify() {
      if (MapUtils.isEmpty(input)) {
        throw new IllegalArgumentException("input can not be empty or null in transform operators");
      }

      if (StringUtils.isEmpty(expression)) {
        throw new IllegalArgumentException(
            "sql can not be empty or null in check transform operators");
      }
    }
  }
}
