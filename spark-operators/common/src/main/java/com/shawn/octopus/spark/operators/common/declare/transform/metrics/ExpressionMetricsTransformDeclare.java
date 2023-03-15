package com.shawn.octopus.spark.operators.common.declare.transform.metrics;

import com.shawn.octopus.spark.operators.common.SupportedTransformType;
import com.shawn.octopus.spark.operators.common.declare.transform.TransformOptions;
import com.shawn.octopus.spark.operators.common.declare.transform.metrics.ExpressionMetricsTransformDeclare.ExpressionMetricsTransformOptions;
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
public class ExpressionMetricsTransformDeclare
    implements MetricsTransformDeclare<ExpressionMetricsTransformOptions> {

  @Default private final MetricsType metricsType = MetricsType.expression;
  @Default private final SupportedTransformType type = SupportedTransformType.metrics;
  private ExpressionMetricsTransformOptions options;
  private String name;

  @Builder
  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class ExpressionMetricsTransformOptions implements TransformOptions {
    private Map<String, String> input;
    private String output;
    private Integer repartition;
    private String expression;

    @Override
    public Map<String, String> getOptions() {
      return null;
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
      if (StringUtils.isEmpty(expression)) {
        throw new IllegalArgumentException(
            "expression can not be empty or null in expression metrics operators");
      }
    }
  }
}
