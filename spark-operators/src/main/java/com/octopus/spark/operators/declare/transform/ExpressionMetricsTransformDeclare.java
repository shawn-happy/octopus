package com.octopus.spark.operators.declare.transform;

import com.octopus.spark.operators.declare.common.SupportedTransformType;
import com.octopus.spark.operators.declare.transform.ExpressionMetricsTransformDeclare.ExpressionMetricsTransformOptions;
import java.util.Collections;
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
public class ExpressionMetricsTransformDeclare
    implements MetricsDeclare<ExpressionMetricsTransformOptions> {

  @Default private final SupportedTransformType type = SupportedTransformType.expression;
  private ExpressionMetricsTransformOptions options;
  private String name;
  private Map<String, String> input;
  private String output;
  private Integer repartition;

  @Builder
  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class ExpressionMetricsTransformOptions implements MetricsOptions {
    private String expression;

    @Override
    public Map<String, String> getOptions() {
      return Collections.emptyMap();
    }

    @Override
    public void verify() {
      Verify.verify(
          StringUtils.isEmpty(expression),
          "expression can not be empty or null in expression metrics");
    }
  }
}
