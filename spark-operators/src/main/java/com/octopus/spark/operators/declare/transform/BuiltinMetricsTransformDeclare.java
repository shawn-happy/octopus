package com.octopus.spark.operators.declare.transform;

import com.octopus.spark.operators.declare.common.SupportedTransformType;
import com.octopus.spark.operators.declare.transform.BuiltinMetricsTransformDeclare.BuiltinMetricsTransformOptions;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.hadoop.shaded.com.google.common.base.Verify;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class BuiltinMetricsTransformDeclare
    implements MetricsDeclare<BuiltinMetricsTransformOptions> {

  @Default private final SupportedTransformType type = SupportedTransformType.metrics;
  private BuiltinMetricsTransformOptions options;
  private String name;
  private Map<String, String> input;
  private String output;
  private Integer repartition;

  @Builder
  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class BuiltinMetricsTransformOptions implements MetricsOptions {
    private List<String> columns;
    private BuiltinMetricsOpType opType;

    @Override
    public Map<String, String> getOptions() {
      return Collections.emptyMap();
    }

    @Override
    public void verify() {
      Verify.verify(
          CollectionUtils.isEmpty(columns), "columns can not be empty or null in builtin metrics");
      Verify.verify(
          ObjectUtils.isEmpty(opType), "opType can not be empty or null in builtin metrics");
    }
  }
}
