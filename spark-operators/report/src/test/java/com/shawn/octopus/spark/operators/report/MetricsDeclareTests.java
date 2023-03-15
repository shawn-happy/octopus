package com.shawn.octopus.spark.operators.report;

import com.shawn.octopus.spark.operators.common.declare.transform.metrics.BuiltinMetricsOpType;
import com.shawn.octopus.spark.operators.common.declare.transform.metrics.BuiltinMetricsTransformDeclare;
import com.shawn.octopus.spark.operators.common.declare.transform.metrics.BuiltinMetricsTransformDeclare.BuiltinMetricsTransformOptions;
import com.shawn.octopus.spark.operators.etl.SparkOperatorUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MetricsDeclareTests {

  @Test
  public void testBuiltinMetrics() {
    String path =
        Thread.currentThread()
            .getContextClassLoader()
            .getResource("builtin_metrics.yaml")
            .getPath();
    BuiltinMetricsTransformDeclare config =
        SparkOperatorUtils.getConfig(path, BuiltinMetricsTransformDeclare.class);
    Assertions.assertNotNull(config);
    BuiltinMetricsTransformOptions options = config.getOptions();
    Assertions.assertNotNull(options);
    Assertions.assertEquals(BuiltinMetricsOpType.max, options.getOpType());
    Assertions.assertEquals(1, options.getColumns().size());
    Assertions.assertEquals(1, options.getInput().size());
  }
}
