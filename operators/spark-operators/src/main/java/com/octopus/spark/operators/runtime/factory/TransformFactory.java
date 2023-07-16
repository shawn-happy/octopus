package com.octopus.spark.operators.runtime.factory;

import com.octopus.spark.operators.declare.common.TransformType;
import com.octopus.spark.operators.declare.transform.BuiltinMetricsTransformDeclare;
import com.octopus.spark.operators.declare.transform.ExpressionMetricsTransformDeclare;
import com.octopus.spark.operators.declare.transform.MetricsDeclare;
import com.octopus.spark.operators.declare.transform.SparkSQLTransformDeclare;
import com.octopus.spark.operators.declare.transform.TransformDeclare;
import com.octopus.spark.operators.runtime.step.transform.custom.CustomTransform;
import com.octopus.spark.operators.runtime.step.transform.custom.SparkSQLTransform;
import com.octopus.spark.operators.runtime.step.transform.metrics.BuiltinMetrics;
import com.octopus.spark.operators.runtime.step.transform.metrics.ExpressionMetrics;
import com.octopus.spark.operators.runtime.step.transform.metrics.Metrics;
import com.octopus.spark.operators.runtime.step.transform.metrics.SparkSQLMetric;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TransformFactory {

  public static CustomTransform<?> createETLTransform(TransformDeclare<?> transformDeclare) {
    TransformType type = transformDeclare.getType();
    CustomTransform<?> transform = null;
    switch (type) {
      case sparkSQL:
        transform = new SparkSQLTransform((SparkSQLTransformDeclare) transformDeclare);
        break;
      default:
        throw new IllegalArgumentException("unsupported transform type: " + type);
    }
    return transform;
  }

  public static Metrics<?> createMetrics(
      MetricsDeclare<?> metricsDeclare, Map<String, Object> indicators) {
    TransformType type = metricsDeclare.getType();
    Metrics<?> metrics = null;
    switch (type) {
      case metrics:
        metrics = new BuiltinMetrics((BuiltinMetricsTransformDeclare) metricsDeclare);
        break;
      case expression:
        Map<String, Object> inputMetrics = new LinkedHashMap<>();
        metricsDeclare
            .getInput()
            .forEach(
                (k, v) -> {
                  if (!indicators.containsKey(k)) {
                    log.error("input metric [{}] not found.", k);
                    throw new RuntimeException("input metric not found.");
                  }
                  inputMetrics.put(k, indicators.get(k));
                });
        metrics =
            new ExpressionMetrics((ExpressionMetricsTransformDeclare) metricsDeclare, inputMetrics);
        break;
      case sparkSQL:
        metrics = new SparkSQLMetric((SparkSQLTransformDeclare) metricsDeclare);
        break;
      default:
        throw new IllegalArgumentException("unsupported transform type: " + type);
    }
    return metrics;
  }
}
