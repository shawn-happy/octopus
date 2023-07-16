package com.octopus.operators.spark.runtime.step.transform.metrics;

import com.googlecode.aviator.AviatorEvaluator;
import com.octopus.operators.spark.declare.transform.ExpressionMetricsTransformDeclare;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ExpressionMetrics extends BaseMetrics<ExpressionMetricsTransformDeclare> {

  private final Map<String, Object> metrics;

  public ExpressionMetrics(ExpressionMetricsTransformDeclare declare, Map<String, Object> metrics) {
    super(declare);
    this.metrics = metrics;
  }

  @Override
  protected Object doMetrics(SparkSession spark, Map<String, Dataset<Row>> dfs) throws Exception {
    // expression: "avg_age>=20", metrics: Map.of("avg_age", 19)
    // expression: "avg_age+min_age", metrics: Map.of("avg_age", 19, "min_age", 8)
    return AviatorEvaluator.execute(declare.getOptions().getExpression(), metrics);
  }
}
