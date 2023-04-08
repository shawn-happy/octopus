package com.octopus.spark.operators.runtime.step.transform.metrics;

import com.googlecode.aviator.AviatorEvaluator;
import com.octopus.spark.operators.declare.transform.ExpressionMetricsTransformDeclare;
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
    return AviatorEvaluator.execute(declare.getOptions().getExpression(), metrics);
  }
}
