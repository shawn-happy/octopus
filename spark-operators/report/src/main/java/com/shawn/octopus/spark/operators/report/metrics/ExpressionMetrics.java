package com.shawn.octopus.spark.operators.report.metrics;

import com.googlecode.aviator.AviatorEvaluator;
import com.shawn.octopus.spark.operators.common.declare.transform.metrics.ExpressionMetricsTransformDeclare;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ExpressionMetrics implements Metrics<ExpressionMetricsTransformDeclare> {

  private final ExpressionMetricsTransformDeclare declare;
  private final Map<String, Object> metrics;
  private final String expression;

  public ExpressionMetrics(ExpressionMetricsTransformDeclare declare, Map<String, Object> metrics) {
    this.declare = declare;
    this.metrics = metrics;
    this.expression = declare.getOptions().getExpression();
  }

  @Override
  public Object calculate(SparkSession spark, Map<String, Dataset<Row>> dfs) throws Exception {
    return AviatorEvaluator.execute(expression, metrics);
  }

  @Override
  public ExpressionMetricsTransformDeclare getDeclare() {
    return declare;
  }
}
