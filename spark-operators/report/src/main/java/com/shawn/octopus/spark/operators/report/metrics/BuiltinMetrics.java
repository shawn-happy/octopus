package com.shawn.octopus.spark.operators.report.metrics;

import com.shawn.octopus.spark.operators.common.declare.transform.metrics.BuiltinMetricsOpType;
import com.shawn.octopus.spark.operators.common.declare.transform.metrics.BuiltinMetricsTransformDeclare;
import com.shawn.octopus.spark.operators.common.declare.transform.metrics.BuiltinMetricsTransformDeclare.BuiltinMetricsTransformOptions;
import com.shawn.octopus.spark.operators.common.declare.transform.metrics.MetricsType;
import com.shawn.octopus.spark.operators.report.metrics.op.Op;
import com.shawn.octopus.spark.operators.report.registry.OpRegistry;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class BuiltinMetrics implements Metrics<BuiltinMetricsTransformDeclare> {

  private final BuiltinMetricsTransformDeclare declare;

  public BuiltinMetrics(BuiltinMetricsTransformDeclare declare) {
    this.declare = declare;
    BuiltinMetricsTransformOptions options = declare.getOptions();
  }

  @Override
  public Object calculate(SparkSession spark, Map<String, Dataset<Row>> dfs, List<String> columns)
      throws Exception {
    MetricsType metricsType = declare.getMetricsType();
    BuiltinMetricsOpType opType = declare.getOptions().getOpType();
    Op<?> op = OpRegistry.OP_REGISTRY.get(opType);
    return op.process(spark, dfs, columns);
  }

  @Override
  public BuiltinMetricsTransformDeclare getDeclare() {
    return declare;
  }
}
