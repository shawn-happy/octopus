package com.octopus.spark.operators.runtime.step.transform.metrics;

import com.octopus.spark.operators.declare.transform.BuiltinMetricsOpType;
import com.octopus.spark.operators.declare.transform.BuiltinMetricsTransformDeclare;
import com.octopus.spark.operators.declare.transform.BuiltinMetricsTransformDeclare.BuiltinMetricsTransformOptions;
import com.octopus.spark.operators.registry.OpRegistry;
import com.octopus.spark.operators.runtime.step.transform.metrics.op.Op;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class BuiltinMetrics extends BaseMetrics<BuiltinMetricsTransformDeclare> {

  public BuiltinMetrics(BuiltinMetricsTransformDeclare declare) {
    super(declare);
  }

  @Override
  protected Object doMetrics(SparkSession spark, Map<String, Dataset<Row>> dfs) throws Exception {
    BuiltinMetricsTransformOptions options = declare.getOptions();
    BuiltinMetricsOpType opType = options.getOpType();
    Op<?> op = OpRegistry.OP_REGISTRY.get(opType);
    return op.process(spark, dfs, options.getColumns());
  }
}
