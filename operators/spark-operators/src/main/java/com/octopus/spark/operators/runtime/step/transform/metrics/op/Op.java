package com.octopus.spark.operators.runtime.step.transform.metrics.op;

import com.octopus.spark.operators.declare.transform.BuiltinMetricsOpType;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface Op<T> {

  BuiltinMetricsOpType getOpType();

  T process(SparkSession spark, Map<String, Dataset<Row>> dfs, List<String> columns)
      throws Exception;
}
