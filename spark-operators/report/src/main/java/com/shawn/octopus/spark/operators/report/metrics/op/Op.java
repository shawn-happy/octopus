package com.shawn.octopus.spark.operators.report.metrics.op;

import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface Op<T> {

  T process(SparkSession spark, Map<String, Dataset<Row>> dfs, List<String> columns)
      throws Exception;
}
