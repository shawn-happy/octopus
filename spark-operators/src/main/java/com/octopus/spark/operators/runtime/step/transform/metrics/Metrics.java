package com.octopus.spark.operators.runtime.step.transform.metrics;

import com.octopus.spark.operators.declare.transform.MetricsDeclare;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface Metrics<MD extends MetricsDeclare<?>> {

  Object calculate(SparkSession spark, Map<String, Dataset<Row>> dfs) throws Exception;
}
