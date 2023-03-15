package com.shawn.octopus.spark.operators.report.metrics;

import com.shawn.octopus.spark.operators.common.declare.transform.metrics.MetricsTransformDeclare;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface Metrics<MD extends MetricsTransformDeclare<?>> {

  Object calculate(SparkSession spark, Map<String, Dataset<Row>> dfs) throws Exception;

  MD getDeclare();
}
