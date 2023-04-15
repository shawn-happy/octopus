package com.octopus.spark.operators.runtime.step.transform.metrics;

import com.octopus.spark.operators.declare.transform.TransformDeclare;
import com.octopus.spark.operators.declare.transform.TransformOptions;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface Metrics<MD extends TransformDeclare<? extends TransformOptions>> {

  Object calculate(SparkSession spark, Map<String, Dataset<Row>> dfs) throws Exception;
}
