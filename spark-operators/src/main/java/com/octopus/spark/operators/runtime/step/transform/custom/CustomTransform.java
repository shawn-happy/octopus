package com.octopus.spark.operators.runtime.step.transform.custom;

import com.octopus.spark.operators.declare.transform.TransformDeclare;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface CustomTransform<TD extends TransformDeclare<?>> {

  Dataset<Row> trans(SparkSession spark, Map<String, Dataset<Row>> dfs) throws Exception;
}
