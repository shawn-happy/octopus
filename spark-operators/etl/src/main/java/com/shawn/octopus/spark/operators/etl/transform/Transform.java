package com.shawn.octopus.spark.operators.etl.transform;

import com.shawn.octopus.spark.operators.common.declare.transform.TransformDeclare;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface Transform<TD extends TransformDeclare<?>> {

  TD getDeclare();

  Dataset<Row> trans(SparkSession spark, Map<String, Dataset<Row>> dfs) throws Exception;
}
