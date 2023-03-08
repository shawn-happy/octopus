package com.shawn.octopus.spark.operators.etl.transform;

import com.shawn.octopus.spark.operators.common.declare.transform.SparkSQLTransformDeclare;
import com.shawn.octopus.spark.operators.common.declare.transform.SparkSQLTransformDeclare.SparkSQLTransformOptions;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQLTransform extends BaseTransform<SparkSQLTransformDeclare> {

  private final SparkSQLTransformDeclare declare;

  public SparkSQLTransform(SparkSQLTransformDeclare declare) {
    this.declare = declare;
  }

  @Override
  protected Dataset<Row> process(SparkSession spark, Map<String, Dataset<Row>> dfs)
      throws Exception {
    SparkSQLTransformOptions transformOptions = declare.getOptions();
    return spark.sql(transformOptions.getSql());
  }

  @Override
  public SparkSQLTransformDeclare getDeclare() {
    return declare;
  }
}
