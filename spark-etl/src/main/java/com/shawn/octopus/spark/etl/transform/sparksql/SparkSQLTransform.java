package com.shawn.octopus.spark.etl.transform.sparksql;

import com.shawn.octopus.spark.etl.core.step.StepContext;
import com.shawn.octopus.spark.etl.transform.Transform;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQLTransform implements Transform {

  private final String name;
  private final SparkSQLTransformOptions options;

  public SparkSQLTransform(String name, SparkSQLTransformOptions options) {
    this.name = name;
    this.options = options;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void process(StepContext context) {
    SparkSession sparkSession = context.getSparkSession();
    SparkSQLTransformOptions config = getConfig();
    Dataset<Row> sql = sparkSession.sql(config.getSql());
    context.setDataFrame("", sql);
  }

  @Override
  public SparkSQLTransformOptions getConfig() {
    return options;
  }

  @Override
  public Dataset<Row> processRow() {
    return null;
  }
}
