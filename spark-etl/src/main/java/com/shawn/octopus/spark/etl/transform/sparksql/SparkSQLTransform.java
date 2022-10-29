package com.shawn.octopus.spark.etl.transform.sparksql;

import com.shawn.octopus.spark.etl.core.enums.TransformType;
import com.shawn.octopus.spark.etl.core.model.ETLContext;
import com.shawn.octopus.spark.etl.transform.BaseTransform;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQLTransform extends BaseTransform {

  public SparkSQLTransform(String name, SparkSQLTransformOptions options) {
    super(name, options);
  }

  @Override
  public TransformType getTransformType() {
    return TransformType.SPARK_SQL;
  }

  @Override
  public Dataset<Row> trans(ETLContext context) {
    SparkSession sparkSession = context.getSparkSession();
    SparkSQLTransformOptions config = (SparkSQLTransformOptions) getConfig();
    return sparkSession.sql(config.getSql());
  }
}
