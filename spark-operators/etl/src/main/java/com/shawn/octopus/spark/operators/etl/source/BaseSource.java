package com.shawn.octopus.spark.operators.etl.source;

import com.shawn.octopus.spark.operators.common.declare.source.SourceDeclare;
import com.shawn.octopus.spark.operators.common.declare.source.SourceOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public abstract class BaseSource<SD extends SourceDeclare<?>> implements Source<SD> {

  @Override
  public Dataset<Row> input(SparkSession spark) throws Exception {
    SourceOptions sourceOptions = getDeclare().getOptions();
    sourceOptions.verify();
    return process(spark);
  }

  protected abstract Dataset<Row> process(SparkSession spark) throws Exception;
}
