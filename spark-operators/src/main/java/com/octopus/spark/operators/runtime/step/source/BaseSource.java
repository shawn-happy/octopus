package com.octopus.spark.operators.runtime.step.source;

import com.octopus.spark.operators.declare.source.SourceDeclare;
import com.octopus.spark.operators.declare.source.SourceOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public abstract class BaseSource<SD extends SourceDeclare<?>> implements Source<SD> {

  private final SD sourceDeclare;

  protected BaseSource(SD sd) {
    this.sourceDeclare = sd;
  }

  @Override
  public Dataset<Row> input(SparkSession spark) throws Exception {
    SourceOptions sourceOptions = sourceDeclare.getOptions();
    sourceOptions.verify();
    return process(spark);
  }

  protected abstract Dataset<Row> process(SparkSession spark) throws Exception;

  protected SD getSourceDeclare() {
    return sourceDeclare;
  }
}
