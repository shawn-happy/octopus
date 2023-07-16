package com.octopus.spark.operators.runtime.step.source;

import com.octopus.spark.operators.declare.source.IcebergSourceDeclare;
import com.octopus.spark.operators.declare.source.IcebergSourceDeclare.IcebergSourceOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class IcebergSource extends BaseSource<IcebergSourceDeclare> {

  public IcebergSource(IcebergSourceDeclare declare) {
    super(declare);
  }

  @Override
  protected Dataset<Row> process(SparkSession spark) throws Exception {
    IcebergSourceOptions options = getSourceDeclare().getOptions();
    return spark.read().options(options.getOptions()).table(options.getFullTableName());
  }
}
