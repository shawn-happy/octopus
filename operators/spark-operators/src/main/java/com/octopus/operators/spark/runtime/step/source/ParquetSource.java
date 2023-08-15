package com.octopus.operators.spark.runtime.step.source;

import com.octopus.operators.spark.declare.source.ParquetSourceDeclare;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ParquetSource extends BaseSource<ParquetSourceDeclare> {
  public ParquetSource(ParquetSourceDeclare parquetSourceDeclare) {
    super(parquetSourceDeclare);
  }

  @Override
  protected Dataset<Row> process(SparkSession spark) throws Exception {
    ParquetSourceDeclare.ParquetSourceOptions sourceOptions = getSourceDeclare().getOptions();
    String[] paths = sourceOptions.getPaths();
    Map<String, String> options = sourceOptions.getOptions();
    return spark.read().options(options).parquet(paths);
  }
}
