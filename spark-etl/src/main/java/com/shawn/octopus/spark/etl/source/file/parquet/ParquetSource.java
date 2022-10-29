package com.shawn.octopus.spark.etl.source.file.parquet;

import com.shawn.octopus.spark.etl.core.enums.SourceType;
import com.shawn.octopus.spark.etl.core.model.ETLContext;
import com.shawn.octopus.spark.etl.source.BaseSource;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ParquetSource extends BaseSource {

  public ParquetSource(String name, ParquetSourceOptions config) {
    super(name, config);
  }

  @Override
  public SourceType getFormat() {
    return SourceType.parquet;
  }

  @Override
  public Dataset<Row> read(ETLContext context) {
    SparkSession spark = context.getSparkSession();
    ParquetSourceOptions config = (ParquetSourceOptions) getConfig();
    return spark.read().options(config.getOptions()).parquet(config.getPaths());
  }
}
