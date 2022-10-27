package com.shawn.octopus.spark.etl.source.file.parquet;

import com.shawn.octopus.spark.etl.core.enums.Format;
import com.shawn.octopus.spark.etl.core.step.StepContext;
import com.shawn.octopus.spark.etl.source.BaseSource;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ParquetSource extends BaseSource {

  public ParquetSource(String name, ParquetSourceOptions config) {
    super(name, config);
  }

  @Override
  public Format getFormat() {
    return Format.parquet;
  }

  @Override
  public Dataset<Row> read(StepContext context) {
    SparkSession spark = context.getSparkSession();
    ParquetSourceOptions config = (ParquetSourceOptions) getConfig();
    return spark.read().options(config.getOptions()).parquet(config.getPaths());
  }
}
