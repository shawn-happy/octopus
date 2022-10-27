package com.shawn.octopus.spark.etl.source.file.csv;

import static com.shawn.octopus.spark.etl.core.util.ETLUtils.columnDescToSchema;

import com.shawn.octopus.spark.etl.core.enums.Format;
import com.shawn.octopus.spark.etl.core.step.StepContext;
import com.shawn.octopus.spark.etl.source.BaseSource;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class CSVSource extends BaseSource {

  public CSVSource(String name, CSVSourceOptions config) {
    super(name, config);
  }

  @Override
  public Format getFormat() {
    return Format.csv;
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public Dataset<Row> read(StepContext context) {
    SparkSession spark = context.getSparkSession();
    CSVSourceOptions config = (CSVSourceOptions) getConfig();
    StructType schema = columnDescToSchema(config.getColumns());
    DataFrameReader reader = spark.read().options(config.getOptions());
    if (schema != null && !schema.isEmpty()) {
      reader.schema(schema);
    }
    return reader.csv(config.getPaths());
  }
}
