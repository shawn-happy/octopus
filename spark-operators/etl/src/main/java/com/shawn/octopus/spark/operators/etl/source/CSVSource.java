package com.shawn.octopus.spark.operators.etl.source;

import com.shawn.octopus.spark.operators.common.ColumnDesc;
import com.shawn.octopus.spark.operators.common.declare.source.CSVSourceDeclare;
import com.shawn.octopus.spark.operators.common.declare.source.CSVSourceDeclare.CSVSourceOptions;
import com.shawn.octopus.spark.operators.etl.ETLUtils;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CSVSource extends BaseSource<CSVSourceDeclare> {

  private final CSVSourceDeclare declare;

  public CSVSource(CSVSourceDeclare declare) {
    this.declare = declare;
  }

  @Override
  public CSVSourceDeclare getDeclare() {
    return declare;
  }

  @Override
  protected Dataset<Row> process(SparkSession spark) throws Exception {
    CSVSourceOptions sourceOptions = declare.getOptions();
    String[] paths = sourceOptions.getPaths();
    StructType schema = getSchema();
    Map<String, String> options = sourceOptions.getOptions();
    if (schema == null || schema.isEmpty()) {
      return spark.read().options(options).csv(paths);
    }
    return spark.read().options(options).schema(schema).csv(paths);
  }

  private StructType getSchema() {
    CSVSourceOptions sourceOptions = declare.getOptions();
    List<ColumnDesc> schemas = sourceOptions.getSchemas();
    if (CollectionUtils.isEmpty(schemas)) {
      return null;
    }
    StructField[] structFields = new StructField[schemas.size()];
    for (int i = 0; i < schemas.size(); i++) {
      ColumnDesc schemaTerm = schemas.get(i);
      structFields[i] =
          new StructField(
              schemaTerm.getName(),
              ETLUtils.toDataTypes(schemaTerm.getType().name()),
              schemaTerm.isNullable(),
              Metadata.empty());
    }
    return new StructType(structFields);
  }
}
