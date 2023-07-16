package com.octopus.operators.spark.runtime.step.source;

import com.octopus.operators.spark.declare.common.ColumnDesc;
import com.octopus.operators.spark.declare.source.CSVSourceDeclare;
import com.octopus.operators.spark.declare.source.CSVSourceDeclare.CSVSourceOptions;
import com.octopus.operators.spark.utils.SparkOperatorUtils;
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

  public CSVSource(CSVSourceDeclare csvSourceDeclare) {
    super(csvSourceDeclare);
  }

  @Override
  protected Dataset<Row> process(SparkSession spark) throws Exception {
    CSVSourceOptions sourceOptions = getSourceDeclare().getOptions();
    String[] paths = sourceOptions.getPaths();
    StructType schema = getSchema(sourceOptions);
    Map<String, String> options = sourceOptions.getOptions();
    if (schema == null || schema.isEmpty()) {
      return spark.read().options(options).csv(paths);
    }
    return spark.read().options(options).schema(schema).csv(paths);
  }

  private StructType getSchema(CSVSourceOptions sourceOptions) {
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
              SparkOperatorUtils.toDataTypes(schemaTerm.getType().name()),
              schemaTerm.isNullable(),
              Metadata.empty());
    }
    return new StructType(structFields);
  }
}
