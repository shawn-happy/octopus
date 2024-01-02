package com.octopus.operators.connector.spark.source;

import com.octopus.operators.connector.spark.DatasetTableInfo;
import com.octopus.operators.connector.spark.SparkAbstractExecuteProcessor;
import com.octopus.operators.connector.spark.SparkDataTypeParser;
import com.octopus.operators.connector.spark.SparkJobContext;
import com.octopus.operators.connector.spark.SparkRuntimeEnvironment;
import com.octopus.operators.engine.connector.source.Source;
import com.octopus.operators.engine.connector.source.csv.CSVSourceConfig;
import com.octopus.operators.engine.connector.source.csv.CSVSourceConfig.CSVSourceOptions;
import com.octopus.operators.engine.table.catalog.Column;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CSVSource extends SparkAbstractExecuteProcessor
    implements Source<SparkRuntimeEnvironment, CSVSourceConfig, DatasetTableInfo> {

  private final CSVSourceConfig config;
  private final CSVSourceOptions options;

  public CSVSource(
      SparkRuntimeEnvironment sparkRuntimeEnvironment,
      SparkJobContext jobContext,
      CSVSourceConfig config) {
    super(sparkRuntimeEnvironment, jobContext);
    this.config = config;
    this.options = config.getOptions();
  }

  @Override
  public DatasetTableInfo read() {
    SparkSession sparkSession = jobContext.getSparkSession();
    StructType schema = getSchema(options.getSchemas());
    Dataset<Row> csv =
        sparkSession.read().options(options()).schema(schema).csv(options.getPaths());
    jobContext.registerTable(config.getOutput(), csv);
    return DatasetTableInfo.of(config.getOutput(), csv);
  }

  private StructType getSchema(List<Column> columns) {
    if (CollectionUtils.isEmpty(columns)) {
      return null;
    }
    StructField[] structFields = new StructField[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      Column column = columns.get(i);
      structFields[i] =
          new StructField(
              column.getName(),
              SparkDataTypeParser.toDataType(column.getRowDataType()),
              column.isNullable(),
              Metadata.empty());
    }
    return new StructType(structFields);
  }

  private Map<String, String> options() {
    Map<String, String> map = new HashMap<>();
    map.put("recursiveFileLookup", String.valueOf(true));
    map.put("encoding", options.getEncoding());
    map.put("header", String.valueOf(options.isHeader()));
    map.put("dateFormat", options.getDateFormat());
    map.put("timestampFormat", options.getDateTimeFormat());
    if (CollectionUtils.isEmpty(options.getSchemas())) {
      map.put("inferSchema", String.valueOf(true));
    }
    return map;
  }
}
