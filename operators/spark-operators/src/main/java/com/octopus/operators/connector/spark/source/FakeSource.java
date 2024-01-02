package com.octopus.operators.connector.spark.source;

import com.octopus.operators.connector.spark.DatasetTableInfo;
import com.octopus.operators.connector.spark.SparkAbstractExecuteProcessor;
import com.octopus.operators.connector.spark.SparkDataTypeParser;
import com.octopus.operators.connector.spark.SparkJobContext;
import com.octopus.operators.connector.spark.SparkRuntimeEnvironment;
import com.octopus.operators.engine.connector.source.Source;
import com.octopus.operators.engine.connector.source.fake.FakeDataGenerator;
import com.octopus.operators.engine.connector.source.fake.FakeSourceConfig;
import com.octopus.operators.engine.connector.source.fake.FakeSourceConfig.FakeSourceOptions;
import com.octopus.operators.engine.connector.source.fake.FakeSourceConfig.FakeSourceRow;
import com.octopus.operators.engine.table.type.RowDataType;
import com.octopus.operators.engine.table.type.RowDataTypeParse;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class FakeSource extends SparkAbstractExecuteProcessor
    implements Source<SparkRuntimeEnvironment, FakeSourceConfig, DatasetTableInfo> {

  private final FakeSourceConfig config;
  private final FakeSourceOptions options;

  public FakeSource(
      SparkRuntimeEnvironment sparkRuntimeEnvironment,
      SparkJobContext jobContext,
      FakeSourceConfig config) {
    super(sparkRuntimeEnvironment, jobContext);
    this.config = config;
    this.options = config.getOptions();
  }

  @Override
  public DatasetTableInfo read() {
    SparkSession sparkSession = jobContext.getSparkSession();
    FakeDataGenerator fakeDataGenerator = new FakeDataGenerator(options);
    FakeSourceRow[] fields = options.getFields();
    String[] fieldNames = new String[fields.length];
    RowDataType[] fieldTypes = new RowDataType[fields.length];
    for (int i = 0; i < fields.length; i++) {
      String fieldName = fields[i].getFieldName();
      fieldNames[i] = fieldName;
      String fieldType = fields[i].getFieldType();
      fieldTypes[i] = RowDataTypeParse.parseDataType(fieldType);
    }
    StructField[] structFields = new StructField[fieldNames.length];
    for (int i = 0; i < fieldNames.length; i++) {
      StructField structField =
          DataTypes.createStructField(
              fieldNames[i], SparkDataTypeParser.toDataType(fieldTypes[i]), true);
      structFields[i] = structField;
    }
    StructType structType = new StructType(structFields);

    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < options.getRowNum(); i++) {
      Object[] fieldValues = fakeDataGenerator.random();
      Row row = RowFactory.create(fieldValues);
      rows.add(row);
    }

    Dataset<Row> df = sparkSession.createDataFrame(rows, structType);
    jobContext.registerTable(config.getOutput(), df);
    return DatasetTableInfo.of(config.getOutput(), df);
  }
}
