package com.octopus.operators.spark.runtime.source;

import com.octopus.operators.engine.connector.source.Source;
import com.octopus.operators.engine.connector.source.fake.FakeSourceConfig;
import com.octopus.operators.spark.runtime.DatasetTableInfo;
import com.octopus.operators.spark.runtime.SparkAbstractExecuteProcessor;
import com.octopus.operators.spark.runtime.SparkJobContext;
import com.octopus.operators.spark.runtime.SparkRuntimeEnvironment;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class FakeSource extends SparkAbstractExecuteProcessor
    implements Source<DatasetTableInfo, FakeSourceConfig, SparkRuntimeEnvironment> {

  private FakeSourceConfig config;

  public FakeSource(
      SparkRuntimeEnvironment sparkRuntimeEnvironment,
      SparkJobContext jobContext,
      FakeSourceConfig config) {
    super(sparkRuntimeEnvironment, jobContext);
    this.config = config;
  }

  @Override
  public DatasetTableInfo read() {
    SparkSession sparkSession = jobContext.getSparkSession();
    StructField[] structFields = new StructField[1];
    var dataType = DataTypes.IntegerType;
    String column = "num";
    StructField structField = new StructField(column, dataType, true, Metadata.empty());
    structFields[0] = structField;

    StructType structType = new StructType(structFields);

    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      Object[] objects = new Object[structFields.length];
      objects[0] = i + 1;
      Row row = RowFactory.create(objects);
      rows.add(row);
    }

    Dataset<Row> df = sparkSession.createDataFrame(rows, structType);
    jobContext.setDataset("fake", df);
    return new DatasetTableInfo(df, "fake");
  }

  @Override
  public void setConfig(FakeSourceConfig config) {
    this.config = config;
  }
}
