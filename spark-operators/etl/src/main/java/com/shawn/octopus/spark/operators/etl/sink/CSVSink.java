package com.shawn.octopus.spark.operators.etl.sink;

import com.shawn.octopus.spark.operators.common.WriteMode;
import com.shawn.octopus.spark.operators.common.declare.sink.CSVSinkDeclare;
import com.shawn.octopus.spark.operators.common.declare.sink.CSVSinkDeclare.CSVSinkOptions;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class CSVSink extends BaseSink<CSVSinkDeclare> {

  private final CSVSinkDeclare declare;

  public CSVSink(CSVSinkDeclare declare) {
    this.declare = declare;
  }

  @Override
  protected void process(SparkSession spark, Dataset<Row> df) throws Exception {
    CSVSinkOptions sinkOptions = declare.getOptions();
    Map<String, String> options = sinkOptions.getOptions();
    df.write().mode(getSaveMode()).options(options).csv(sinkOptions.getPath());
  }

  @Override
  public CSVSinkDeclare getDeclare() {
    return declare;
  }

  @Override
  public SaveMode getSaveMode() {
    CSVSinkOptions sinkOptions = declare.getOptions();
    WriteMode writeMode = sinkOptions.getWriteMode();
    SaveMode saveMode;
    switch (writeMode) {
      case append:
        saveMode = SaveMode.Append;
        break;
      case replace:
        saveMode = SaveMode.Overwrite;
        break;
      default:
        throw new IllegalArgumentException("unsupported write mode: " + writeMode);
    }
    return saveMode;
  }
}
