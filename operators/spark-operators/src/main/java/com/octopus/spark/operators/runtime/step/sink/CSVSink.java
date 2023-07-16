package com.octopus.spark.operators.runtime.step.sink;

import com.octopus.spark.operators.declare.common.WriteMode;
import com.octopus.spark.operators.declare.sink.CSVSinkDeclare;
import com.octopus.spark.operators.declare.sink.CSVSinkDeclare.CSVSinkOptions;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class CSVSink extends BaseSink<CSVSinkDeclare> {

  public CSVSink(CSVSinkDeclare declare) {
    super(declare);
  }

  @Override
  protected void process(SparkSession spark, Dataset<Row> df) throws Exception {
    CSVSinkOptions sinkOptions = declare.getOptions();
    Map<String, String> options = sinkOptions.getOptions();
    df.write().mode(getSaveMode(declare)).options(options).csv(sinkOptions.getPath());
  }

  private SaveMode getSaveMode(CSVSinkDeclare declare) {
    WriteMode writeMode = declare.getWriteMode();
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
