package com.octopus.operators.spark.runtime.step.sink;

import com.octopus.operators.spark.declare.common.WriteMode;
import com.octopus.operators.spark.declare.sink.ParquetSinkDeclare;
import com.octopus.operators.spark.declare.sink.ParquetSinkDeclare.ParquetSinkOptions;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class ParquetSink extends BaseSink<ParquetSinkDeclare> {

  public ParquetSink(ParquetSinkDeclare declare) {
    super(declare);
  }

  @Override
  protected void process(SparkSession spark, Dataset<Row> df) throws Exception {
    ParquetSinkOptions sinkOptions = declare.getOptions();
    Map<String, String> options = sinkOptions.getOptions();
    df.write().mode(getSaveMode(declare)).options(options).parquet(sinkOptions.getPath());
  }

  private SaveMode getSaveMode(ParquetSinkDeclare declare) {
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
