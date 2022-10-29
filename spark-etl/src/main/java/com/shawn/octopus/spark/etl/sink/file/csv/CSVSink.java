package com.shawn.octopus.spark.etl.sink.file.csv;

import com.shawn.octopus.spark.etl.core.enums.SinkType;
import com.shawn.octopus.spark.etl.core.enums.WriteMode;
import com.shawn.octopus.spark.etl.sink.BaseSink;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

public class CSVSink extends BaseSink {

  public CSVSink(String name, CSVSinkOptions config) {
    super(name, config);
  }

  @Override
  public SinkType getSinkType() {
    return SinkType.csv;
  }

  @Override
  public void write(Dataset<Row> df) {
    CSVSinkOptions options = (CSVSinkOptions) getConfig();
    WriteMode writeMode = options.getWriteMode();
    SaveMode saveMode;
    switch (writeMode) {
      case append:
        saveMode = SaveMode.Append;
        break;
      case replace:
        saveMode = SaveMode.Overwrite;
        break;
      default:
        throw new IllegalArgumentException("unsupported write mode: " + options.getWriteMode());
    }
    df.write().mode(saveMode).options(options.getOptions()).csv(options.getFilePath());
  }
}
