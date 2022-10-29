package com.shawn.octopus.spark.etl.sink;

import com.shawn.octopus.spark.etl.core.api.StepConfiguration;
import com.shawn.octopus.spark.etl.core.model.BaseStep;
import com.shawn.octopus.spark.etl.core.model.ETLContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public abstract class BaseSink extends BaseStep implements Sink {

  public BaseSink(String name, StepConfiguration config) {
    super(name, config);
  }

  @Override
  public void process(ETLContext context) {
    SinkOptions options = (SinkOptions) getConfig();
    String input = options.input();
    Dataset<Row> dataFrame = context.getDataFrame(input);
    write(dataFrame);
  }
}
