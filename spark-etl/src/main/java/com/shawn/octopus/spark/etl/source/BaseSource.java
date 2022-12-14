package com.shawn.octopus.spark.etl.source;

import com.shawn.octopus.spark.etl.core.model.BaseStep;
import com.shawn.octopus.spark.etl.core.model.ETLContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public abstract class BaseSource extends BaseStep implements Source {

  public BaseSource(String name, SourceOptions config) {
    super(name, config);
  }

  @Override
  public void process(ETLContext context) {
    SourceOptions config = (SourceOptions) getConfig();
    Dataset<Row> df = read(context);
    String output = config.output();
    if (config.getRePartition() != null && config.getRePartition() > 0) {
      df.repartition(config.getRePartition());
    }
    context.setDataFrame(output, df);
  }
}
