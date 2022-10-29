package com.shawn.octopus.spark.etl.transform;

import com.shawn.octopus.spark.etl.core.model.BaseStep;
import com.shawn.octopus.spark.etl.core.model.ETLContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public abstract class BaseTransform extends BaseStep implements Transform {

  public BaseTransform(String name, TransformOptions config) {
    super(name, config);
  }

  @Override
  public void process(ETLContext context) {
    TransformOptions config = (TransformOptions) getConfig();
    Dataset<Row> df = trans(context);
    String output = config.output();
    if (config.getRePartition() != null && config.getRePartition() > 0) {
      df.repartition(config.getRePartition());
    }
    context.setDataFrame(output, df);
  }
}
