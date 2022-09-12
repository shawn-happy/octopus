package com.shawn.octopus.spark.etl.source.file;

import com.shawn.octopus.spark.etl.core.BaseStep;
import com.shawn.octopus.spark.etl.core.Output;
import com.shawn.octopus.spark.etl.core.StepContext;
import com.shawn.octopus.spark.etl.source.Source;
import java.util.List;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public abstract class FileSource extends BaseStep implements Source {

  public FileSource(StepContext context, FileSourceConfig config) {
    super(context, config);
  }

  @Override
  public void processRow() {
    StepContext context = getContext();
    FileSourceConfig config = (FileSourceConfig) getConfig();
    SparkSession spark = context.getSparkSession();
    Dataset<Row> df =
        spark.read().format(config.getType()).options(config.getOptions()).load(config.getPaths());
    List<Output> outputs = config.getOutputs();
    for (Output output : outputs) {
      String alias = output.getAlias();
      String[] selectExprs = output.getSelectExprs();
      if (ArrayUtils.isEmpty(selectExprs)) {
        context.setDataFrame(alias, df);
      } else {
        context.setDataFrame(alias, df.selectExpr(selectExprs));
      }
    }
  }
}
