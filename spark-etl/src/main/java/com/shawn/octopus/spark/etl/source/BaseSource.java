package com.shawn.octopus.spark.etl.source;

import com.shawn.octopus.spark.etl.core.common.TableDesc;
import com.shawn.octopus.spark.etl.core.step.BaseStep;
import com.shawn.octopus.spark.etl.core.step.StepContext;
import java.util.List;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public abstract class BaseSource extends BaseStep implements Source {

  public BaseSource(String name, SourceOptions config) {
    super(name, config);
  }

  @Override
  public void process(StepContext context) {
    SourceOptions config = (SourceOptions) getConfig();
    Dataset<Row> df = read(context);
    List<TableDesc> outputs = config.getOutputs();
    for (TableDesc output : outputs) {
      String alias = output.getAlias();
      String[] selectExprs = output.getSelectExprs();
      df.createOrReplaceTempView(alias);
      if (ArrayUtils.isEmpty(selectExprs)) {
        context.setDataFrame(alias, df);
      } else {
        context.setDataFrame(alias, df.selectExpr(selectExprs));
      }
    }
  }
}
