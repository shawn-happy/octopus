package com.octopus.operators.spark.runtime.executor;

import com.octopus.operators.spark.declare.ETLDeclare;
import com.octopus.operators.spark.declare.sink.SinkDeclare;
import com.octopus.operators.spark.declare.transform.TransformDeclare;
import com.octopus.operators.spark.runtime.factory.SinkFactory;
import com.octopus.operators.spark.runtime.factory.TransformFactory;
import com.octopus.operators.spark.runtime.step.sink.Sink;
import com.octopus.operators.spark.runtime.step.transform.custom.CustomTransform;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class ETLExecutor extends BaseExecutor<ETLDeclare> {

  public ETLExecutor(SparkSession spark, String configPath) {
    super(spark, configPath, ETLDeclare.class);
  }

  @Override
  protected void processTransforms() throws Exception {
    for (TransformDeclare<?> transformDeclare : declare.getTransforms()) {
      CustomTransform<?> etlTransform = TransformFactory.createETLTransform(transformDeclare);
      Map<String, Dataset<Row>> dfs = new HashMap<>();
      for (Map.Entry<String, String> entry : transformDeclare.getInput().entrySet()) {
        Dataset<Row> df = getDataframes().get(entry.getKey());
        if (df == null) {
          log.error("input dataframe [{}] not found.", entry.getKey());
          throw new RuntimeException("input dataframe not found.");
        }
        df.createOrReplaceTempView(entry.getValue());
        dfs.put(entry.getValue(), df);
      }
      Dataset<Row> df = etlTransform.trans(spark, dfs);
      if (transformDeclare.getRepartition() != null) {
        df.repartition(transformDeclare.getRepartition());
      }
      df.createOrReplaceTempView(transformDeclare.getOutput());
      getDataframes().put(transformDeclare.getOutput(), df);
    }
  }

  @Override
  protected void processSinks() throws Exception {
    for (SinkDeclare<?> sinkDeclare : declare.getSinks()) {
      Sink<? extends SinkDeclare<?>> sink = SinkFactory.createSink(sinkDeclare);
      String input = sinkDeclare.getInput();
      Dataset<Row> df = getDataframes().get(input);
      if (df == null) {
        log.error("input dataframe [{}] not found.", input);
        throw new RuntimeException("input dataframe not found.");
      }
      sink.output(spark, df);
    }
  }
}
