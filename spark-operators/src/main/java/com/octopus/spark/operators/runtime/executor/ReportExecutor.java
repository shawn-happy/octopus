package com.octopus.spark.operators.runtime.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.octopus.spark.operators.declare.ReportDeclare;
import com.octopus.spark.operators.declare.sink.SinkDeclare;
import com.octopus.spark.operators.declare.transform.MetricsDeclare;
import com.octopus.spark.operators.runtime.factory.TransformFactory;
import com.octopus.spark.operators.runtime.seria.NullKeySerializer;
import com.octopus.spark.operators.runtime.step.transform.metrics.Metrics;
import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class ReportExecutor extends BaseExecutor<ReportDeclare> {

  private final transient Map<String, Object> metrics = new HashMap<>();
  private static final ObjectMapper objectMapper = new ObjectMapper();

  static {
    objectMapper.getSerializerProvider().setNullKeySerializer(new NullKeySerializer());
  }

  public ReportExecutor(SparkSession spark, String configPath) {
    super(spark, configPath, ReportDeclare.class);
  }

  @Override
  protected void processTransforms() throws Exception {
    for (MetricsDeclare<?> metricsDeclare : declare.getMetrics()) {
      Metrics<?> metricsOp = TransformFactory.createMetrics(metricsDeclare, this.metrics);
      Map<String, Dataset<Row>> dfs = new LinkedHashMap<>();
      metricsDeclare
          .getInput()
          .forEach(
              (k, v) -> {
                Dataset<Row> df = getDataframes().get(k);
                if (df == null) {
                  log.error("input dataframe [{}] not found.", k);
                  throw new RuntimeException("input dataframe not found.");
                }
                dfs.put(v, df);
              });
      Object result = metricsOp.calculate(spark, dfs);
      metrics.put(metricsDeclare.getName(), result);
    }
  }

  @Override
  protected void processSinks() throws Exception {
    Map<String, Object> map = Map.of("metrics", metrics);
    String res = objectMapper.writeValueAsString(map);
    List<SinkDeclare<?>> sinkDeclares = declare.getSinks();
    if (CollectionUtils.isEmpty(sinkDeclares)) {
      FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
      OutputStream outputStream =
          new BufferedOutputStream(fs.create(new Path(declare.getFilePath())));
      outputStream.write(res.getBytes(StandardCharsets.UTF_8));
      outputStream.close();
      fs.close();
    }
  }
}
