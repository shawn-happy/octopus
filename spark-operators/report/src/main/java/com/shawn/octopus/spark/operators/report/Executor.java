package com.shawn.octopus.spark.operators.report;

import com.shawn.octopus.spark.operators.common.SupportedSourceType;
import com.shawn.octopus.spark.operators.common.declare.ReportDeclare;
import com.shawn.octopus.spark.operators.common.declare.source.CSVSourceDeclare;
import com.shawn.octopus.spark.operators.common.declare.source.SourceDeclare;
import com.shawn.octopus.spark.operators.common.declare.source.SourceOptions;
import com.shawn.octopus.spark.operators.common.declare.transform.TransformOptions;
import com.shawn.octopus.spark.operators.common.declare.transform.metrics.BuiltinMetricsTransformDeclare;
import com.shawn.octopus.spark.operators.common.declare.transform.metrics.MetricsTransformDeclare;
import com.shawn.octopus.spark.operators.common.declare.transform.metrics.MetricsType;
import com.shawn.octopus.spark.operators.etl.ETLUtils;
import com.shawn.octopus.spark.operators.etl.source.CSVSource;
import com.shawn.octopus.spark.operators.etl.source.Source;
import com.shawn.octopus.spark.operators.report.load.DefaultLoader;
import com.shawn.octopus.spark.operators.report.load.Loader;
import com.shawn.octopus.spark.operators.report.metrics.BuiltinMetrics;
import com.shawn.octopus.spark.operators.report.registry.OpRegistry;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class Executor {

  private final transient Map<String, Dataset<Row>> dataframes = new HashMap<>();
  private final transient Map<String, Object> metricsResults = new HashMap<>();

  public static void main(String[] args) throws Exception {
    Executor executor = new Executor();
    Logger.getLogger("org.apache.iceberg.hadoop").setLevel(Level.ERROR);
    executor.run(args[0]);
  }

  public void run(String path) throws Exception {
    Loader loader = new DefaultLoader(OpRegistry.OP_REGISTRY);
    loader.init();
    try (SparkSession spark =
        SparkSession.builder()
            //            .enableHiveSupport()
            //            .config("hive.exec.dynamic.partition", "true")
            //            .config("hive.exec.dynamic.partition.mode", "nonstrict")
            .appName("local-test")
            .master("local[2]")
            .getOrCreate()) {
      ReportDeclare config = ETLUtils.getConfig(path, ReportDeclare.class);
      if (config.getSparkConf() != null) {
        for (Map.Entry<String, String> kv : config.getSparkConf().entrySet()) {
          if (spark.conf().isModifiable(kv.getKey())) {
            spark.conf().set(kv.getKey(), kv.getValue());
          } else {
            log.warn("spark conf [{}] can not set runtime.", kv.getKey());
          }
        }
      }

      if (config.getParams() != null) {
        for (Map.Entry<String, String> kv : config.getParams().entrySet()) {
          spark.sql("set " + kv.getKey() + "=" + kv.getValue());
        }
      }

      List<SourceDeclare<?>> sourceDeclares = config.getSources();
      for (SourceDeclare<? extends SourceOptions> sourceDeclare : sourceDeclares) {
        SupportedSourceType type = sourceDeclare.getType();
        SourceOptions sourceOptions = sourceDeclare.getOptions();
        Source<?> source;
        switch (type) {
          case csv:
            CSVSourceDeclare csvSourceDeclare = (CSVSourceDeclare) sourceDeclare;
            source = new CSVSource(csvSourceDeclare);
            break;
          default:
            throw new IllegalArgumentException("unsupported source type: " + type);
        }
        Dataset<Row> df = source.input(spark);
        Integer repartition = sourceOptions.getRepartition();
        if (repartition != null) {
          df = df.repartition(repartition);
        }
        dataframes.put(sourceOptions.getOutput(), df);
      }
      List<MetricsTransformDeclare<?>> metricsList = config.getTransforms();
      for (MetricsTransformDeclare<? extends TransformOptions> metricDeclare : metricsList) {
        MetricsType metricsType = metricDeclare.getMetricsType();
        switch (metricsType) {
          case builtin:
            BuiltinMetrics metrics =
                new BuiltinMetrics((BuiltinMetricsTransformDeclare) metricDeclare);
            Map<String, Dataset<Row>> dfs = new LinkedHashMap<>();
            ((BuiltinMetricsTransformDeclare) metricDeclare)
                .getOptions()
                .getInput()
                .forEach(
                    (k, v) -> {
                      Dataset<Row> df = dataframes.get(k);
                      if (df == null) {
                        log.error("input dataframe [{}] not found.", k);
                        throw new RuntimeException("input dataframe not found.");
                      }
                      dfs.put(v, df);
                    });
            Object result =
                metrics.calculate(
                    spark,
                    dfs,
                    ((BuiltinMetricsTransformDeclare) metricDeclare).getOptions().getColumns());
            metricsResults.put(metricDeclare.getName(), result);
            break;
          default:
            throw new IllegalArgumentException("unsupported metrics type: " + metricsType);
        }
      }
      log.info("metrics results: {}", metricsResults);
    }
  }
}
