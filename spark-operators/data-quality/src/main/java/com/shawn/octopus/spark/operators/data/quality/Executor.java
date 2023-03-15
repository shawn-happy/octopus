package com.shawn.octopus.spark.operators.data.quality;

import com.shawn.octopus.spark.operators.common.SupportedSourceType;
import com.shawn.octopus.spark.operators.common.declare.DataQualityDeclare;
import com.shawn.octopus.spark.operators.common.declare.source.CSVSourceDeclare;
import com.shawn.octopus.spark.operators.common.declare.source.SourceDeclare;
import com.shawn.octopus.spark.operators.common.declare.source.SourceOptions;
import com.shawn.octopus.spark.operators.common.declare.transform.TransformOptions;
import com.shawn.octopus.spark.operators.common.declare.transform.check.CheckTransformDeclare;
import com.shawn.octopus.spark.operators.common.declare.transform.metrics.BuiltinMetricsTransformDeclare;
import com.shawn.octopus.spark.operators.common.declare.transform.metrics.CustomMetricsTransformDeclare;
import com.shawn.octopus.spark.operators.common.declare.transform.metrics.ExpressionMetricsTransformDeclare;
import com.shawn.octopus.spark.operators.common.declare.transform.metrics.MetricsTransformDeclare;
import com.shawn.octopus.spark.operators.common.declare.transform.metrics.MetricsType;
import com.shawn.octopus.spark.operators.common.declare.transform.postprocess.CorrectionPostProcessDeclare;
import com.shawn.octopus.spark.operators.common.declare.transform.postprocess.PostProcessTransformDeclare;
import com.shawn.octopus.spark.operators.common.declare.transform.postprocess.PostProcessType;
import com.shawn.octopus.spark.operators.common.exception.SparkRuntimeException;
import com.shawn.octopus.spark.operators.data.quality.check.Check;
import com.shawn.octopus.spark.operators.data.quality.check.CheckResult;
import com.shawn.octopus.spark.operators.data.quality.check.ExpressionCheck;
import com.shawn.octopus.spark.operators.data.quality.postProcess.CorrectionPostProcess;
import com.shawn.octopus.spark.operators.data.quality.postProcess.PostProcess;
import com.shawn.octopus.spark.operators.etl.SparkOperatorUtils;
import com.shawn.octopus.spark.operators.etl.source.CSVSource;
import com.shawn.octopus.spark.operators.etl.source.Source;
import com.shawn.octopus.spark.operators.report.load.DefaultLoader;
import com.shawn.octopus.spark.operators.report.load.Loader;
import com.shawn.octopus.spark.operators.report.metrics.BuiltinMetrics;
import com.shawn.octopus.spark.operators.report.metrics.CustomMetrics;
import com.shawn.octopus.spark.operators.report.metrics.ExpressionMetrics;
import com.shawn.octopus.spark.operators.report.metrics.Metrics;
import com.shawn.octopus.spark.operators.report.metrics.converter.DatasetToObjectConverter;
import com.shawn.octopus.spark.operators.report.registry.OpRegistry;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class Executor {

  private final transient Map<String, Dataset<Row>> dataframes = new HashMap<>();
  private final transient Map<String, Object> metricsResults = new HashMap<>();
  private final transient Map<String, CheckResult> checkResults = new HashMap<>();
  private final transient Map<String, String> sources = new HashMap<>();

  public static void main(String[] args) {}

  public void run(String path, boolean enableLocal, boolean enableHive) throws Exception {
    Loader loader = new DefaultLoader(OpRegistry.OP_REGISTRY);
    loader.init();
    try (SparkSession spark = SparkOperatorUtils.createSparkSession(enableLocal, enableHive)) {
      DataQualityDeclare config = SparkOperatorUtils.getConfig(path, DataQualityDeclare.class);
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
        // TODO: FOR TEST
        sources.put(sourceOptions.getOutput(), sourceOptions.getOutput());
      }

      List<MetricsTransformDeclare<?>> metricsList = config.getMetrics();
      for (MetricsTransformDeclare<? extends TransformOptions> metricDeclare : metricsList) {
        MetricsType metricsType = metricDeclare.getMetricsType();
        Metrics<?> metrics;
        Map<String, Dataset<Row>> dfs = new LinkedHashMap<>();
        switch (metricsType) {
          case builtin:
            BuiltinMetricsTransformDeclare builtinMetricsTransformDeclare =
                (BuiltinMetricsTransformDeclare) metricDeclare;
            builtinMetricsTransformDeclare
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
            metrics = new BuiltinMetrics(builtinMetricsTransformDeclare);
            break;
          case expression:
            ExpressionMetricsTransformDeclare expressionMetricsTransformDeclare =
                (ExpressionMetricsTransformDeclare) metricDeclare;
            Map<String, Object> inputMetrics = new LinkedHashMap<>();
            expressionMetricsTransformDeclare
                .getOptions()
                .getInput()
                .forEach(
                    (k, v) -> {
                      if (!metricsResults.containsKey(k)) {
                        log.error("input metric [{}] not found.", k);
                        throw new RuntimeException("input metric not found.");
                      }
                      inputMetrics.put(k, metricsResults.get(k));
                    });
            metrics = new ExpressionMetrics(expressionMetricsTransformDeclare, inputMetrics);
            break;
          case custom:
            CustomMetricsTransformDeclare customMetricsTransformDeclare =
                (CustomMetricsTransformDeclare) metricDeclare;
            customMetricsTransformDeclare
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
            metrics = new CustomMetrics(customMetricsTransformDeclare);
            break;
          default:
            throw new IllegalArgumentException("unsupported metrics type: " + metricsType);
        }
        Object calculate = metrics.calculate(spark, dfs);
        if (calculate instanceof Dataset) {
          Dataset<Row> df = (Dataset<Row>) calculate;
          dataframes.put(metricDeclare.getOptions().getOutput(), df);
          metricsResults.put(
              metricDeclare.getName(),
              DatasetToObjectConverter.DATASET_TO_OBJECT_CONVERTER.convert(df));
        } else {
          metricsResults.put(metricDeclare.getName(), calculate);
        }
      }
      log.info("metrics results: {}", metricsResults);

      List<CheckTransformDeclare> checkDeclares = config.getChecks();
      for (CheckTransformDeclare checkDeclare : checkDeclares) {
        Check<CheckTransformDeclare> check = new ExpressionCheck(checkDeclare);
        Map<String, Object> inputMetrics = new LinkedHashMap<>();
        checkDeclare
            .getOptions()
            .getInput()
            .forEach(
                (k, v) -> {
                  if (!metricsResults.containsKey(k)) {
                    log.error("input metric [{}] not found.", k);
                    throw new RuntimeException("input metric not found.");
                  }
                  inputMetrics.put(k, metricsResults.get(k));
                });
        checkResults.put(
            checkDeclare.getName(),
            CheckResult.builder()
                .pass(check.check(spark, inputMetrics))
                .level(checkDeclare.getOptions().getLevel())
                .build());
      }

      List<PostProcessTransformDeclare<?>> postProcesses = config.getPostProcesses();
      if (CollectionUtils.isNotEmpty(postProcesses)) {
        for (PostProcessTransformDeclare<?> postProcessDeclare : postProcesses) {
          PostProcessType postProcessType = postProcessDeclare.getPostProcessType();
          PostProcess<?> postProcess = null;
          switch (postProcessType) {
            case correction:
              CorrectionPostProcessDeclare correctionPostProcessDeclare =
                  (CorrectionPostProcessDeclare) postProcessDeclare;
              postProcess = new CorrectionPostProcess(correctionPostProcessDeclare, sources);
              break;
            default:
              throw new SparkRuntimeException("unsupported post process type: " + postProcessType);
          }
          postProcess.process(spark, checkResults);
        }
      }
    }
  }
}
