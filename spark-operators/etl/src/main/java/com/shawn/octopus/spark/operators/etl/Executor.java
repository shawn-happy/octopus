package com.shawn.octopus.spark.operators.etl;

import com.shawn.octopus.spark.operators.common.SupportedSinkType;
import com.shawn.octopus.spark.operators.common.SupportedSourceType;
import com.shawn.octopus.spark.operators.common.SupportedTransformType;
import com.shawn.octopus.spark.operators.common.declare.ETLDeclare;
import com.shawn.octopus.spark.operators.common.declare.sink.CSVSinkDeclare;
import com.shawn.octopus.spark.operators.common.declare.sink.SinkDeclare;
import com.shawn.octopus.spark.operators.common.declare.sink.SinkOptions;
import com.shawn.octopus.spark.operators.common.declare.source.CSVSourceDeclare;
import com.shawn.octopus.spark.operators.common.declare.source.SourceDeclare;
import com.shawn.octopus.spark.operators.common.declare.source.SourceOptions;
import com.shawn.octopus.spark.operators.common.declare.transform.SparkSQLTransformDeclare;
import com.shawn.octopus.spark.operators.common.declare.transform.TransformDeclare;
import com.shawn.octopus.spark.operators.common.declare.transform.TransformOptions;
import com.shawn.octopus.spark.operators.etl.sink.CSVSink;
import com.shawn.octopus.spark.operators.etl.sink.Sink;
import com.shawn.octopus.spark.operators.etl.source.CSVSource;
import com.shawn.octopus.spark.operators.etl.source.Source;
import com.shawn.octopus.spark.operators.etl.transform.SparkSQLTransform;
import com.shawn.octopus.spark.operators.etl.transform.Transform;
import java.util.HashMap;
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

  public static void main(String[] args) throws Exception {
    Executor executor = new Executor();
    Logger.getLogger("org.apache.iceberg.hadoop").setLevel(Level.ERROR);
    executor.run(args[0]);
  }

  public void run(String configPath) throws Exception {
    try (SparkSession spark =
        SparkSession.builder()
            //            .enableHiveSupport()
            //            .config("hive.exec.dynamic.partition", "true")
            //            .config("hive.exec.dynamic.partition.mode", "nonstrict")
            .appName("local-test")
            .master("local[2]")
            .getOrCreate()) {
      ETLDeclare config = ETLUtils.getConfig(configPath, ETLDeclare.class);
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

      List<TransformDeclare<?>> transformDeclares = config.getTransforms();
      for (TransformDeclare<? extends TransformOptions> transformDeclare : transformDeclares) {
        SupportedTransformType type = transformDeclare.getType();
        TransformOptions transformOptions = transformDeclare.getOptions();
        Map<String, Dataset<Row>> dfs = new HashMap<>();
        Transform<?> transform;
        switch (type) {
          case sparkSQL:
            SparkSQLTransformDeclare sparkSQLTransformDeclare =
                (SparkSQLTransformDeclare) transformDeclare;
            transform = new SparkSQLTransform(sparkSQLTransformDeclare);
            for (Map.Entry<String, String> entry : transformOptions.getInput().entrySet()) {
              Dataset<Row> df = dataframes.get(entry.getKey());
              if (df == null) {
                log.error("input dataframe [{}] not found.", entry.getKey());
                throw new RuntimeException("input dataframe not found.");
              }
              df.createOrReplaceTempView(entry.getValue());
              dfs.put(entry.getValue(), df);
            }
            break;
          default:
            throw new IllegalArgumentException("unsupported transform type: " + type);
        }
        Dataset<Row> df = transform.trans(spark, dfs);
        if (transformOptions.getRepartition() != null) {
          df = df.repartition(transformOptions.getRepartition());
        }
        dataframes.put(transformOptions.getOutput(), df);
      }

      List<SinkDeclare<?>> sinkDeclares = config.getSinks();
      for (SinkDeclare<? extends SinkOptions> sinkDeclare : sinkDeclares) {
        SupportedSinkType type = sinkDeclare.getType();
        SinkOptions sinkOptions = sinkDeclare.getOptions();
        Sink<?> sink;
        Dataset<Row> df;
        switch (type) {
          case csv:
            CSVSinkDeclare csvSinkDeclare = (CSVSinkDeclare) sinkDeclare;
            sink = new CSVSink(csvSinkDeclare);
            df = dataframes.get(sinkOptions.getInput());
            break;
          default:
            throw new IllegalArgumentException("unsupported sink type: " + type);
        }
        sink.output(spark, df);
      }
    }
  }
}
