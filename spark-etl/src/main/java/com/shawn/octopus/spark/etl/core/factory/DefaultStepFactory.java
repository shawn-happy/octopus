package com.shawn.octopus.spark.etl.core.factory;

import com.shawn.octopus.spark.etl.core.api.StepFactory;
import com.shawn.octopus.spark.etl.core.enums.SinkType;
import com.shawn.octopus.spark.etl.core.enums.SourceType;
import com.shawn.octopus.spark.etl.core.enums.TransformType;
import com.shawn.octopus.spark.etl.sink.Sink;
import com.shawn.octopus.spark.etl.sink.SinkOptions;
import com.shawn.octopus.spark.etl.sink.file.csv.CSVSink;
import com.shawn.octopus.spark.etl.sink.file.csv.CSVSinkOptions;
import com.shawn.octopus.spark.etl.source.Source;
import com.shawn.octopus.spark.etl.source.SourceOptions;
import com.shawn.octopus.spark.etl.source.file.csv.CSVSource;
import com.shawn.octopus.spark.etl.source.file.csv.CSVSourceOptions;
import com.shawn.octopus.spark.etl.source.file.parquet.ParquetSource;
import com.shawn.octopus.spark.etl.source.file.parquet.ParquetSourceOptions;
import com.shawn.octopus.spark.etl.transform.Transform;
import com.shawn.octopus.spark.etl.transform.TransformOptions;
import com.shawn.octopus.spark.etl.transform.sparksql.SparkSQLTransform;
import com.shawn.octopus.spark.etl.transform.sparksql.SparkSQLTransformOptions;

public final class DefaultStepFactory implements StepFactory {

  private DefaultStepFactory() {}

  public static StepFactory getStepFactory() {
    return DefaultStepFactoryHolder.STEP_FACTORY;
  }

  @Override
  public Source createSource(SourceType sourceType, String name, SourceOptions config) {
    Source source = null;
    switch (sourceType) {
      case csv:
        source = new CSVSource(name, (CSVSourceOptions) config);
        break;
      case parquet:
        source = new ParquetSource(name, (ParquetSourceOptions) config);
        break;
      default:
        throw new IllegalArgumentException("unsupported source type");
    }
    return source;
  }

  @Override
  public Transform createTransform(
      TransformType transformType, String name, TransformOptions config) {
    Transform transform = null;
    switch (transformType) {
      case SPARK_SQL:
        transform = new SparkSQLTransform(name, (SparkSQLTransformOptions) config);
        break;
      default:
        throw new IllegalArgumentException("unsupported transform type");
    }
    return transform;
  }

  @Override
  public Sink createSink(SinkType sinkType, String name, SinkOptions config) {
    Sink sink = null;
    switch (sinkType) {
      case csv:
        sink = new CSVSink(name, (CSVSinkOptions) config);
        break;
      default:
        throw new IllegalArgumentException("unsupported sink type");
    }
    return sink;
  }

  private static class DefaultStepFactoryHolder {
    private static final StepFactory STEP_FACTORY = new DefaultStepFactory();
  }
}
