package com.shawn.octopus.spark.etl.core.api;

import com.shawn.octopus.spark.etl.core.enums.SinkType;
import com.shawn.octopus.spark.etl.core.enums.SourceType;
import com.shawn.octopus.spark.etl.core.enums.TransformType;
import com.shawn.octopus.spark.etl.sink.Sink;
import com.shawn.octopus.spark.etl.sink.SinkOptions;
import com.shawn.octopus.spark.etl.source.Source;
import com.shawn.octopus.spark.etl.source.SourceOptions;
import com.shawn.octopus.spark.etl.transform.Transform;
import com.shawn.octopus.spark.etl.transform.TransformOptions;

public interface StepFactory {

  Source createSource(SourceType sourceType, String name, SourceOptions config);

  Transform createTransform(TransformType transformType, String name, TransformOptions config);

  Sink createSink(SinkType sinkType, String name, SinkOptions config);
}
