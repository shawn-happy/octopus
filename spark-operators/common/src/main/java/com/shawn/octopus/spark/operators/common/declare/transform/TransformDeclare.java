package com.shawn.octopus.spark.operators.common.declare.transform;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.shawn.octopus.spark.operators.common.SupportedTransformType;
import com.shawn.octopus.spark.operators.common.declare.transform.metrics.MetricsTransformDeclare;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = SparkSQLTransformDeclare.class, name = "sparkSQL"),
  @JsonSubTypes.Type(value = MetricsTransformDeclare.class, name = "metrics"),
})
public interface TransformDeclare<P extends TransformOptions> {

  SupportedTransformType getType();

  String getName();

  P getOptions();
}
