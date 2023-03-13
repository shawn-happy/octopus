package com.shawn.octopus.spark.operators.common.declare.transform.metrics;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.shawn.octopus.spark.operators.common.declare.transform.TransformDeclare;
import com.shawn.octopus.spark.operators.common.declare.transform.TransformOptions;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "metricsType")
@JsonSubTypes({
  @JsonSubTypes.Type(value = BuiltinMetricsTransformDeclare.class, name = "builtin"),
})
public interface MetricsTransformDeclare<TO extends TransformOptions> extends TransformDeclare<TO> {

  MetricsType getMetricsType();
}
