package com.shawn.octopus.spark.operators.common.declare.transform;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.shawn.octopus.spark.operators.common.SupportedTransformType;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = SparkSQLTransformDeclare.class, name = "sparkSQL"),
})
public interface TransformDeclare<P extends TransformOptions> {

  SupportedTransformType getType();

  P getOptions();
}
