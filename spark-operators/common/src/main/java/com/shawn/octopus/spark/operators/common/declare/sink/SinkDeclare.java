package com.shawn.octopus.spark.operators.common.declare.sink;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.shawn.octopus.spark.operators.common.SupportedSinkType;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = CSVSinkDeclare.class, name = "csv"),
})
public interface SinkDeclare<P extends SinkOptions> {
  SupportedSinkType getType();

  P getOptions();
}
