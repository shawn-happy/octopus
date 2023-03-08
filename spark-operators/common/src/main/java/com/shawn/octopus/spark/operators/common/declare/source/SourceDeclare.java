package com.shawn.octopus.spark.operators.common.declare.source;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.shawn.octopus.spark.operators.common.SupportedSourceType;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = CSVSourceDeclare.class, name = "csv"),
})
public interface SourceDeclare<P extends SourceOptions> {

  SupportedSourceType getType();

  P getOptions();
}
