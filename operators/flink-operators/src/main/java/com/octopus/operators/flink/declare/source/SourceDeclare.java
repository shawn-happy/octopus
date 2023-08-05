package com.octopus.operators.flink.declare.source;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Verify;
import com.octopus.operators.flink.declare.common.SourceType;
import com.octopus.operators.flink.declare.common.Verifiable;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({@JsonSubTypes.Type(value = CSVSourceDeclare.class, name = "csv")})
public interface SourceDeclare<P extends SourceOptions> extends Verifiable {

  @NotNull
  SourceType getType();

  Integer getRepartition();

  @NotNull
  String getOutput();

  @NotNull
  String getName();

  P getOptions();

  @Override
  default void verify() {
    Verify.verify(ObjectUtils.isNotEmpty(getType()), "type can not be null");
    Verify.verify(StringUtils.isNotBlank(getName()), "name can not be empty or null");
    Verify.verify(StringUtils.isNotBlank(getOutput()), "output can not be empty or null");
    Verify.verify(
        getRepartition() == null || (getRepartition() != null && getRepartition() >= 1),
        "if repartition is not null, must more than or equals 1");
  }
}
