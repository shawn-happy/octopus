package com.octopus.operators.flink.declare.transform;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Verify;
import com.octopus.operators.flink.declare.common.TransformType;
import com.octopus.operators.flink.declare.common.Verifiable;
import java.util.Map;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({@JsonSubTypes.Type(value = SQLTransformDeclare.class, name = "sql")})
public interface TransformDeclare<P extends TransformOptions> extends Verifiable {
  TransformType getType();

  String getName();

  P getOptions();

  Map<String, String> getInput();

  String getOutput();

  Integer getRepartition();

  @Override
  default void verify() {
    Verify.verify(ObjectUtils.isNotEmpty(getType()), "type can not be null");
    Verify.verify(StringUtils.isNotBlank(getName()), "name can not be empty or null");
    Verify.verify(MapUtils.isNotEmpty(getInput()), "input can not be empty or null");
    Verify.verify(StringUtils.isNotBlank(getOutput()), "output can not be empty or null");
    Verify.verify(
        getRepartition() == null || (getRepartition() != null && getRepartition() >= 1),
        "if repartition is not null, must more than or equals 1");
  }
}
