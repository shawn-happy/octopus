package com.octopus.spark.operators.declare.transform;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.octopus.spark.operators.declare.common.TransformType;
import com.octopus.spark.operators.declare.common.Verifiable;
import java.util.Map;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.shaded.com.google.common.base.Verify;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = SparkSQLTransformDeclare.class, name = "sparkSQL"),
  @JsonSubTypes.Type(value = BuiltinMetricsTransformDeclare.class, name = "metrics"),
  @JsonSubTypes.Type(value = ExpressionMetricsTransformDeclare.class, name = "expression"),
})
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
