package com.octopus.operators.spark.declare.postprocess;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.octopus.operators.spark.declare.common.Verifiable;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.shaded.com.google.common.base.Verify;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = CorrectionPostProcessDeclare.class, name = "correction"),
  @JsonSubTypes.Type(value = AlarmPostProcessDeclare.class, name = "alarm"),
})
public interface PostProcessDeclare<P extends PostProcessOptions> extends Verifiable {

  PostProcessType getType();

  String getName();

  P getOptions();

  @Override
  default void verify() {
    Verify.verify(ObjectUtils.isNotEmpty(getType()), "type can not be null");
    Verify.verify(StringUtils.isNotBlank(getName()), "name can not be null or empty");
  }
}
