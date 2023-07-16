package com.octopus.spark.operators.declare.check;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.octopus.spark.operators.declare.common.CheckType;
import com.octopus.spark.operators.declare.common.Verifiable;
import java.util.List;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.shaded.com.google.common.base.Verify;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = ExpressionCheckDeclare.class, name = "expression"),
})
public interface CheckDeclare<P extends CheckOptions> extends Verifiable {

  CheckType getType();

  String getName();

  List<String> getMetrics();

  String getOutput();

  Integer getRepartition();

  CheckLevel getCheckLevel();

  P getOptions();

  @Override
  default void verify() {
    Verify.verify(ObjectUtils.isNotEmpty(getType()), "type can not be null");
    Verify.verify(StringUtils.isNotBlank(getName()), "name can not be empty or null");
    Verify.verify(CollectionUtils.isNotEmpty(getMetrics()), "metrics can not be empty or null");
    Verify.verify(StringUtils.isNotBlank(getOutput()), "output can not be empty or null");
    Verify.verify(
        getRepartition() == null || (getRepartition() != null && getRepartition() >= 1),
        "if repartition is not null, must more than or equals 1");
    Verify.verify(ObjectUtils.isEmpty(getCheckLevel()), "check level can not be empty or null");
  }
}
