package com.octopus.operators.spark.declare.sink;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.octopus.operators.spark.declare.common.SinkType;
import com.octopus.operators.spark.declare.common.Verifiable;
import com.octopus.operators.spark.declare.common.WriteMode;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.shaded.com.google.common.base.Verify;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = CSVSinkDeclare.class, name = "csv"),
  @JsonSubTypes.Type(value = IcebergSinkDeclare.class, name = "iceberg"),
  @JsonSubTypes.Type(value = JDBCSinkDeclare.class, name = "jdbc"),
})
public interface SinkDeclare<P extends SinkOptions> extends Verifiable {
  SinkType getType();

  P getOptions();

  String getName();

  String getInput();

  WriteMode getWriteMode();

  @Override
  default void verify() {
    Verify.verify(ObjectUtils.isNotEmpty(getType()), "type can not be null");
    Verify.verify(StringUtils.isNotBlank(getName()), "name can not be empty or null in sink step");
    Verify.verify(
        StringUtils.isNotBlank(getInput()), "input can not be empty or null in sink step");
    Verify.verify(
        ObjectUtils.isNotEmpty(getWriteMode()), "writeMode can not be empty or null in sink step");
  }
}
