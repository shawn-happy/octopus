package com.octopus.operators.spark.declare.source;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.octopus.operators.spark.declare.common.SourceType;
import com.octopus.operators.spark.declare.common.Verifiable;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.shaded.com.google.common.base.Verify;
import org.jetbrains.annotations.NotNull;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = CSVSourceDeclare.class, name = "csv"),
  @JsonSubTypes.Type(value = IcebergSourceDeclare.class, name = "iceberg"),
  @JsonSubTypes.Type(value = JDBCSourceDeclare.class, name = "jdbc"),
  @JsonSubTypes.Type(value = ParquetSourceDeclare.class, name = "parquet"),
})
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
