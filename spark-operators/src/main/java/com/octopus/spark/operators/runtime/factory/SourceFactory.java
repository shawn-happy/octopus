package com.octopus.spark.operators.runtime.factory;

import com.octopus.spark.operators.declare.common.SupportedSourceType;
import com.octopus.spark.operators.declare.source.CSVSourceDeclare;
import com.octopus.spark.operators.declare.source.IcebergSourceDeclare;
import com.octopus.spark.operators.declare.source.JDBCSourceDeclare;
import com.octopus.spark.operators.declare.source.SourceDeclare;
import com.octopus.spark.operators.exception.SparkRuntimeException;
import com.octopus.spark.operators.runtime.step.source.CSVSource;
import com.octopus.spark.operators.runtime.step.source.IcebergSource;
import com.octopus.spark.operators.runtime.step.source.JDBCSource;
import com.octopus.spark.operators.runtime.step.source.Source;
import java.util.List;

public class SourceFactory {

  private static final List<SupportedSourceType> ETL_SUPPORTED_SOURCES =
      List.of(
          SupportedSourceType.csv,
          SupportedSourceType.parquet,
          SupportedSourceType.json,
          SupportedSourceType.jdbc,
          SupportedSourceType.hive,
          SupportedSourceType.iceberg);

  private static final List<SupportedSourceType> REPORT_SUPPORTED_SOURCES =
      List.of(
          SupportedSourceType.csv,
          SupportedSourceType.parquet,
          SupportedSourceType.json,
          SupportedSourceType.jdbc,
          SupportedSourceType.hive,
          SupportedSourceType.iceberg);

  private static final List<SupportedSourceType> DATA_QUALITY_SUPPORTED_SOURCES =
      List.of(
          SupportedSourceType.csv,
          SupportedSourceType.parquet,
          SupportedSourceType.json,
          SupportedSourceType.jdbc,
          SupportedSourceType.hive,
          SupportedSourceType.iceberg);

  public static Source<?> createETLSource(SourceDeclare<?> sourceDeclare) {
    Source<?> source = null;
    SupportedSourceType type = sourceDeclare.getType();
    if (!ETL_SUPPORTED_SOURCES.contains(type)) {
      throw new SparkRuntimeException("Etl Operator do not support this type: " + type);
    }
    if (type == SupportedSourceType.csv) {
      CSVSourceDeclare csvSourceDeclare = (CSVSourceDeclare) sourceDeclare;
      source = new CSVSource(csvSourceDeclare);

    } else if (type == SupportedSourceType.jdbc) {
      JDBCSourceDeclare jdbcSourceDeclare = (JDBCSourceDeclare) sourceDeclare;
      source = new JDBCSource(jdbcSourceDeclare);
    } else if (type == SupportedSourceType.iceberg) {
      IcebergSourceDeclare icebergSourceDeclare = (IcebergSourceDeclare) sourceDeclare;
      source = new IcebergSource(icebergSourceDeclare);
    }
    return source;
  }
}
