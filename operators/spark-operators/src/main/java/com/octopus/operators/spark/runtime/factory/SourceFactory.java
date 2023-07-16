package com.octopus.operators.spark.runtime.factory;

import com.octopus.operators.spark.declare.common.SourceType;
import com.octopus.operators.spark.declare.source.CSVSourceDeclare;
import com.octopus.operators.spark.declare.source.IcebergSourceDeclare;
import com.octopus.operators.spark.declare.source.JDBCSourceDeclare;
import com.octopus.operators.spark.declare.source.SourceDeclare;
import com.octopus.operators.spark.runtime.step.source.CSVSource;
import com.octopus.operators.spark.runtime.step.source.IcebergSource;
import com.octopus.operators.spark.runtime.step.source.JDBCSource;
import com.octopus.operators.spark.runtime.step.source.Source;

public class SourceFactory {

  public static Source<?> createSource(SourceDeclare<?> sourceDeclare) {
    Source<?> source = null;
    SourceType type = sourceDeclare.getType();
    SourceType.validate(type);
    if (type == SourceType.csv) {
      CSVSourceDeclare csvSourceDeclare = (CSVSourceDeclare) sourceDeclare;
      source = new CSVSource(csvSourceDeclare);
    } else if (type == SourceType.jdbc) {
      JDBCSourceDeclare jdbcSourceDeclare = (JDBCSourceDeclare) sourceDeclare;
      source = new JDBCSource(jdbcSourceDeclare);
    } else if (type == SourceType.iceberg) {
      IcebergSourceDeclare icebergSourceDeclare = (IcebergSourceDeclare) sourceDeclare;
      source = new IcebergSource(icebergSourceDeclare);
    }
    return source;
  }
}
