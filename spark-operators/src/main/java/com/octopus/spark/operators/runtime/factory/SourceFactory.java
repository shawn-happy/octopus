package com.octopus.spark.operators.runtime.factory;

import com.octopus.spark.operators.declare.common.SourceType;
import com.octopus.spark.operators.declare.source.CSVSourceDeclare;
import com.octopus.spark.operators.declare.source.IcebergSourceDeclare;
import com.octopus.spark.operators.declare.source.JDBCSourceDeclare;
import com.octopus.spark.operators.declare.source.SourceDeclare;
import com.octopus.spark.operators.runtime.step.source.CSVSource;
import com.octopus.spark.operators.runtime.step.source.IcebergSource;
import com.octopus.spark.operators.runtime.step.source.JDBCSource;
import com.octopus.spark.operators.runtime.step.source.Source;

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
