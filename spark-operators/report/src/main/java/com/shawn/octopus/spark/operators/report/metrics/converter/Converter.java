package com.shawn.octopus.spark.operators.report.metrics.converter;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface Converter<T> {

  T convert(Dataset<Row> df) throws Exception;
}
