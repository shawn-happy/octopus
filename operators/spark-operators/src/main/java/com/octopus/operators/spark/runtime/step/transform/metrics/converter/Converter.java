package com.octopus.operators.spark.runtime.step.transform.metrics.converter;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface Converter<T> {

  int MAX_DF_CNT = 10000;

  T convert(Dataset<Row> df) throws Exception;
}
