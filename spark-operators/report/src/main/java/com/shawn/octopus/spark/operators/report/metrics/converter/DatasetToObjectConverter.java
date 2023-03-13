package com.shawn.octopus.spark.operators.report.metrics.converter;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public enum DatasetToObjectConverter implements Converter<Object> {
  DATASET_TO_OBJECT_CONVERTER {
    @Override
    public Object convert(Dataset<Row> df) throws Exception {
      long cnt = df.count();
      if (cnt == 0) {
        return null;
      }
      int colCnt = df.columns().length;
      if (cnt != 1 || colCnt != 1) {
        throw new Exception(
            "df row num [" + cnt + "], col num [" + colCnt + "] not fit to single object.");
      }
      return df.first().get(0);
    }
  }
}
