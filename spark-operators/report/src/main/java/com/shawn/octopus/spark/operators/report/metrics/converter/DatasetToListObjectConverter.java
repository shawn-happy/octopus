package com.shawn.octopus.spark.operators.report.metrics.converter;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public enum DatasetToListObjectConverter implements Converter<List<Object>> {
  DATASET_TO_LIST_OBJECT_CONVERTER {
    @Override
    public List<Object> convert(Dataset<Row> df) throws Exception {
      long cnt = df.count();
      if (cnt == 0) {
        return null;
      }

      if (cnt > MAX_DF_CNT) {
        throw new Exception("df row num [" + cnt + "] too big to convert to list object");
      }

      int colCnt = df.columns().length;
      if (colCnt != 1) {
        throw new Exception("df col num [" + colCnt + "] not fit to list object.");
      }

      List<Object> list = new ArrayList<>();
      for (Row row : (Row[]) df.collect()) {
        list.add(row.get(0));
      }
      return list;
    }
  }
}
