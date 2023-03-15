package com.shawn.octopus.spark.operators.report.metrics.converter;

import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public enum DatasetToMapConverter implements Converter<Map<Object, Object>> {
  DATASET_TO_MULTI_ROW_MAP_OBJECT_CONVERTER {
    @Override
    public Map<Object, Object> convert(Dataset<Row> df) throws Exception {
      long cnt = df.count();
      if (cnt == 0) {
        return null;
      }

      if (cnt > MAX_DF_CNT) {
        throw new Exception("df row num [" + cnt + "] too big to convert to list object");
      }

      int colCnt = df.columns().length;
      if (colCnt != 2) {
        throw new Exception("df col num [" + colCnt + "] not fit to multi row map object.");
      }

      Row[] rows = (Row[]) df.collect(); // 本地编译通不过，加了个强制转换
      Map<Object, Object> map = new HashMap<>();
      for (Row row : rows) {
        map.put(row.get(0), row.get(1));
      }
      return map;
    }
  },
  DATASET_TO_MULTI_FIELD_MAP_OBJECT_CONVERTER {
    @Override
    public Map<Object, Object> convert(Dataset<Row> df) throws Exception {
      long cnt = df.count();
      if (cnt == 0) {
        return null;
      }

      int colCnt = df.columns().length;
      if (cnt != 1 || colCnt < 1) {
        throw new Exception(
            "df row num ["
                + cnt
                + "], col num ["
                + colCnt
                + "] not fit to multi field map object.");
      }

      Row[] rows = (Row[]) df.collect(); // 本地编译通不过，加了个强制转换
      Map<Object, Object> map = new HashMap<>();
      for (int i = 0; i < rows[0].size(); i++) {
        map.put(rows[0].schema().fieldNames()[i], rows[0].get(i));
      }
      return map;
    }
  }
}
