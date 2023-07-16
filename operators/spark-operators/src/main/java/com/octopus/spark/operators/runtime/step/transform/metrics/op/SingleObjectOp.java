package com.octopus.spark.operators.runtime.step.transform.metrics.op;

import com.octopus.spark.operators.declare.transform.BuiltinMetricsOpType;
import com.octopus.spark.operators.runtime.step.transform.metrics.converter.DatasetToMapConverter;
import com.octopus.spark.operators.runtime.step.transform.metrics.converter.DatasetToObjectConverter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.CommandExecutionMode;
import org.apache.spark.sql.functions$;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

public enum SingleObjectOp implements Op<Object> {
  MAX_OP {
    @Override
    public BuiltinMetricsOpType getOpType() {
      return BuiltinMetricsOpType.max;
    }

    @Override
    protected String sql(String tableName, List<String> columns) {
      return String.format("SELECT max(%2$s) FROM %1$s", tableName, columns.get(0));
    }

    @Override
    protected String rename(List<String> columns) {
      return String.format("_max_%s", columns.get(0));
    }
  },

  MIN_OP {
    @Override
    public BuiltinMetricsOpType getOpType() {
      return BuiltinMetricsOpType.min;
    }

    @Override
    protected String sql(String tableName, List<String> columns) {
      return String.format("SELECT min(%2$s) FROM %1$s", tableName, columns.get(0));
    }

    @Override
    protected String rename(List<String> columns) {
      return String.format("_min_%s", columns.get(0));
    }
  },

  APPROX_COUNT_DISTINCT_OP {

    @Override
    public BuiltinMetricsOpType getOpType() {
      return BuiltinMetricsOpType.approxCountDistinct;
    }

    @Override
    protected String sql(String tableName, List<String> columns) {
      return String.format(
          "SELECT approx_count_distinct(%2$s) FROM %1$s", tableName, columns.get(0));
    }

    @Override
    protected String rename(List<String> columns) {
      return String.format("_approx_count_distinct_%s", columns.get(0));
    }
  },

  APPROX_MEDIAN_OP {
    @Override
    public BuiltinMetricsOpType getOpType() {
      return BuiltinMetricsOpType.approxMedian;
    }

    @Override
    protected String sql(String tableName, List<String> columns) {
      return String.format(
          "SELECT approx_percentile(%2$s, 0.5, 100) FROM %1$s", tableName, columns.get(0));
    }

    @Override
    protected String rename(List<String> columns) {
      return String.format("_approx_percentile_%s", columns.get(0));
    }
  },

  COUNT_OP {
    @Override
    public BuiltinMetricsOpType getOpType() {
      return BuiltinMetricsOpType.count;
    }

    @Override
    protected String sql(String tableName, List<String> columns) {
      return String.format("SELECT count(%2$s) FROM %1$s", tableName, columns.get(0));
    }

    @Override
    public Object process(SparkSession spark, Map<String, Dataset<Row>> dfs, List<String> columns)
        throws Exception {
      if (CollectionUtils.isEmpty(columns)) {
        columns = Collections.singletonList("*");
      }
      return super.process(spark, dfs, columns);
    }

    @Override
    protected String rename(List<String> columns) {
      return String.format("_count_%s", columns.get(0));
    }
  },

  AVG_OP {
    @Override
    public BuiltinMetricsOpType getOpType() {
      return BuiltinMetricsOpType.mean;
    }

    @Override
    protected String sql(String tableName, List<String> columns) {
      return String.format("SELECT avg(%2$s) FROM %1$s", tableName, columns.get(0));
    }

    @Override
    protected String rename(List<String> columns) {
      return String.format("_avg_%s", columns.get(0));
    }
  },

  MEDIAN_OP {
    @Override
    public BuiltinMetricsOpType getOpType() {
      return BuiltinMetricsOpType.median;
    }

    @Override
    protected String sql(String tableName, List<String> columns) {
      return String.format("SELECT percentile(%2$s, 0.5) FROM %1$s", tableName, columns.get(0));
    }

    @Override
    protected String rename(List<String> columns) {
      return String.format("_percentile_%s", columns.get(0));
    }
  },

  NULL_COUNT_OP {
    @Override
    public BuiltinMetricsOpType getOpType() {
      return BuiltinMetricsOpType.nullCount;
    }

    @Override
    protected String sql(String tableName, List<String> columns) {
      return String.format(
          "SELECT count(*) FROM %1$s WHERE %2$s is null", tableName, columns.get(0));
    }

    @Override
    protected String rename(List<String> columns) {
      return String.format("_null_count_%s", columns.get(0));
    }
  },

  DISTINCT_COUNT_OP {
    @Override
    public BuiltinMetricsOpType getOpType() {
      return BuiltinMetricsOpType.distinctCount;
    }

    @Override
    protected String sql(String tableName, List<String> columns) {
      return String.format(
          "SELECT count(distinct %2$s) FROM %1$s", tableName, String.join(",", columns));
    }

    @Override
    protected String rename(List<String> columns) {
      return String.format("_distinct_count_%s", String.join("_", columns));
    }
  },

  NULL_OR_EMPTY_COUNT_OP {
    @Override
    public BuiltinMetricsOpType getOpType() {
      return BuiltinMetricsOpType.nullOrEmptyCount;
    }

    @Override
    protected String sql(String tableName, List<String> columns) {
      return String.format(
          "SELECT count(*) FROM %1$s WHERE %2$s is null OR %2$s=''", tableName, columns.get(0));
    }

    @Override
    protected String rename(List<String> columns) {
      return String.format("_null_or_empty_count_%s", columns.get(0));
    }
  },

  NULL_RATIO_OP {
    @Override
    public BuiltinMetricsOpType getOpType() {
      return BuiltinMetricsOpType.nullRatio;
    }

    @Override
    protected String sql(String tableName, List<String> columns) {
      return String.format(
          "SELECT sum(nvl2(%2$s,0,1))/count(*) FROM %1$s", tableName, columns.get(0));
    }

    @Override
    protected String rename(List<String> columns) {
      return String.format("_null_ratio_%s", columns.get(0));
    }
  },

  UNIQUE_RATIO_OP {
    @Override
    public BuiltinMetricsOpType getOpType() {
      return BuiltinMetricsOpType.uniqueRatio;
    }

    @Override
    protected String sql(String tableName, List<String> columns) {
      return String.format(
          "SELECT count(distinct %2$s)/count(*) FROM %1$s", tableName, String.join(",", columns));
    }

    @Override
    protected String rename(List<String> columns) {
      return String.format("_unique_ratio_%s", String.join(",", columns));
    }
  },

  STDDEV_OP {
    @Override
    public BuiltinMetricsOpType getOpType() {
      return BuiltinMetricsOpType.stddev;
    }

    @Override
    protected String sql(String tableName, List<String> columns) {
      return String.format("SELECT stddev(%2$s) FROM %1$s", tableName, columns.get(0));
    }

    @Override
    protected String rename(List<String> columns) {
      return String.format("_stddev_%s", columns.get(0));
    }
  },

  VARIANCE_OP {
    @Override
    public BuiltinMetricsOpType getOpType() {
      return BuiltinMetricsOpType.variance;
    }

    @Override
    protected String sql(String tableName, List<String> columns) {
      return String.format("SELECT variance(%2$s) FROM %1$s", tableName, columns.get(0));
    }

    @Override
    protected String rename(List<String> columns) {
      return String.format("_variance_%s", columns.get(0));
    }
  },

  STORAGE_SIZE_OP {
    private String tableName;

    @Override
    public BuiltinMetricsOpType getOpType() {
      return BuiltinMetricsOpType.storageSize;
    }

    @Override
    protected String sql(String tableName, List<String> columns) {
      this.tableName = tableName;
      return null;
    }

    @Override
    public Object process(SparkSession spark, Map<String, Dataset<Row>> dfs, List<String> columns)
        throws Exception {
      Dataset<Row> df = dfs.entrySet().iterator().next().getValue();
      return df.queryExecution().logical().stats().sizeInBytes().longValue();
    }

    @Override
    protected String rename(List<String> columns) {
      return String.format("_storage_size_%s", tableName);
    }
  },

  SUMMARY_OP {

    private String tableName;

    @Override
    public BuiltinMetricsOpType getOpType() {
      return BuiltinMetricsOpType.summary;
    }

    @Override
    protected String sql(String tableName, List<String> columns) {
      this.tableName = tableName;
      return null;
    }

    @Override
    protected String rename(List<String> columns) {
      return String.format("_summary_%s", tableName);
    }

    @Override
    public Object process(SparkSession spark, Map<String, Dataset<Row>> dfs, List<String> columns)
        throws Exception {
      Map.Entry<String, Dataset<Row>> df = dfs.entrySet().iterator().next();
      df.getValue().createOrReplaceTempView(df.getKey());

      Map<String, DataType> fieldTypes = new HashMap<>();
      for (StructField structField : df.getValue().schema().fields()) {
        if (columns == null || columns.contains(structField.name())) {
          fieldTypes.put(structField.name(), structField.dataType());
        }
      }

      List<String> fields = new ArrayList<>();
      fields.add("count(*) as count");

      for (Map.Entry<String, DataType> fieldType : fieldTypes.entrySet()) {
        fields.add(
            String.format("count(%s) as %s", fieldType.getKey(), fieldType.getKey() + "_count"));
        fields.add(
            String.format(
                "sum(nvl2(%s,0,1))/count(*) as %s",
                fieldType.getKey(), fieldType.getKey() + "_nullRatio"));
        if (fieldType.getValue().sameType(DataTypes.DateType)
            || fieldType.getValue().sameType(DataTypes.TimestampType)) {
          fields.add(
              String.format("min(%s) as %s", fieldType.getKey(), fieldType.getKey() + "_min"));
          fields.add(
              String.format("max(%s) as %s", fieldType.getKey(), fieldType.getKey() + "_max"));
          fields.add(
              String.format(
                  "count(distinct date(%s)) as %s",
                  fieldType.getKey(), fieldType.getKey() + "_distinctDayCount"));
        } else if (fieldType.getValue().sameType(DataTypes.StringType)) {
          fields.add(
              String.format(
                  "count(distinct %s) as %s",
                  fieldType.getKey(), fieldType.getKey() + "_distinctCount"));
          fields.add(
              String.format(
                  "sum(if(%s is null or %s='',1,0)) as %s",
                  fieldType.getKey(),
                  fieldType.getKey(),
                  fieldType.getKey() + "_nullOrEmptyCount"));
        } else if (fieldType.getValue().sameType(DataTypes.ShortType)
            || fieldType.getValue().sameType(DataTypes.IntegerType)
            || fieldType.getValue().sameType(DataTypes.LongType)
            || fieldType.getValue().sameType(DataTypes.FloatType)
            || fieldType.getValue().sameType(DataTypes.DoubleType)) {
          fields.add(
              String.format("avg(%s) as %s", fieldType.getKey(), fieldType.getKey() + "_mean"));
          fields.add(
              String.format(
                  "stddev(%s) as %s", fieldType.getKey(), fieldType.getKey() + "_stddev"));
          fields.add(
              String.format("min(%s) as %s", fieldType.getKey(), fieldType.getKey() + "_min"));
          fields.add(
              String.format("max(%s) as %s", fieldType.getKey(), fieldType.getKey() + "_max"));
          fields.add(
              String.format(
                  "percentile(%s,0.5) as %s", fieldType.getKey(), fieldType.getKey() + "_median"));
          fields.add(
              String.format(
                  "percentile(%s,0.25) as %s",
                  fieldType.getKey(), fieldType.getKey() + "_firstQuantile"));
          fields.add(
              String.format(
                  "percentile(%s,0.75) as %s",
                  fieldType.getKey(), fieldType.getKey() + "_thirdQuantile"));
        }
      }

      String sql = String.format("SELECT %2$s FROM %1$s", df.getKey(), String.join(",", fields));
      Map<Object, Object> res =
          DatasetToMapConverter.DATASET_TO_MULTI_FIELD_MAP_OBJECT_CONVERTER.convert(spark.sql(sql));
      if (res == null) return null;

      long storageSize = 0;
      if (!df.getValue().isEmpty()) {
        //      storageSize =
        // df.getValue().queryExecution().logical().stats().sizeInBytes().longValue();
        storageSize =
            spark
                .sessionState()
                .executePlan(df.getValue().queryExecution().logical(), CommandExecutionMode.ALL())
                .optimizedPlan()
                .stats()
                .sizeInBytes()
                .longValue();
      }
      res.put("storageSize", storageSize);

      // string type mode metric
      for (Map.Entry<String, DataType> fieldType : fieldTypes.entrySet()) {
        if (fieldType.getValue().sameType(DataTypes.StringType)) {
          Dataset<Row> df1 = df.getValue().groupBy(fieldType.getKey()).count();
          if (df1.count() == 0) {
            res.put(fieldType.getKey() + "_mode", null);
          } else {
            res.put(
                fieldType.getKey() + "_mode",
                df1.orderBy(df1.col("count").desc()).select(fieldType.getKey()).first().get(0));
          }
        }
      }

      return res;
    }
  },

  PSI_OP {
    private String tableName;

    @Override
    public BuiltinMetricsOpType getOpType() {
      return BuiltinMetricsOpType.psi;
    }

    @Override
    protected String sql(String tableName, List<String> columns) {
      this.tableName = tableName;
      return null;
    }

    @Override
    protected String rename(List<String> columns) {
      return String.format("_psi_%s", tableName);
    }

    @Override
    public Object process(SparkSession spark, Map<String, Dataset<Row>> dfs, List<String> columns)
        throws Exception {
      Dataset<Row> df = dfs.entrySet().iterator().next().getValue();
      df.createOrReplaceTempView("t0");
      Dataset<Row> df1 =
          df.repartitionByRange(2, df.col(columns.get(0)))
              .withColumn("__partition_id", functions$.MODULE$.spark_partition_id());
      Dataset<Row> part1 = df1.filter("__partition_id = 0");
      long part1Cnt = part1.count();
      //    System.out.println("partition1 count: " + part1Cnt);

      Dataset<Row> part2 = df1.filter("__partition_id = 1");
      long part2Cnt = part2.count();
      //    System.out.println("partition2 count: " + part2Cnt);

      if (part1Cnt == 0
          || part2Cnt == 0
          || Math.abs((part1Cnt - part2Cnt) / ((float) part1Cnt)) > 0.3) {
        System.out.println("PSI operator ordered distribution not valid.");
        return null;
      }

      part1.createOrReplaceTempView("t1");
      part2.createOrReplaceTempView("t2");

      DataType columnType = null;
      for (StructField structField : df.schema().fields()) {
        if (structField.name().equals(columns.get(1))) {
          columnType = structField.dataType();
          break;
        }
      }
      if (columnType == null) {
        return null;
      } else if (columnType.sameType(DataTypes.StringType)) {
        spark
            .sql(
                String.format(
                    "select %1$s as gp,count(*)/sum(count(*)) over() as rate from t1 group by %1$s",
                    columns.get(1)))
            .createOrReplaceTempView("t11");
        spark
            .sql(
                String.format(
                    "select %1$s as gp,count(*)/sum(count(*)) over() as rate from t2 group by %1$s",
                    columns.get(1)))
            .createOrReplaceTempView("t22");
      } else if (columnType.sameType(DataTypes.ShortType)
          || columnType.sameType(DataTypes.IntegerType)
          || columnType.sameType(DataTypes.LongType)
          || columnType.sameType(DataTypes.FloatType)
          || columnType.sameType(DataTypes.DoubleType)) {
        spark
            .sql(
                String.format(
                    "select percentile_approx(%s,array(0.10,0.20,0.30,0.40,0.50,0.60,0.70,0.80,0.90),9999) as percent from t0",
                    columns.get(1)))
            .createOrReplaceTempView("p");
        spark
            .sql(
                String.format(
                    "select gp,count(*)/sum(count(*)) over() as rate\n"
                        + "from (\n"
                        + "      select %1$s,\n"
                        + "            case when %1$s is null                    then 'gp0'\n"
                        + "            when %1$s< percent[0]                     then 'gp1'\n"
                        + "            when %1$s>=percent[0] and %1$s<percent[1] then 'gp2'\n"
                        + "            when %1$s>=percent[1] and %1$s<percent[2] then 'gp3'\n"
                        + "            when %1$s>=percent[2] and %1$s<percent[3] then 'gp4'\n"
                        + "            when %1$s>=percent[3] and %1$s<percent[4] then 'gp5'\n"
                        + "            when %1$s>=percent[4] and %1$s<percent[5] then 'gp6'\n"
                        + "            when %1$s>=percent[5] and %1$s<percent[6] then 'gp7'\n"
                        + "            when %1$s>=percent[6] and %1$s<percent[7] then 'gp8'\n"
                        + "            when %1$s>=percent[7] and %1$s<percent[8] then 'gp9'\n"
                        + "            when %1$s>=percent[8]                     then 'gp10'\n"
                        + "      end as gp\n"
                        + "      from t1 left join p on 1=1\n"
                        + ") t\n"
                        + "group by gp",
                    columns.get(1)))
            .createOrReplaceTempView("t11");
        spark
            .sql(
                String.format(
                    "select gp,count(*)/sum(count(*)) over() as rate\n"
                        + "from (\n"
                        + "      select %1$s,\n"
                        + "            case when %1$s is null                    then 'gp0'\n"
                        + "            when %1$s< percent[0]                     then 'gp1'\n"
                        + "            when %1$s>=percent[0] and %1$s<percent[1] then 'gp2'\n"
                        + "            when %1$s>=percent[1] and %1$s<percent[2] then 'gp3'\n"
                        + "            when %1$s>=percent[2] and %1$s<percent[3] then 'gp4'\n"
                        + "            when %1$s>=percent[3] and %1$s<percent[4] then 'gp5'\n"
                        + "            when %1$s>=percent[4] and %1$s<percent[5] then 'gp6'\n"
                        + "            when %1$s>=percent[5] and %1$s<percent[6] then 'gp7'\n"
                        + "            when %1$s>=percent[6] and %1$s<percent[7] then 'gp8'\n"
                        + "            when %1$s>=percent[7] and %1$s<percent[8] then 'gp9'\n"
                        + "            when %1$s>=percent[8]                     then 'gp10'\n"
                        + "      end as gp\n"
                        + "      from t2 left join p on 1=1\n"
                        + ") t\n"
                        + "group by gp",
                    columns.get(1)))
            .createOrReplaceTempView("t22");
      } else {
        return null;
      }

      Dataset<Row> res =
          spark.sql(
              "select sum((t22.rate - t11.rate) * ln(t22.rate / t11.rate)) as psi from t22 left join t11 on (t22.gp=t11.gp)");
      return res.first().get(0);
    }
  },

  APPROX_QUANTILES_OP {

    private String tableName;

    @Override
    public BuiltinMetricsOpType getOpType() {
      return BuiltinMetricsOpType.approxQuantiles;
    }

    @Override
    public String sql(String tableName, List<String> columns) {
      this.tableName = tableName;
      return null;
    }

    @Override
    protected String rename(List<String> columns) {
      return String.format("_approx_quantiles_%s", tableName);
    }

    @Override
    public Object process(SparkSession spark, Map<String, Dataset<Row>> dfs, List<String> columns)
        throws Exception {
      Dataset<Row> df = dfs.entrySet().iterator().next().getValue();
      return Arrays.stream(
              df.stat().approxQuantile(columns.get(0), new double[] {0.25, 0.5, 0.75}, 0.1))
          .boxed()
          .collect(Collectors.toList());
    }
  },
  ;
  private static final DatasetToObjectConverter converter =
      DatasetToObjectConverter.DATASET_TO_OBJECT_CONVERTER;

  @Override
  public Object process(SparkSession spark, Map<String, Dataset<Row>> dfs, List<String> columns)
      throws Exception {
    Map.Entry<String, Dataset<Row>> df = dfs.entrySet().iterator().next();
    df.getValue().createOrReplaceTempView(df.getKey());
    String sql = sql(df.getKey(), columns);
    return converter.convert(spark.sql(sql).toDF(rename(columns)));
  }

  protected abstract String sql(String tableName, List<String> columns);

  protected abstract String rename(List<String> columns);
}
