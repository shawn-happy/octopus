package com.octopus.spark.operators.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.Iterator;

@Slf4j
public class SparkOperatorUtils {

  private static final Map<String, DataType> JDBC_DATA_TYPES = new HashMap<>();
  private static final ObjectMapper objectMapperYaml =
      new ObjectMapper(new YAMLFactory().disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER));
  private static final FileSystem fileSystem;

  static {
    JDBC_DATA_TYPES.put("String", DataTypes.StringType);
    JDBC_DATA_TYPES.put("Boolean", DataTypes.BooleanType);
    JDBC_DATA_TYPES.put("Int", DataTypes.IntegerType);
    JDBC_DATA_TYPES.put("Integer", DataTypes.IntegerType);
    JDBC_DATA_TYPES.put("BigInt", DataTypes.LongType);
    JDBC_DATA_TYPES.put("Long", DataTypes.LongType);
    JDBC_DATA_TYPES.put("Float", DataTypes.FloatType);
    JDBC_DATA_TYPES.put("Double", DataTypes.DoubleType);
    JDBC_DATA_TYPES.put("Date", DataTypes.DateType);
    JDBC_DATA_TYPES.put("Timestamp", DataTypes.TimestampType);
    try {
      fileSystem = FileSystem.get(new Configuration());
    } catch (IOException e) {
      log.error("load file system error", e);
      throw new RuntimeException(e);
    }
  }

  public static String createRTASSQL(
      String table, String[] partitionExpressions, Properties tableProperties, String select) {
    return "REPLACE TABLE "
        + table
        + " USING iceberg "
        + getTablePartition(partitionExpressions)
        + getTableProperties(tableProperties)
        + "AS "
        + select;
  }

  public static void setJDBCParamValue(PreparedStatement ps, int index, Object value, String type)
      throws SQLException {
    if (Objects.isNull(value)) {
      ps.setObject(index, null);
    }
    DataType dataType = toDataTypes(type);
    setJDBCParamValue(ps, index, value, dataType);
  }

  public static void setJDBCParamValue(
      PreparedStatement ps, int index, Object value, DataType dataType) throws SQLException {
    if (Objects.isNull(value)) {
      ps.setObject(index, null);
    }
    if (DataTypes.StringType.equals(dataType)) {
      ps.setString(index, value.toString());
    }
    if (DataTypes.BooleanType.equals(dataType)) {
      ps.setBoolean(index, Boolean.parseBoolean(value.toString()));
    }
    if (DataTypes.IntegerType.equals(dataType)) {
      ps.setInt(index, Integer.parseInt(value.toString()));
    }
    if (DataTypes.LongType.equals(dataType)) {
      ps.setLong(index, Long.parseLong(value.toString()));
    }
    if (DataTypes.FloatType.equals(dataType)) {
      ps.setFloat(index, Float.parseFloat(value.toString()));
    }
    if (DataTypes.DoubleType.equals(dataType)) {
      ps.setDouble(index, Double.parseDouble(value.toString()));
    }
    if (DataTypes.DateType.equals(dataType)) {
      ps.setDate(index, Date.valueOf(value.toString()));
    }
    if (DataTypes.TimestampType.equals(dataType)) {
      ps.setTimestamp(index, Timestamp.valueOf(value.toString()));
    }
  }

  public static DataType toDataTypes(String type) {
    String key = type.substring(0, 1).toUpperCase() + type.substring(1).toLowerCase();
    DataType dataType = JDBC_DATA_TYPES.get(key);
    if (Objects.isNull(dataType)) {
      throw new IllegalArgumentException("No Such Data Type: [" + type + "]");
    }
    return dataType;
  }

  public static <T> T getConfig(String path, Class<T> tClass) {
    try {
      return objectMapperYaml.readValue((InputStream) fileSystem.open(new Path(path)), tClass);
    } catch (Exception e) {
      log.error("load config yaml error", e);
      throw new RuntimeException("load config yaml error", e);
    }
  }

  public static SparkSession createSparkSession(boolean enableLocal, boolean enableHive) {
    SparkSession session = SparkSession.builder().getOrCreate();
    if (enableLocal && enableHive) {
      if (enableHive) {
        session =
            SparkSession.builder()
                .master("local")
                .enableHiveSupport()
                .config("hive.exec.dynamic.partition", "true")
                .config("hive.exec.dynamic.partition.mode", "nonstrict")
                .getOrCreate();
      } else {
        session = SparkSession.builder().master("local").getOrCreate();
      }
    } else if (enableHive) {
      session =
          SparkSession.builder()
              .enableHiveSupport()
              .config("hive.exec.dynamic.partition", "true")
              .config("hive.exec.dynamic.partition.mode", "nonstrict")
              .getOrCreate();
    }
    return session;
  }

  public static Column[] validateSchema(StructType sourceSchema, StructType targetSchema) {
    Column[] columns = new Column[targetSchema.size()];
    int colIndex = 0;
    Map<String, StructField> fieldMap =
        Arrays.stream(sourceSchema.fields())
            .collect(Collectors.toMap(i -> i.name().toLowerCase(), i -> i));
    Iterator<StructField> iterator = targetSchema.iterator();
    while (iterator.hasNext()) {
      StructField field = iterator.next();
      String key = field.name().toLowerCase();
      if (!fieldMap.containsKey(key)) {
        log.error("Can't find field [{}] from source data", field.name());
        return null;
      }
      StructField sourceField = fieldMap.get(key);
      if (!sourceField.dataType().equals(field.dataType())) {
        log.error(
            "Type is not same, field: [{}], source type: [{}], target type: [{}]",
            field.name(),
            sourceField.dataType(),
            field.dataType());
        return null;
      } else {
        columns[colIndex] = functions.col(sourceField.name()).as(field.name());
      }
      colIndex++;
    }
    return columns;
  }

  private static String getTablePartition(String[] partitions) {
    if (partitions == null || partitions.length == 0) {
      return "";
    }
    return "PARTITIONED BY (" + String.join(",", partitions) + ") ";
  }

  private static String getTableProperties(Properties properties) {
    if (properties == null || properties.size() == 0) {
      return "";
    }
    Map<String, String> map = Maps.fromProperties(properties);
    return "TBLPROPERTIES ("
        + map.entrySet().stream()
            .map(i -> "'" + i.getKey() + "'='" + i.getValue() + "'")
            .collect(Collectors.joining(","))
        + ") ";
  }
}
