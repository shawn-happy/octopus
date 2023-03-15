package com.shawn.octopus.spark.operators.etl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

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
}
