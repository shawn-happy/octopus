package com.octopus.operators.spark.runtime.step.sink;

import com.octopus.operators.spark.declare.common.ColumnDesc;
import com.octopus.operators.spark.declare.common.WriteMode;
import com.octopus.operators.spark.declare.sink.JDBCSinkDeclare;
import com.octopus.operators.spark.declare.sink.JDBCSinkDeclare.IsolationLevel;
import com.octopus.operators.spark.declare.sink.JDBCSinkDeclare.JDBCSinkOptions;
import com.octopus.operators.spark.exception.SparkRuntimeException;
import com.octopus.operators.spark.runtime.sql.MySQLSQLBuilder;
import com.octopus.operators.spark.runtime.sql.SQLBuilder;
import com.octopus.operators.spark.utils.SparkOperatorUtils;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.Iterator;

@Slf4j
public class JDBCSink extends BaseSink<JDBCSinkDeclare> {

  public JDBCSink(JDBCSinkDeclare declare) {
    super(declare);
  }

  @Override
  protected void process(SparkSession spark, Dataset<Row> df) throws Exception {
    JDBCSinkOptions options = declare.getOptions();
    String driverClassName = getDriverClassName(options.getDriverClassName());
    String url = options.getUrl();
    String username = options.getUsername();
    String password = options.getPassword();
    int transactionLevel = IsolationLevel.toJDBCRawTransactionLevel(options.getIsolationLevel());
    int batchSize = options.getBatchSize();
    String sql = getSQL(options);
    WriteMode writeMode = declare.getWriteMode();
    List<String> columns =
        options.getSchemas().stream()
            .filter(schemaTerm -> !schemaTerm.isPrimaryKey())
            .map(ColumnDesc::getName)
            .collect(Collectors.toList());
    Integer numPartitions = options.getNumPartitions();
    if (numPartitions != null) {
      df.repartition(numPartitions);
    }
    df.foreachPartition(
        partition -> {
          Connection connection = null;
          PreparedStatement ps = null;
          try {
            Class.forName(driverClassName);
            connection = DriverManager.getConnection(url, username, password);
            ps = connection.prepareStatement(sql);
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(transactionLevel);
            int count = 0;
            while (partition.hasNext()) {
              Row row = partition.next();
              setParameter(ps, row, writeMode, columns);
              count++;
              ps.addBatch();
              if (count % batchSize == 0) {
                int[] batch = ps.executeBatch();
                connection.commit();
                log.info("finish commit: {}", batch.length);
                ps.clearBatch();
              }
            }
            if (count != 0) {
              int[] batch = ps.executeBatch();
              connection.commit();
              log.info("finish commit: {}", batch.length);
              ps.clearBatch();
            }
          } catch (Exception e) {
            if (connection != null) {
              connection.rollback();
            }
            throw new SparkRuntimeException(e);
          } finally {
            if (ps != null) {
              ps.close();
            }
            if (connection != null) {
              connection.close();
            }
          }
        });
  }

  private String getSQL(JDBCSinkOptions options) {
    SQLBuilder sqlBuilder = newSQLBuilder(options);
    WriteMode writeMode = declare.getWriteMode();
    switch (writeMode) {
      case append:
        String insert = sqlBuilder.insert();
        log.info("jdbc insert sql: {}", insert);
        return insert;
      case replace:
        String insertOrUpdate = sqlBuilder.insertOrUpdate();
        log.info("jdbc insert or update sql: {}", insertOrUpdate);
        return insertOrUpdate;
      default:
        throw new IllegalArgumentException("unsupported write mode: " + writeMode);
    }
  }

  private static void setParameter(
      PreparedStatement ps, Row row, WriteMode writeMode, List<String> columns)
      throws SQLException {
    StructType structType = row.schema();
    Iterator<StructField> iterator = structType.iterator();
    int index = 1;
    int i = 0;
    while (iterator.hasNext()) {
      StructField next = iterator.next();
      DataType dataType = next.dataType();
      SparkOperatorUtils.setJDBCParamValue(ps, index, row.get(i), dataType);
      i++;
      index++;
    }
    if (writeMode == WriteMode.overwrite_partitions) {
      iterator = structType.iterator();
      i = 0;
      while (iterator.hasNext()) {
        StructField next = iterator.next();
        DataType dataType = next.dataType();
        if (columns.contains(next.name())) {
          SparkOperatorUtils.setJDBCParamValue(ps, index, row.get(i), dataType);
          index++;
        }
        i++;
      }
    }
  }

  private String getDriverClassName(String driverClassName) {
    if (StringUtils.isBlank(driverClassName)) {
      ServiceLoader<Driver> sl = ServiceLoader.load(Driver.class);
      java.util.Iterator<Driver> iterator = sl.iterator();
      if (!iterator.hasNext()) {
        throw new IllegalArgumentException("driver class not found");
      }
      driverClassName = sl.iterator().next().getClass().getName();
    }
    return driverClassName;
  }

  private SQLBuilder newSQLBuilder(JDBCSinkOptions options) {
    String driverClassName = getDriverClassName(options.getDriverClassName());
    if ("com.mysql.cj.jdbc.Driver".equals(driverClassName)
        || "com.mysql.jdbc.Driver".equals(driverClassName)) {
      return new MySQLSQLBuilder(options.getTable(), options.getSchemas());
    } else {
      throw new IllegalArgumentException(
          "The database type is not supported, driver class name: " + options.getDriverClassName());
    }
  }
}
