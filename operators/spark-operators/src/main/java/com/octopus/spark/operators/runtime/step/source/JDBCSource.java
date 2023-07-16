package com.octopus.spark.operators.runtime.step.source;

import com.octopus.spark.operators.declare.source.JDBCSourceDeclare;
import com.octopus.spark.operators.declare.source.JDBCSourceDeclare.JDBCSourceOptions;
import com.octopus.spark.operators.declare.source.JDBCSourceDeclare.JDBCSourceOptions.ParamValue;
import com.octopus.spark.operators.runtime.parser.GenericTokenParser;
import com.octopus.spark.operators.utils.SparkOperatorUtils;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class JDBCSource extends BaseSource<JDBCSourceDeclare> {

  private final GenericTokenParser parser = new GenericTokenParser("${", "}", "?");

  public JDBCSource(JDBCSourceDeclare declare) {
    super(declare);
  }

  @Override
  protected Dataset<Row> process(SparkSession spark) throws Exception {
    JDBCSourceDeclare sourceDeclare = getSourceDeclare();
    JDBCSourceOptions options = sourceDeclare.getOptions();
    Properties connectProperties = new Properties();
    options.getOptions().forEach(connectProperties::put);
    String preparedSQL = preparedSQL(options);
    if (ArrayUtils.isNotEmpty(options.getPartitionExpressions())) {
      return spark
          .read()
          .jdbc(
              options.getUrl(),
              "(" + preparedSQL + ") as subQuery",
              options.getPartitionExpressions(),
              connectProperties);
    } else {
      return spark
          .read()
          .jdbc(options.getUrl(), "(" + preparedSQL + ") as subQuery", connectProperties);
    }
  }

  private String preparedSQL(JDBCSourceOptions options) {
    String sql = parser.parse(options.getQuery());
    List<ParamValue> params = options.getParams();
    if (CollectionUtils.isEmpty(params)) {
      return sql;
    }
    try {
      Connection connection =
          DriverManager.getConnection(
              options.getUrl(), options.getUsername(), options.getPassword());
      PreparedStatement ps = connection.prepareStatement(sql);
      for (int i = 0; i < params.size(); i++) {
        ParamValue paramValue = params.get(i);
        Object value = paramValue.getValue();
        SparkOperatorUtils.setJDBCParamValue(ps, (i + 1), value, paramValue.getType().name());
      }
      String querySQL = parser.getSQL(ps);
      log.info("jdbc query sql: {}", querySQL);
      ps.close();
      connection.close();
      return querySQL;
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }
  }
}
