package com.octopus.operators.spark.declare.sink;

import static com.octopus.operators.spark.declare.sink.JDBCSinkDeclare.IsolationLevel.READ_COMMITTED;
import static java.sql.Connection.TRANSACTION_NONE;
import static java.sql.Connection.TRANSACTION_READ_COMMITTED;
import static java.sql.Connection.TRANSACTION_READ_UNCOMMITTED;
import static java.sql.Connection.TRANSACTION_REPEATABLE_READ;
import static java.sql.Connection.TRANSACTION_SERIALIZABLE;

import com.octopus.operators.spark.declare.common.ColumnDesc;
import com.octopus.operators.spark.declare.common.SinkType;
import com.octopus.operators.spark.declare.common.WriteMode;
import com.octopus.operators.spark.declare.sink.JDBCSinkDeclare.JDBCSinkOptions;
import java.sql.Driver;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.shaded.com.google.common.base.Verify;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class JDBCSinkDeclare implements SinkDeclare<JDBCSinkOptions> {

  @Default private final SinkType type = SinkType.jdbc;
  private JDBCSinkOptions options;
  private WriteMode writeMode;
  private String input;
  private String name;

  @Builder
  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class JDBCSinkOptions implements SinkOptions {
    private String url;
    private String username;
    private String password;
    private String driverClassName;
    private String table;
    private List<ColumnDesc> schemas;
    @Default private int batchSize = 1000;
    @Default private IsolationLevel isolationLevel = READ_COMMITTED;
    private Integer numPartitions;

    public String getDriverClassName() {
      if (StringUtils.isBlank(driverClassName)) {
        ServiceLoader<Driver> sl = ServiceLoader.load(Driver.class);
        Iterator<Driver> iterator = sl.iterator();
        if (!iterator.hasNext()) {
          throw new IllegalArgumentException("driver class not found");
        }
        driverClassName = sl.iterator().next().getClass().getName();
      }
      return driverClassName;
    }

    @Override
    public Map<String, String> getOptions() {
      Map<String, String> options = new HashMap<>();
      options.put("url", url);
      options.put("username", username);
      options.put("password", password);
      options.put("driver", getDriverClassName());
      options.put("dbtable", table);
      options.put("isolationLevel", isolationLevel.name());
      options.put("batchsize", String.valueOf(batchSize));
      if (numPartitions != null) {
        options.put("numPartitions", String.valueOf(numPartitions));
      }
      return options;
    }

    @Override
    public void verify() {
      Verify.verify(StringUtils.isNotBlank(url), "url can not be empty or null in jdbc sink");
      Verify.verify(
          StringUtils.isNotBlank(getDriverClassName()),
          "driver class can not be empty or null in jdbc sink");
    }
  }

  public enum IsolationLevel {
    READ_UNCOMMITTED,
    READ_COMMITTED,
    REPEATABLE_READ,
    SERIALIZABLE,
    NONE,
    ;

    public static IsolationLevel of(String type) {
      return Arrays.stream(values())
          .filter(level -> level.name().equalsIgnoreCase(type))
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException("No Such Transaction Level: " + type));
    }

    public static int toJDBCRawTransactionLevel(String level) {
      IsolationLevel isolation = of(level);
      return toJDBCRawTransactionLevel(isolation);
    }

    public static int toJDBCRawTransactionLevel(IsolationLevel isolation) {
      switch (isolation) {
        case READ_COMMITTED:
          return TRANSACTION_READ_COMMITTED;
        case READ_UNCOMMITTED:
          return TRANSACTION_READ_UNCOMMITTED;
        case REPEATABLE_READ:
          return TRANSACTION_REPEATABLE_READ;
        case SERIALIZABLE:
          return TRANSACTION_SERIALIZABLE;
        default:
          return TRANSACTION_NONE;
      }
    }
  }
}
