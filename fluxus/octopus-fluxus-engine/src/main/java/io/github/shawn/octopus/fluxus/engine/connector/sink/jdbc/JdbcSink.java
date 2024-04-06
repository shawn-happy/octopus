package io.github.shawn.octopus.fluxus.engine.connector.sink.jdbc;

import io.github.shawn.octopus.fluxus.api.connector.Sink;
import io.github.shawn.octopus.fluxus.api.exception.StepExecutionException;
import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;
import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import io.github.shawn.octopus.fluxus.engine.common.jdbc.connection.DefaultJdbcConnectionProvider;
import io.github.shawn.octopus.fluxus.engine.common.jdbc.connection.JdbcConnectionConfig;
import io.github.shawn.octopus.fluxus.engine.common.jdbc.connection.JdbcConnectionProvider;
import io.github.shawn.octopus.fluxus.engine.common.jdbc.dialect.JdbcDialect;
import io.github.shawn.octopus.fluxus.engine.common.jdbc.dialect.JdbcDialectLoader;
import io.github.shawn.octopus.fluxus.engine.model.type.FieldTypeUtils;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;

@Slf4j
public class JdbcSink implements Sink<JdbcSinkConfig> {

  private final JdbcSinkConfig config;
  private final JdbcSinkConfig.JdbcSinkOptions options;

  private final JdbcConnectionProvider jdbcConnectionProvider;
  private final JdbcDialect jdbcDialect;
  private Connection connection;
  private PreparedStatement ps;

  //  private final AtomicLong incr = new AtomicLong(0);

  public JdbcSink(JdbcSinkConfig config) {
    this.config = config;
    this.options = config.getOptions();
    this.jdbcConnectionProvider =
        new DefaultJdbcConnectionProvider(
            JdbcConnectionConfig.builder()
                .url(options.getUrl())
                .driverClass(options.getDriver())
                .username(options.getUsername())
                .password(options.getPassword())
                .autoCommit(false)
                .build());
    this.jdbcDialect = JdbcDialectLoader.load(options.getUrl());
  }

  @Override
  public void init() throws StepExecutionException {
    try {
      this.connection = jdbcConnectionProvider.getOrEstablishConnection();
      String sql = generateSQL();
      log.info("jdbc sink sql: {}", sql);
      ps = this.connection.prepareStatement(sql);

    } catch (Exception e) {
      throw new StepExecutionException(e);
    }
  }

  @Override
  public void begin() throws StepExecutionException {
    try {
      this.connection.setAutoCommit(false);
    } catch (SQLException e) {
      throw new StepExecutionException(e);
    }
  }

  @Override
  public void write(RowRecord source) throws StepExecutionException {
    String[] fields = options.getFields();
    String[] conditionFields = options.getConditionFields();
    Object[] values;
    try {
      while ((values = source.pollNext()) != null) {
        for (int i = 0; i < fields.length; i++) {
          String field = fields[i];
          int index = source.indexOf(field);
          setValue(source.getFieldType(index), i + 1, values[index]);
        }
        if (ArrayUtils.isNotEmpty(conditionFields)) {
          for (int i = 0; i < conditionFields.length; i++) {
            String conditionField = conditionFields[i];
            int index = source.indexOf(conditionField);
            setValue(source.getFieldType(index), i + 1 + fields.length, values[index]);
          }
        }
        ps.addBatch();
      }
    } catch (Exception e) {
      throw new StepExecutionException(e);
    }
  }

  @Override
  public JdbcSinkConfig getSinkConfig() {
    return config;
  }

  @Override
  public boolean commit() throws StepExecutionException {
    flush();
    return true;
  }

  @Override
  public void abort() throws StepExecutionException {
    try {
      this.connection.rollback();
    } catch (Exception e) {
      throw new StepExecutionException(e);
    }
  }

  @Override
  public void dispose() throws StepExecutionException {
    try {
      flush();
      if (ps != null) {
        ps.close();
      }
      jdbcConnectionProvider.closeConnection();
    } catch (SQLException e) {
      throw new StepExecutionException(e);
    }
  }

  private void setValue(DataWorkflowFieldType fieldType, int index, Object value)
      throws SQLException {
    if (value == null) {
      ps.setObject(index, null);
      return;
    }
    int sqlType = FieldTypeUtils.getSqlType(fieldType);
    switch (sqlType) {
      case Types.BOOLEAN:
        ps.setBoolean(index, Boolean.parseBoolean(value.toString()));
        break;
      case Types.TINYINT:
        ps.setByte(index, Byte.parseByte(value.toString()));
        break;
      case Types.SMALLINT:
        ps.setShort(index, Short.parseShort(value.toString()));
        break;
      case Types.INTEGER:
        ps.setInt(index, Integer.parseInt(value.toString()));
        break;
      case Types.BIGINT:
        ps.setLong(index, Long.parseLong(value.toString()));
        break;
      case Types.FLOAT:
        ps.setFloat(index, Float.parseFloat(value.toString()));
        break;
      case Types.DOUBLE:
        ps.setDouble(index, Double.parseDouble(value.toString()));
        break;
      case Types.VARCHAR:
        ps.setString(index, value.toString());
        break;
      case Types.DATE:
        LocalDate localDate = LocalDate.parse(value.toString());
        ps.setDate(index, java.sql.Date.valueOf(localDate));
        break;
      case Types.TIME:
        LocalTime localTime = LocalTime.parse(value.toString());
        ps.setTime(index, java.sql.Time.valueOf(localTime));
        break;
      case Types.TIMESTAMP:
        LocalDateTime localDateTime =
            LocalDateTime.parse(
                value.toString(), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        ps.setTimestamp(index, java.sql.Timestamp.valueOf(localDateTime));
        break;
      case Types.DECIMAL:
        ps.setBigDecimal(index, (BigDecimal) value);
        break;
      default:
        throw new StepExecutionException(
            "Unsupported data type, Unexpected value: [" + fieldType + "]");
    }
  }

  private String generateSQL() {
    JdbcSinkConfig.JdbcSinkMode mode = options.getMode();
    switch (mode) {
      case INSERT:
        return jdbcDialect.getInsertIntoStatement(
            options.getDatabase(), options.getTable(), options.getFields());
      case UPDATE:
        return jdbcDialect.getUpdateStatement(
            options.getDatabase(),
            options.getTable(),
            options.getFields(),
            options.getConditionFields());
      case UPSERT:
        return jdbcDialect
            .getUpsertStatement(
                options.getDatabase(),
                options.getTable(),
                options.getFields(),
                options.getPrimaryKeys())
            .orElse(null);
    }
    return null;
  }

  private void flush() throws StepExecutionException {
    try {
      ps.executeBatch();
      ps.clearBatch();
      connection.commit();
    } catch (Exception e) {
      throw new StepExecutionException(e);
    }
  }
}
