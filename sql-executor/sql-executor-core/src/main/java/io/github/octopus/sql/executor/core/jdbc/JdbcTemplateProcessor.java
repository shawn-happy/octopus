package io.github.octopus.sql.executor.core.jdbc;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@Slf4j
public class JdbcTemplateProcessor {

  private final NamedParameterJdbcTemplate jdbcTemplate;
  private final TransactionTemplate transactionTemplate;

  public JdbcTemplateProcessor(DataSource dataSource) {
    this.jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
    PlatformTransactionManager platformTransactionManager =
        new DataSourceTransactionManager(dataSource);
    this.transactionTemplate = new TransactionTemplate(platformTransactionManager);
  }

  public void execute(String sql) {
    jdbcTemplate.getJdbcTemplate().execute(sql);
  }

  public <R> R queryOne(String sql, Class<R> returnType) {
    logDebugSql(sql, null);
    return jdbcTemplate.getJdbcTemplate().queryForObject(sql, returnType);
  }

  public <R> R queryOne(String sql, Object[] args, Class<R> returnType) {
    logDebugSql(sql, args);
    return jdbcTemplate.getJdbcTemplate().queryForObject(sql, returnType, args);
  }

  public <R> R queryOne(String sql, Map<String, Object> args, Class<R> returnType) {
    logDebugSql(sql, args);
    return jdbcTemplate.queryForObject(sql, args, returnType);
  }

  public <R> R queryOne(String sql, Object[] args, RowMapper<R> rowMapper) {
    logDebugSql(sql, args);
    return jdbcTemplate.getJdbcTemplate().queryForObject(sql, rowMapper, args);
  }

  public <R> R queryOne(String sql, Map<String, Object> args, RowMapper<R> rowMapper) {
    logDebugSql(sql, args);
    return jdbcTemplate.queryForObject(sql, args, rowMapper);
  }

  public Map<String, Object> queryOne(String sql, Map<String, Object> args) {
    logDebugSql(sql, args);
    return jdbcTemplate.queryForMap(sql, args);
  }

  public <R> List<R> queryList(String sql, Class<R> returnType) {
    logDebugSql(sql, null);
    return jdbcTemplate.getJdbcTemplate().queryForList(sql, returnType);
  }

  public <R> List<R> queryList(String sql, Object[] args, Class<R> returnType) {
    logDebugSql(sql, args);
    return jdbcTemplate.getJdbcTemplate().queryForList(sql, returnType, args);
  }

  public <R> List<R> queryList(String sql, Object[] args, RowMapper<R> rowMapper) {
    logDebugSql(sql, args);
    return jdbcTemplate.getJdbcTemplate().query(sql, rowMapper, args);
  }

  public <R> List<R> queryList(String sql, Map<String, Object> args, Class<R> returnType) {
    logDebugSql(sql, args);
    return jdbcTemplate.queryForList(sql, args, returnType);
  }

  public <R> List<R> queryList(String sql, Map<String, Object> args, RowMapper<R> rowMapper) {
    logDebugSql(sql, args);
    return jdbcTemplate.query(sql, args, rowMapper);
  }

  public List<Map<String, Object>> queryList(String sql, Object[] args) {
    logDebugSql(sql, args);
    return jdbcTemplate.getJdbcTemplate().queryForList(sql, args);
  }

  public List<Map<String, Object>> queryList(String sql, Map<String, Object> args) {
    logDebugSql(sql, args);
    return jdbcTemplate.queryForList(sql, args);
  }

  public int update(String sql, Map<String, Object> args) {
    logDebugSql(sql, args);
    Integer count =
        transactionTemplate.execute(transactionStatus -> jdbcTemplate.update(sql, args));
    return count == null ? 0 : count;
  }

  public int updateBatch(String sql, List<Object[]> args) {
    logDebugSql(sql, args);
    int[] count =
        transactionTemplate.execute(
            transactionStatus -> jdbcTemplate.getJdbcTemplate().batchUpdate(sql, args));
    return count == null ? 0 : Arrays.stream(count).sum();
  }

  public int[] updateBatch(String sql, Map<String, Object>[] args) {
    logDebugSql(sql, args);
    int[] count =
        transactionTemplate.execute(transactionStatus -> jdbcTemplate.batchUpdate(sql, args));
    return count == null ? new int[0] : count;
  }

  @SuppressWarnings("unchecked")
  private void logDebugSql(String sql, Object args) {
    if (ObjectUtils.isEmpty(args)) {
      log.debug("execute sql: {}", sql);
    } else {
      if (args instanceof Map) {
        Map<String, Object> params = (Map<String, Object>) args;
        log.debug(
            "sql: {}, \nparams: {}",
            sql,
            params
                .entrySet()
                .stream()
                .map(entry -> String.format("%s=%s", entry.getKey(), entry.getValue()))
                .collect(Collectors.joining(", ")));
      } else if (args instanceof Object[]) {
        Object[] params = (Object[]) args;
        log.debug(
            "sql: {}, \nparams: {}",
            sql,
            Arrays.stream(params).map(Object::toString).collect(Collectors.joining(", ")));
      } else if (args instanceof List) {
        List<Object[]> params = (List<Object[]>) args;
        log.debug(
            "sql: {}, \nparams: {}",
            sql,
            params.stream().map(Arrays::toString).collect(Collectors.joining(", ")));
      }
    }
  }
}
