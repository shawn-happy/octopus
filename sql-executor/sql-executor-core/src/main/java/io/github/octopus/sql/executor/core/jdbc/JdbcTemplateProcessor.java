package io.github.octopus.sql.executor.core.jdbc;

import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

public class JdbcTemplateProcessor {

  private final NamedParameterJdbcTemplate jdbcTemplate;
  private final TransactionTemplate transactionTemplate;

  public JdbcTemplateProcessor(DataSource dataSource) {
    this.jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
    PlatformTransactionManager platformTransactionManager =
        new DataSourceTransactionManager(dataSource);
    this.transactionTemplate = new TransactionTemplate(platformTransactionManager);
  }

  public int executeUpdate(String sql, Map<String, Object> params) {
    Integer count =
        transactionTemplate.execute(transactionStatus -> jdbcTemplate.update(sql, params));
    return count == null ? 0 : count;
  }

  public int[] executeUpdateBatch(String sql, Map<String, Object>[] params) {
    int[] count =
        transactionTemplate.execute(transactionStatus -> jdbcTemplate.batchUpdate(sql, params));
    return count == null ? new int[0] : count;
  }

  public List<Map<String, Object>> executeQuery(String sql, Map<String, Object> params) {
    return jdbcTemplate.queryForList(sql, params);
  }
}
