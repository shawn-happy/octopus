package io.github.octopus.sql.executor.plugin.sqlserver.dialect;

import io.github.octopus.sql.executor.core.model.DatabaseIdentifier;
import io.github.octopus.sql.executor.core.model.FieldIdeEnum;
import io.github.octopus.sql.executor.core.model.schema.FieldType;
import io.github.octopus.sql.executor.plugin.api.dialect.JdbcDialect;
import io.github.octopus.sql.executor.plugin.api.executor.AbstractCurdExecutor;
import io.github.octopus.sql.executor.plugin.api.executor.AbstractDDLExecutor;
import io.github.octopus.sql.executor.plugin.api.executor.AbstractMetaDataExecutor;
import io.github.octopus.sql.executor.plugin.sqlserver.executor.SqlServerCurdExecutor;
import io.github.octopus.sql.executor.plugin.sqlserver.executor.SqlServerDDLExecutor;
import io.github.octopus.sql.executor.plugin.sqlserver.executor.SqlServerMetaDataExecutor;
import io.github.octopus.sql.executor.plugin.sqlserver.model.SqlServerFieldType;
import java.util.Arrays;
import java.util.List;
import javax.sql.DataSource;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

@Getter
public class SqlServerJdbcDialect implements JdbcDialect {

  private final String dialectName = DatabaseIdentifier.SQLSERVER;
  private final FieldIdeEnum fieldIde;
  private final List<FieldType> supportedFieldTypes = Arrays.asList(SqlServerFieldType.values());

  public SqlServerJdbcDialect() {
    this.fieldIde = FieldIdeEnum.ORIGINAL;
  }

  public SqlServerJdbcDialect(FieldIdeEnum fieldIde) {
    this.fieldIde = fieldIde;
  }

  @Override
  public AbstractCurdExecutor createCurdExecutor(DataSource dataSource) {
    return new SqlServerCurdExecutor(dataSource);
  }

  @Override
  public AbstractDDLExecutor createDDLExecutor(DataSource dataSource) {
    return new SqlServerDDLExecutor(dataSource);
  }

  @Override
  public AbstractMetaDataExecutor createMetaDataExecutor(DataSource dataSource) {
    return new SqlServerMetaDataExecutor(dataSource);
  }

  @Override
  public String buildPageSql(String sql, long offset, long limit) {
    return "";
  }

  @Override
  public String quoteLeft() {
    return "[";
  }

  @Override
  public String quoteRight() {
    return "]";
  }

  @Override
  public String getUrl(String host, int port, String database, String suffix) {
    // jdbc:sqlserver://192.168.5.61:1433;database=test_shawn;encrypt=false;trustServerCertificate=false
    StringBuilder url =
        new StringBuilder("jdbc:sqlserver://").append(host).append(":").append(port);
    if (StringUtils.isNotBlank(database)) {
      url.append(";database=").append(database);
    }
    if (StringUtils.isNotBlank(suffix)) {
      url.append(";").append(suffix);
    }
    return url.toString();
  }
}
