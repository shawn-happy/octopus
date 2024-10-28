package io.github.octopus.sql.executor.plugin.sqlserver.dialect;

import io.github.octopus.sql.executor.core.model.DatabaseIdentifier;
import io.github.octopus.sql.executor.core.model.FieldIdeEnum;
import io.github.octopus.sql.executor.core.model.schema.FieldType;
import io.github.octopus.sql.executor.plugin.api.dialect.JdbcDialect;
import io.github.octopus.sql.executor.plugin.api.dialect.JdbcType;
import io.github.octopus.sql.executor.plugin.api.executor.CurdExecutor;
import io.github.octopus.sql.executor.plugin.api.executor.DDLExecutor;
import io.github.octopus.sql.executor.plugin.api.executor.MetaDataExecutor;
import io.github.octopus.sql.executor.plugin.sqlserver.model.SqlServerFieldType;
import java.util.Arrays;
import java.util.List;
import javax.sql.DataSource;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

@Getter
public class SqlServerJdbcDialect implements JdbcDialect {
  private String fieldIde = FieldIdeEnum.ORIGINAL.getValue();

  private final String dialectName = DatabaseIdentifier.SQLSERVER;
  private final JdbcType jdbcType = SqlServerJdbcType.getJdbcType();
  private final List<FieldType> supportedFieldTypes = Arrays.asList(SqlServerFieldType.values());

  public SqlServerJdbcDialect() {}

  public SqlServerJdbcDialect(final String fieldIde) {
    this.fieldIde = fieldIde;
  }

  @Override
  public CurdExecutor createCurdExecutor(String name, DataSource dataSource) {
    return null;
  }

  @Override
  public DDLExecutor createDDLExecutor(String name, DataSource dataSource) {
    return null;
  }

  @Override
  public MetaDataExecutor createMetaDataExecutor(String name, DataSource dataSource) {
    return null;
  }

  @Override
  public String quoteIdentifier(String identifier) {
    if (identifier.contains(".")) {
      String[] parts = identifier.split("\\.");
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < parts.length - 1; i++) {
        sb.append("[").append(parts[i]).append("]").append(".");
      }
      return sb.append("[")
          .append(getFieldIde(parts[parts.length - 1], fieldIde))
          .append("]")
          .toString();
    }

    return "[" + getFieldIde(identifier, fieldIde) + "]";
  }

  @Override
  public String quoteDatabaseIdentifier(String identifier) {
    return "[" + identifier + "]";
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
