package io.github.octopus.sql.executor.plugin.sqlserver.dialect;

import com.baomidou.mybatisplus.annotation.DbType;
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
  private String fieldIde = FieldIdeEnum.ORIGINAL.getValue();

  private final String dialectName = DatabaseIdentifier.SQLSERVER;
  private final DbType mybatisDbType = DbType.SQL_SERVER;
  private final List<FieldType> supportedFieldTypes = Arrays.asList(SqlServerFieldType.values());

  public SqlServerJdbcDialect() {}

  public SqlServerJdbcDialect(final String fieldIde) {
    this.fieldIde = fieldIde;
  }

  @Override
  public AbstractCurdExecutor createCurdExecutor(String name, DataSource dataSource) {
    return new SqlServerCurdExecutor(name, dataSource);
  }

  @Override
  public AbstractDDLExecutor createDDLExecutor(String name, DataSource dataSource) {
    return new SqlServerDDLExecutor(name, dataSource);
  }

  @Override
  public AbstractMetaDataExecutor createMetaDataExecutor(String name, DataSource dataSource) {
    return new SqlServerMetaDataExecutor(name, dataSource);
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
