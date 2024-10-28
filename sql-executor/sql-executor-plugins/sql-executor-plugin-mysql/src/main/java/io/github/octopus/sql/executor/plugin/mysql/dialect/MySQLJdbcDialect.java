package io.github.octopus.sql.executor.plugin.mysql.dialect;

import io.github.octopus.sql.executor.core.model.DatabaseIdentifier;
import io.github.octopus.sql.executor.core.model.FieldIdeEnum;
import io.github.octopus.sql.executor.core.model.schema.FieldType;
import io.github.octopus.sql.executor.core.model.schema.TableEngine;
import io.github.octopus.sql.executor.plugin.api.dialect.JdbcDialect;
import io.github.octopus.sql.executor.plugin.api.dialect.JdbcType;
import io.github.octopus.sql.executor.plugin.api.executor.CurdExecutor;
import io.github.octopus.sql.executor.plugin.api.executor.DDLExecutor;
import io.github.octopus.sql.executor.plugin.api.executor.MetaDataExecutor;
import io.github.octopus.sql.executor.plugin.mysql.executor.MySQLCurdExecutor;
import io.github.octopus.sql.executor.plugin.mysql.executor.MySQLDDLExecutor;
import io.github.octopus.sql.executor.plugin.mysql.executor.MySQLMetaDataExecutor;
import io.github.octopus.sql.executor.plugin.mysql.model.MySQLFieldType;
import io.github.octopus.sql.executor.plugin.mysql.model.MySQLTableEngine;
import java.util.Arrays;
import java.util.List;
import javax.sql.DataSource;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

@Getter
public class MySQLJdbcDialect implements JdbcDialect {

  private String fieldIde = FieldIdeEnum.ORIGINAL.getValue();

  private final String dialectName = DatabaseIdentifier.MYSQL;
  private final JdbcType jdbcType = getJdbcType();
  private final List<FieldType> supportedFieldTypes = Arrays.asList(MySQLFieldType.values());
  private final List<TableEngine> supportedTableEngines = Arrays.asList(MySQLTableEngine.values());

  public MySQLJdbcDialect() {}

  public MySQLJdbcDialect(String fieldIde) {
    this.fieldIde = fieldIde;
  }

  @Override
  public CurdExecutor createCurdExecutor(String name, DataSource dataSource) {
    return new MySQLCurdExecutor(name, dataSource);
  }

  @Override
  public DDLExecutor createDDLExecutor(String name, DataSource dataSource) {
    return new MySQLDDLExecutor(name, dataSource);
  }

  @Override
  public MetaDataExecutor createMetaDataExecutor(String name, DataSource dataSource) {
    return new MySQLMetaDataExecutor(name, dataSource);
  }

  @Override
  public String quoteIdentifier(String identifier) {
    return "`" + getFieldIde(identifier, fieldIde) + "`";
  }

  @Override
  public String quoteDatabaseIdentifier(String identifier) {
    return "`" + identifier + "`";
  }

  @Override
  public String getUrl(String host, int port, String database, String suffix) {
    // jdbc:mysql://192.168.5.51:3306/test_shawn?characterEncoding=utf-8&useSSL=false
    StringBuilder url = new StringBuilder("jdbc:mysql://").append(host).append(":").append(port);
    if (StringUtils.isNotBlank(database)) {
      url.append("/").append(database);
    }
    if (StringUtils.isNotBlank(suffix)) {
      url.append("?").append(suffix);
    }
    return url.toString();
  }
}
