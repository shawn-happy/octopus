package io.github.octopus.actus.plugin.mysql.dialect;

import io.github.octopus.actus.core.model.DatabaseIdentifier;
import io.github.octopus.actus.core.model.FieldIdeEnum;
import io.github.octopus.actus.core.model.schema.FieldType;
import io.github.octopus.actus.core.model.schema.TableEngine;
import io.github.octopus.actus.plugin.api.dialect.JdbcDialect;
import io.github.octopus.actus.plugin.api.executor.AbstractCurdExecutor;
import io.github.octopus.actus.plugin.api.executor.AbstractDDLExecutor;
import io.github.octopus.actus.plugin.api.executor.AbstractMetaDataExecutor;
import io.github.octopus.actus.plugin.mysql.executor.MySQLCurdExecutor;
import io.github.octopus.actus.plugin.mysql.executor.MySQLDDLExecutor;
import io.github.octopus.actus.plugin.mysql.executor.MySQLMetaDataExecutor;
import io.github.octopus.actus.plugin.mysql.model.MySQLFieldType;
import io.github.octopus.actus.plugin.mysql.model.MySQLTableEngine;
import java.util.Arrays;
import java.util.List;
import javax.sql.DataSource;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

@Getter
public class MySQLJdbcDialect implements JdbcDialect {

  private final String dialectName = DatabaseIdentifier.MYSQL;
  private final FieldIdeEnum fieldIde;
  private final List<FieldType> supportedFieldTypes = Arrays.asList(MySQLFieldType.values());
  private final List<TableEngine> supportedTableEngines = Arrays.asList(MySQLTableEngine.values());

  public MySQLJdbcDialect() {
    this.fieldIde = FieldIdeEnum.ORIGINAL;
  }

  public MySQLJdbcDialect(FieldIdeEnum fieldIde) {
    this.fieldIde = fieldIde;
  }

  @Override
  public AbstractCurdExecutor createCurdExecutor(DataSource dataSource) {
    return new MySQLCurdExecutor(dataSource);
  }

  @Override
  public AbstractDDLExecutor createDDLExecutor(DataSource dataSource) {
    return new MySQLDDLExecutor(dataSource);
  }

  @Override
  public AbstractMetaDataExecutor createMetaDataExecutor(DataSource dataSource) {
    return new MySQLMetaDataExecutor(dataSource);
  }

  @Override
  public String buildPageSql(String sql, long offset, long limit) {
    return "";
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
