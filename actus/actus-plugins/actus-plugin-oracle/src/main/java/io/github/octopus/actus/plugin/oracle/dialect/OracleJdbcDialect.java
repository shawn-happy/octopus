package io.github.octopus.actus.plugin.oracle.dialect;

import io.github.octopus.actus.core.model.DatabaseIdentifier;
import io.github.octopus.actus.core.model.FieldIdeEnum;
import io.github.octopus.actus.core.model.schema.FieldType;
import io.github.octopus.actus.plugin.api.dialect.JdbcDialect;
import io.github.octopus.actus.plugin.api.executor.AbstractCurdExecutor;
import io.github.octopus.actus.plugin.api.executor.AbstractDDLExecutor;
import io.github.octopus.actus.plugin.api.executor.AbstractMetaDataExecutor;
import io.github.octopus.actus.plugin.oracle.executor.OracleDDLExecutor;
import io.github.octopus.actus.plugin.oracle.executor.OracleMetaDataExecutor;
import io.github.octopus.actus.plugin.oracle.executor.OracleCurdExecutor;
import io.github.octopus.actus.plugin.oracle.model.OracleFieldType;
import java.util.Arrays;
import java.util.List;
import javax.sql.DataSource;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

@Getter
public class OracleJdbcDialect implements JdbcDialect {

  private final String dialectName = DatabaseIdentifier.ORACLE;
  private final FieldIdeEnum fieldIde;
  private final List<FieldType> supportedFieldTypes = Arrays.asList(OracleFieldType.values());

  public OracleJdbcDialect() {
    this.fieldIde = FieldIdeEnum.UPPERCASE;
  }

  public OracleJdbcDialect(FieldIdeEnum fieldIde) {
    this.fieldIde = fieldIde;
  }

  @Override
  public AbstractCurdExecutor createCurdExecutor(DataSource dataSource) {
    return new OracleCurdExecutor(dataSource);
  }

  @Override
  public AbstractDDLExecutor createDDLExecutor(DataSource dataSource) {
    return new OracleDDLExecutor(dataSource);
  }

  @Override
  public AbstractMetaDataExecutor createMetaDataExecutor(DataSource dataSource) {
    return new OracleMetaDataExecutor(dataSource);
  }

  @Override
  public String quoteLeft() {
    return "\"";
  }

  @Override
  public String quoteRight() {
    return "\"";
  }

  @Override
  public String buildPageSql(String sql, long offset, long limit) {
    return "";
  }

  @Override
  public String getUrl(String host, int port, String database, String suffix) {
    // jdbc:oracle:thin:@192.168.5.75:1521:orcl
    StringBuilder url =
        new StringBuilder("jdbc:oracle:thin:@").append(host).append(":").append(port);
    if (StringUtils.isNotBlank(database)) {
      url.append(":").append(database);
    }
    if (StringUtils.isNotBlank(suffix)) {
      url.append("?").append(suffix);
    }
    return url.toString();
  }
}
