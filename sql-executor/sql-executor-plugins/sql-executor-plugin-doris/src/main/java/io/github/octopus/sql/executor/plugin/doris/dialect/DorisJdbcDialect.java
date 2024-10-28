package io.github.octopus.sql.executor.plugin.doris.dialect;

import io.github.octopus.sql.executor.core.model.DatabaseIdentifier;
import io.github.octopus.sql.executor.core.model.FieldIdeEnum;
import io.github.octopus.sql.executor.core.model.schema.FieldType;
import io.github.octopus.sql.executor.core.model.schema.TableEngine;
import io.github.octopus.sql.executor.plugin.api.dialect.JdbcDialect;
import io.github.octopus.sql.executor.plugin.api.dialect.JdbcType;
import io.github.octopus.sql.executor.plugin.api.executor.CurdExecutor;
import io.github.octopus.sql.executor.plugin.api.executor.DDLExecutor;
import io.github.octopus.sql.executor.plugin.api.executor.MetaDataExecutor;
import io.github.octopus.sql.executor.plugin.doris.executor.DorisCurdExecutor;
import io.github.octopus.sql.executor.plugin.doris.executor.DorisDDLExecutor;
import io.github.octopus.sql.executor.plugin.doris.executor.DorisMetaDataExecutor;
import io.github.octopus.sql.executor.plugin.doris.model.DorisFieldType;
import io.github.octopus.sql.executor.plugin.doris.model.DorisTableEngine;
import java.util.Arrays;
import java.util.List;
import javax.sql.DataSource;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

@Getter
public class DorisJdbcDialect implements JdbcDialect {

  private String fieldIde = FieldIdeEnum.ORIGINAL.getValue();

  private final String dialectName = DatabaseIdentifier.DORIS;
  private final JdbcType jdbcType = DorisJdbcType.getJdbcType();
  private final List<FieldType> supportedFieldTypes = Arrays.asList(DorisFieldType.values());
  private final List<TableEngine> supportedTableEngines = Arrays.asList(DorisTableEngine.values());

  public DorisJdbcDialect() {}

  public DorisJdbcDialect(String fieldIde) {
    this.fieldIde = fieldIde;
  }

  @Override
  public CurdExecutor createCurdExecutor(String name, DataSource dataSource) {
    return new DorisCurdExecutor(name, dataSource);
  }

  @Override
  public DDLExecutor createDDLExecutor(String name, DataSource dataSource) {
    return new DorisDDLExecutor(name, dataSource);
  }

  @Override
  public MetaDataExecutor createMetaDataExecutor(String name, DataSource dataSource) {
    return new DorisMetaDataExecutor(name, dataSource);
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
    // jdbc:mysql://192.168.5.22:9030/information_schema?characterEncoding=utf-8&useSSL=false
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
