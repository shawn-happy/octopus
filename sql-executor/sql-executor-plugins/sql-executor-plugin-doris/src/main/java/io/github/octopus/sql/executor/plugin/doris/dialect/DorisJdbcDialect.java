package io.github.octopus.sql.executor.plugin.doris.dialect;

import io.github.octopus.sql.executor.core.model.DatabaseIdentifier;
import io.github.octopus.sql.executor.core.model.FieldIdeEnum;
import io.github.octopus.sql.executor.core.model.schema.FieldType;
import io.github.octopus.sql.executor.core.model.schema.TableEngine;
import io.github.octopus.sql.executor.plugin.api.dialect.JdbcDialect;
import io.github.octopus.sql.executor.plugin.api.executor.AbstractCurdExecutor;
import io.github.octopus.sql.executor.plugin.api.executor.AbstractDDLExecutor;
import io.github.octopus.sql.executor.plugin.api.executor.AbstractMetaDataExecutor;
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

  private final String dialectName = DatabaseIdentifier.DORIS;
  private final FieldIdeEnum fieldIde;
  private final List<FieldType> supportedFieldTypes = Arrays.asList(DorisFieldType.values());
  private final List<TableEngine> supportedTableEngines = Arrays.asList(DorisTableEngine.values());

  public DorisJdbcDialect() {
    this.fieldIde = FieldIdeEnum.ORIGINAL;
  }

  public DorisJdbcDialect(FieldIdeEnum fieldIde) {
    this.fieldIde = fieldIde;
  }

  @Override
  public AbstractCurdExecutor createCurdExecutor(DataSource dataSource) {
    return new DorisCurdExecutor(dataSource);
  }

  @Override
  public AbstractDDLExecutor createDDLExecutor(DataSource dataSource) {
    return new DorisDDLExecutor(dataSource);
  }

  @Override
  public AbstractMetaDataExecutor createMetaDataExecutor(DataSource dataSource) {
    return new DorisMetaDataExecutor(dataSource);
  }

  @Override
  public String buildPageSql(String sql, long offset, long limit) {
    return "";
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
