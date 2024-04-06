package io.github.shawn.octopus.fluxus.engine.connector.source.jdbc;

import static io.github.shawn.octopus.fluxus.api.common.PredicateUtils.verify;
import static io.github.shawn.octopus.fluxus.engine.common.Constants.SourceConstants.JDBC;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import io.github.shawn.octopus.fluxus.api.common.IdGenerator;
import io.github.shawn.octopus.fluxus.api.common.JsonUtils;
import io.github.shawn.octopus.fluxus.api.config.BaseSourceConfig;
import io.github.shawn.octopus.fluxus.api.config.Column;
import io.github.shawn.octopus.fluxus.api.config.SourceConfig;
import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class JdbcSourceConfig extends BaseSourceConfig<JdbcSourceConfig.JdbcSourceOptions> {

  private String id;
  private String name;

  @JsonProperty("type")
  @Builder.Default
  private transient String identifier = JDBC;

  private String output;
  private JdbcSourceOptions options;
  private List<Column> columns;

  @Override
  protected void checkOptions() {
    verify(StringUtils.isNotBlank(options.getUrl()), "jdbc url cannot be null");
    verify(StringUtils.isNotBlank(options.getDriver()), "jdbc driver url cannot be null");
    verify(StringUtils.isNotBlank(options.getQuery()), "jdbc query sql cannot be null");
  }

  @Override
  protected void loadSourceConfig(String json) {
    JdbcSourceConfig jdbcSourceConfig =
        JsonUtils.fromJson(json, new TypeReference<JdbcSourceConfig>() {})
            .orElseThrow(
                () ->
                    new DataWorkflowException(
                        String.format("jdbc source config deserialize error. json:\n%s", json)));
    this.id =
        StringUtils.isNotBlank(jdbcSourceConfig.getId())
            ? jdbcSourceConfig.getId()
            : IdGenerator.uuid();
    this.name = jdbcSourceConfig.getName();
    this.output = jdbcSourceConfig.getOutput();
    this.options = jdbcSourceConfig.getOptions();
    this.columns = jdbcSourceConfig.getColumns();
  }

  @Builder
  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class JdbcSourceOptions implements SourceConfig.SourceOptions {

    private String url;
    private String username;
    private String password;
    private String driver;
    private String query;
    private Integer limit;
  }
}
