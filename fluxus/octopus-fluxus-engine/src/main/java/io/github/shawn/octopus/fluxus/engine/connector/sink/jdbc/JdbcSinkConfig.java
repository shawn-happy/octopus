package io.github.shawn.octopus.fluxus.engine.connector.sink.jdbc;

import static io.github.shawn.octopus.fluxus.api.common.PredicateUtils.verify;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.shawn.octopus.fluxus.api.common.IdGenerator;
import io.github.shawn.octopus.fluxus.api.common.JsonUtils;
import io.github.shawn.octopus.fluxus.api.config.BaseSinkConfig;
import io.github.shawn.octopus.fluxus.api.config.SinkConfig;
import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import io.github.shawn.octopus.fluxus.engine.common.Constants;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class JdbcSinkConfig extends BaseSinkConfig<JdbcSinkConfig.JdbcSinkOptions> {
  private String id;
  private String name;
  @Builder.Default private String identifier = Constants.SinkConstants.JDBC_SINK;
  private String input;
  private JdbcSinkOptions options;

  @Override
  protected void checkOptions() {
    verify(StringUtils.isNotBlank(options.getUrl()), "jdbc url cannot be null");
    verify(StringUtils.isNotBlank(options.getDriver()), "jdbc driver url cannot be null");
    verify(StringUtils.isNotBlank(options.getTable()), "jdbc table cannot be null");
    JdbcSinkMode mode = options.getMode();
    switch (mode) {
      case INSERT:
        verify(ArrayUtils.isNotEmpty(options.getFields()), "insert fields cannot be empty");
        break;
      case UPDATE:
        verify(ArrayUtils.isNotEmpty(options.getFields()), "update fields cannot be empty");
        break;
      case UPSERT:
        verify(
            ArrayUtils.isNotEmpty(options.getFields())
                && ArrayUtils.isNotEmpty(options.getPrimaryKeys()),
            "upsert fields or unique key cannot be null where jdbc sink mode is upsert");
        break;
    }
  }

  @Override
  protected void loadSinkConfig(String json) {
    JdbcSinkConfig jdbcSinkConfig =
        JsonUtils.fromJson(json, new TypeReference<JdbcSinkConfig>() {})
            .orElseThrow(
                () ->
                    new DataWorkflowException(
                        String.format("jdbc sink config parse error. %s", json)));
    this.id =
        StringUtils.isNotBlank(jdbcSinkConfig.getId())
            ? jdbcSinkConfig.getId()
            : IdGenerator.uuid();
    this.name = jdbcSinkConfig.getName();
    this.input = jdbcSinkConfig.getInput();
    this.options = jdbcSinkConfig.getOptions();
  }

  @Builder
  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class JdbcSinkOptions implements SinkConfig.SinkOptions {
    private String url;
    private String username;
    private String password;
    private String driver;
    private String database;
    private String table;
    private JdbcSinkMode mode;
    private String[] fields;
    private String[] conditionFields;
    private String[] primaryKeys;
    @Builder.Default private transient int batchSize = 1000;
  }

  public static enum JdbcSinkMode {
    /** 仅插入 */
    INSERT,
    /** 仅更新 */
    UPDATE,
    /** 存在则更新，不存在则插入 */
    UPSERT,
    ;
  }
}
