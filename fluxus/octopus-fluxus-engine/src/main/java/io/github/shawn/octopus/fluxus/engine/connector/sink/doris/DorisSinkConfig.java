package io.github.shawn.octopus.fluxus.engine.connector.sink.doris;

import static io.github.shawn.octopus.fluxus.api.common.PredicateUtils.verify;
import static io.github.shawn.octopus.fluxus.engine.connector.sink.doris.DorisConstants.FORMAT_KEY;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.shawn.octopus.fluxus.api.common.IdGenerator;
import io.github.shawn.octopus.fluxus.api.common.JsonUtils;
import io.github.shawn.octopus.fluxus.api.config.BaseSinkConfig;
import io.github.shawn.octopus.fluxus.api.config.SinkConfig;
import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import io.github.shawn.octopus.fluxus.engine.common.Constants;
import java.util.Map;
import java.util.Properties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DorisSinkConfig extends BaseSinkConfig<DorisSinkConfig.DorisSinkOptions>
    implements SinkConfig<DorisSinkConfig.DorisSinkOptions> {

  private String id;
  private String name;
  @Default private String identifier = Constants.SinkConstants.DORIS_SINK;
  private String input;
  private DorisSinkOptions options;

  @Override
  protected void checkOptions() {
    verify(StringUtils.isNotBlank(options.getJdbcUrl()), "doris jdbc url cannot be null");
    verify(StringUtils.isNotBlank(options.getFeNodes()), "doris fe url cannot be null");
    verify(StringUtils.isNotBlank(options.getUsername()), "doris username cannot be null");
    verify(StringUtils.isNotBlank(options.getDatabase()), "doris database cannot be null");
    verify(StringUtils.isNotBlank(options.getTable()), "doris table cannot be null");
    verify(ArrayUtils.isNotEmpty(options.getColumns()), "doris columns cannot be null");
  }

  @Override
  protected void loadSinkConfig(String json) {
    DorisSinkConfig dorisSinkConfig =
        JsonUtils.fromJson(json, new TypeReference<DorisSinkConfig>() {})
            .orElseThrow(
                () ->
                    new DataWorkflowException(
                        String.format("console sink config parse error. %s", json)));
    this.id =
        StringUtils.isNotBlank(dorisSinkConfig.getId())
            ? dorisSinkConfig.getId()
            : IdGenerator.uuid();
    this.name = dorisSinkConfig.getName();
    this.input = dorisSinkConfig.getInput();
    this.options = dorisSinkConfig.getOptions();
  }

  @Getter
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class DorisSinkOptions implements SinkConfig.SinkOptions {

    private static final int DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT = 30 * 1000;
    private static final int DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT = 30 * 1000;
    private static final int DORIS_REQUEST_QUERY_TIMEOUT_S_DEFAULT = 3600;
    private static final int DORIS_REQUEST_RETRIES_DEFAULT = 3;
    private static final int DEFAULT_SINK_CHECK_INTERVAL = 10000;
    private static final int DEFAULT_SINK_MAX_RETRIES = 3;
    private static final int DEFAULT_SINK_BUFFER_SIZE = 256 * 1024;
    private static final int DEFAULT_SINK_BUFFER_COUNT = 3;

    private String jdbcUrl;
    private String feNodes;
    private String username;
    private String password;
    private String database;
    private String table;
    @Default private final transient String driver = "com.mysql.cj.jdbc.Driver";
    private String[] columns;
    private int maxRetries;
    private String labelPrefix;
    private boolean enable2PC;
    private boolean enableDelete;
    @Default private int bufferSize = DEFAULT_SINK_BUFFER_SIZE;
    @Default private int bufferCount = DEFAULT_SINK_BUFFER_COUNT;
    @Default private Integer requestConnectTimeoutMs = DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT;
    @Default private Integer requestReadTimeoutMs = DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT;
    @Default private Integer requestQueryTimeoutS = DORIS_REQUEST_QUERY_TIMEOUT_S_DEFAULT;
    @Default private Integer requestRetries = DORIS_REQUEST_RETRIES_DEFAULT;

    @Default private long flushInterval = DEFAULT_SINK_CHECK_INTERVAL;
    private Map<String, Object> extraOptions;

    public StreamLoadFormat getStreamLoadFormat() {
      Map<String, Object> loadProps = getExtraOptions();
      if (null == loadProps) {
        return StreamLoadFormat.CSV;
      }
      if (loadProps.containsKey(FORMAT_KEY)
          && StreamLoadFormat.JSON
              .name()
              .equalsIgnoreCase(String.valueOf(loadProps.get(FORMAT_KEY)))) {
        return StreamLoadFormat.JSON;
      }
      return StreamLoadFormat.CSV;
    }

    public Properties getStreamLoadProps() {
      Properties properties = new Properties();
      if (MapUtils.isNotEmpty(extraOptions)) {
        properties = MapUtils.toProperties(extraOptions);
      }
      return properties;
    }

    public String getTableIdentifier() {
      return database + "." + table;
    }
  }
}
