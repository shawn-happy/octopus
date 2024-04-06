package io.github.shawn.octopus.fluxus.engine.connector.source.rabbitmq;

import static io.github.shawn.octopus.fluxus.api.common.PredicateUtils.verify;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import io.github.shawn.octopus.fluxus.api.common.IdGenerator;
import io.github.shawn.octopus.fluxus.api.common.JsonUtils;
import io.github.shawn.octopus.fluxus.api.config.BaseSourceConfig;
import io.github.shawn.octopus.fluxus.api.config.Column;
import io.github.shawn.octopus.fluxus.api.config.SourceConfig;
import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import io.github.shawn.octopus.fluxus.engine.common.Constants;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

/** @author zuoyl */
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RabbitmqSourceConfig
    extends BaseSourceConfig<RabbitmqSourceConfig.RabbitmqSourceOptions>
    implements SourceConfig<RabbitmqSourceConfig.RabbitmqSourceOptions> {

  @JsonProperty("type")
  @Builder.Default
  private final transient String identifier = Constants.SourceConstants.RABBITMQ_SOURCE;

  private String id;
  private String name;

  private String output;
  private RabbitmqSourceConfig.RabbitmqSourceOptions options;
  private List<Column> columns;

  @Override
  protected void checkOptions() {
    verify(StringUtils.isNotBlank(options.getHost()), "rabbitmq host cannot be null");
    verify(options.getPort() != null, "rabbitmq port cannot be null");
    verify(StringUtils.isNotBlank(options.getUserName()), "rabbitmq userName cannot be null");
    verify(StringUtils.isNotBlank(options.getPassword()), "rabbitmq password cannot be null");
  }

  @Override
  protected void loadSourceConfig(String json) {
    RabbitmqSourceConfig sourceConfig =
        JsonUtils.fromJson(json, new TypeReference<RabbitmqSourceConfig>() {})
            .orElseThrow(
                () ->
                    new DataWorkflowException(
                        String.format("rabbitmq config parse error. %s", json)));
    this.id =
        StringUtils.isNotBlank(sourceConfig.getId()) ? sourceConfig.getId() : IdGenerator.uuid();
    this.name = sourceConfig.getName();
    this.output = sourceConfig.getOutput();
    this.options = sourceConfig.getOptions();
    this.columns = sourceConfig.getColumns();
  }

  @Getter
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class RabbitmqSourceOptions implements SourceConfig.SourceOptions {
    private String userName;
    private String password;
    private String host;
    private Integer port;
    private String virtualhost;
    private String queueName;
    private String exchangeName;
    private String routingKey;
    @Builder.Default private final int connectionTimeout = 500;
    @Builder.Default private final int commitSize = 1000;
  }
}
