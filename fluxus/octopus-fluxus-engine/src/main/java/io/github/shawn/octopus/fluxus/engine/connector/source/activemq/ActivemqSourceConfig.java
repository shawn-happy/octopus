package io.github.shawn.octopus.fluxus.engine.connector.source.activemq;

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
import javax.jms.Session;
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
public class ActivemqSourceConfig
    extends BaseSourceConfig<ActivemqSourceConfig.ActivemqSourceOptions>
    implements SourceConfig<ActivemqSourceConfig.ActivemqSourceOptions> {

  @JsonProperty("type")
  @Builder.Default
  private final transient String identifier = Constants.SourceConstants.ACTIVEMQ_SOURCE;

  private String id;
  private String name;

  private String output;
  private ActivemqSourceConfig.ActivemqSourceOptions options;
  private List<Column> columns;

  @Override
  protected void checkOptions() {
    verify(StringUtils.isNotBlank(options.getBrokerUrl()), "activemq brokerUrl cannot be null");
    verify(StringUtils.isNotBlank(options.getUserName()), "activemq userName cannot be null");
    verify(StringUtils.isNotBlank(options.getPassword()), "activemq password cannot be null");
  }

  @Override
  protected void loadSourceConfig(String json) {
    ActivemqSourceConfig sourceConfig =
        JsonUtils.fromJson(json, new TypeReference<ActivemqSourceConfig>() {})
            .orElseThrow(
                () ->
                    new DataWorkflowException(
                        String.format("activemq config parse error. %s", json)));
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
  public static class ActivemqSourceOptions implements SourceConfig.SourceOptions {
    private String userName;
    private String password;
    private String brokerUrl;
    private String queueName;
    // 队列类型，支持topic和queue;默认queue,
    @Builder.Default private final String type = "queue";
    // 是否开启事务 默认不开启
    @Builder.Default private final boolean transacted = false;
    // ack确认模式，默认客户端手动确认
    @Builder.Default private final int acknowledgeMode = Session.CLIENT_ACKNOWLEDGE;
    @Builder.Default private final int commitSize = 1000;
  }
}
