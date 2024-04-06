package io.github.shawn.octopus.fluxus.executor.bo.stepOptions.source;

import static io.github.shawn.octopus.fluxus.engine.common.Constants.SourceConstants.RABBITMQ_SOURCE;

import io.github.shawn.octopus.fluxus.api.config.PluginType;
import io.github.shawn.octopus.fluxus.executor.bo.StepAttribute;
import io.github.shawn.octopus.fluxus.executor.bo.StepOptions;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.Arrays;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@JsonTypeName("source_rabbitmq")
@NoArgsConstructor
@AllArgsConstructor
public class RabbitMqSourceStepOptions implements StepOptions {
  @Builder.Default private final PluginType pluginType = PluginType.SOURCE;
  @Builder.Default private final String identify = RABBITMQ_SOURCE;
  private String username;
  private String password;
  private String host;
  private Integer port;
  private String virtualhost;
  private String queue;
  private String exchange;
  private String routing;
  @Builder.Default private final int connectionTimeout = 500;

  @Override
  public List<StepAttribute> getStepAttributes() {
    return Arrays.asList(
        StepAttribute.builder().code("username").value(getUsername()).build(),
        StepAttribute.builder().code("password").value(getPassword()).build(),
        StepAttribute.builder().code("host").value(getHost()).build(),
        StepAttribute.builder().code("port").value(getPort()).build(),
        StepAttribute.builder().code("virtualhost").value(getVirtualhost()).build(),
        StepAttribute.builder().code("queue").value(getQueue()).build(),
        StepAttribute.builder().code("exchangeName").value(getExchange()).build(),
        StepAttribute.builder().code("routing").value(getRouting()).build(),
        StepAttribute.builder().code("connectionTimeout").value(getConnectionTimeout()).build());
  }
}
