package io.github.shawn.octopus.fluxus.executor.bo.stepOptions.source;

import static io.github.shawn.octopus.fluxus.engine.common.Constants.SourceConstants.ACTIVEMQ_SOURCE;

import io.github.shawn.octopus.fluxus.api.config.PluginType;
import io.github.shawn.octopus.fluxus.executor.bo.StepAttribute;
import io.github.shawn.octopus.fluxus.executor.bo.StepOptions;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.Arrays;
import java.util.List;
import javax.jms.Session;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@JsonTypeName("source_activemq")
@NoArgsConstructor
@AllArgsConstructor
public class ActiveMqSourceStepOptions implements StepOptions {

  @Builder.Default private final PluginType pluginType = PluginType.SOURCE;
  @Builder.Default private final String identify = ACTIVEMQ_SOURCE;
  private String username;
  private String password;
  private String brokerUrl;
  private String queue;
  @Builder.Default private final String type = "queue";
  @Builder.Default private final boolean transacted = false;
  @Builder.Default private final int acknowledgeMode = Session.CLIENT_ACKNOWLEDGE;

  @Override
  public List<StepAttribute> getStepAttributes() {
    return Arrays.asList(
        StepAttribute.builder().code("username").value(getUsername()).build(),
        StepAttribute.builder().code("password").value(getPassword()).build(),
        StepAttribute.builder().code("brokerUrl").value(getBrokerUrl()).build(),
        StepAttribute.builder().code("queue").value(getQueue()).build(),
        StepAttribute.builder().code("type").value(getType()).build(),
        StepAttribute.builder().code("transacted").value(isTransacted()).build(),
        StepAttribute.builder().code("acknowledgeMode").value(getAcknowledgeMode()).build());
  }
}
