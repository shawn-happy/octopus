package io.github.shawn.octopus.fluxus.executor.bo.stepOptions.source;

import static io.github.shawn.octopus.fluxus.engine.common.Constants.SourceConstants.PULSAR_SOURCE;

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

@Builder
@Getter
@JsonTypeName("source_pulsar")
@NoArgsConstructor
@AllArgsConstructor
public class PulsarSourceStepOptions implements StepOptions {
  @Builder.Default private final PluginType pluginType = PluginType.SOURCE;
  @Builder.Default private final String identify = PULSAR_SOURCE;
  private String clientUrl;
  private String adminUrl;
  private String token;
  private String topic;
  private String subscription;
  @Builder.Default private String subscriptionType = "Earliest";

  @Override
  public List<StepAttribute> getStepAttributes() {
    return Arrays.asList(
        StepAttribute.builder().code("clientUrl").value(getClientUrl()).build(),
        StepAttribute.builder().code("adminUrl").value(getAdminUrl()).build(),
        StepAttribute.builder().code("token").value(getToken()).build(),
        StepAttribute.builder().code("topic").value(getTopic()).build(),
        StepAttribute.builder().code("subscription").value(getSubscription()).build(),
        StepAttribute.builder().code("subscriptionType").value(getSubscriptionType()).build());
  }
}
