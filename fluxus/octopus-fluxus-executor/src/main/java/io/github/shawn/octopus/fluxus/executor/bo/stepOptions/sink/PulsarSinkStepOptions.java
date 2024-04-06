package io.github.shawn.octopus.fluxus.executor.bo.stepOptions.sink;

import static io.github.shawn.octopus.fluxus.engine.common.Constants.SinkConstants.PULSAR;

import io.github.shawn.octopus.fluxus.api.config.PluginType;
import io.github.shawn.octopus.fluxus.executor.bo.DataFormat;
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
@JsonTypeName("sink_pulsar")
@NoArgsConstructor
@AllArgsConstructor
public class PulsarSinkStepOptions implements StepOptions {
  @Builder.Default private final PluginType pluginType = PluginType.SOURCE;
  @Builder.Default private final String identify = PULSAR;

  private String clientUrl;
  private String adminUrl;
  private String token;
  private String topic;
  private String producer;
  @Builder.Default private DataFormat format = DataFormat.JSON;
  private char delimiter;
  private String arrayElementDelimiter;

  @Override
  public List<StepAttribute> getStepAttributes() {
    return Arrays.asList(
        StepAttribute.builder().code("clientUrl").value(getClientUrl()).build(),
        StepAttribute.builder().code("adminUrl").value(getAdminUrl()).build(),
        StepAttribute.builder().code("token").value(getToken()).build(),
        StepAttribute.builder().code("topic").value(getTopic()).build(),
        StepAttribute.builder().code("producer").value(getProducer()).build(),
        StepAttribute.builder().code("format").value(getFormat()).build(),
        StepAttribute.builder().code("delimiter").value(getDelimiter()).build(),
        StepAttribute.builder()
            .code("arrayElementDelimiter")
            .value(getArrayElementDelimiter())
            .build());
  }
}
