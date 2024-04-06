package io.github.shawn.octopus.fluxus.engine.connector.sink.pulsar;

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
import org.apache.commons.lang3.StringUtils;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class PulsarSinkConfig extends BaseSinkConfig<PulsarSinkConfig.PulsarSinkOptions> {

  private String id;
  private String name;
  @Builder.Default private String identifier = Constants.SinkConstants.PULSAR;
  private String input;
  private PulsarSinkOptions options;

  @Override
  protected void checkOptions() {
    verify(StringUtils.isNotBlank(options.getClientUrl()), "pulsar client url cannot be null");
    verify(StringUtils.isNotBlank(options.getAdminUrl()), "pulsar admin url cannot be null");
    verify(StringUtils.isNotBlank(options.getTopic()), "pulsar topic cannot be null");
  }

  @Override
  protected void loadSinkConfig(String json) {
    PulsarSinkConfig pulsarSinkConfig =
        JsonUtils.fromJson(json, new TypeReference<PulsarSinkConfig>() {})
            .orElseThrow(
                () ->
                    new DataWorkflowException(
                        String.format("jdbc sink config parse error. %s", json)));
    this.id =
        StringUtils.isNotBlank(pulsarSinkConfig.getId())
            ? pulsarSinkConfig.getId()
            : IdGenerator.uuid();
    this.name = pulsarSinkConfig.getName();
    this.input = pulsarSinkConfig.getInput();
    this.options = pulsarSinkConfig.getOptions();
  }

  @Getter
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class PulsarSinkOptions implements SinkConfig.SinkOptions {
    private String clientUrl;
    private String adminUrl;
    private String token;
    private String topic;
    private String producer;
    @Builder.Default private PulsarSinkFormat format = PulsarSinkFormat.JSON;
    private char delimiter;
    private String arrayElementDelimiter;
  }
}
