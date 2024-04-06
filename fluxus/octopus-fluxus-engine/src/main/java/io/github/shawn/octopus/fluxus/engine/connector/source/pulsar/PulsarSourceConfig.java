package io.github.shawn.octopus.fluxus.engine.connector.source.pulsar;

import static io.github.shawn.octopus.fluxus.api.common.PredicateUtils.verify;
import static io.github.shawn.octopus.fluxus.engine.common.Constants.SourceConstants.PULSAR_SOURCE;

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

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PulsarSourceConfig extends BaseSourceConfig<PulsarSourceConfig.PulsarSourceOptions>
    implements SourceConfig<PulsarSourceConfig.PulsarSourceOptions> {

  private String id;
  private String name;

  @JsonProperty("type")
  @Builder.Default
  private transient String identifier = PULSAR_SOURCE;

  private String output;
  private PulsarSourceOptions options;
  private List<Column> columns;

  @Override
  protected void checkOptions() {
    verify(StringUtils.isNotBlank(options.getClientUrl()), "pulsar client url cannot be null");
    verify(StringUtils.isNotBlank(options.getAdminUrl()), "pulsar admin url cannot be null");
    verify(StringUtils.isNotBlank(options.getTopic()), "pulsar topic cannot be null");
  }

  @Override
  protected void loadSourceConfig(String json) {
    PulsarSourceConfig pulsarSourceConfig =
        JsonUtils.fromJson(json, new TypeReference<PulsarSourceConfig>() {})
            .orElseThrow(
                () ->
                    new DataWorkflowException(
                        String.format("pulsar config parse error. %s", json)));
    this.id =
        StringUtils.isNotBlank(pulsarSourceConfig.getId())
            ? pulsarSourceConfig.getId()
            : IdGenerator.uuid();
    this.name = pulsarSourceConfig.getName();
    this.output = pulsarSourceConfig.getOutput();
    this.options = pulsarSourceConfig.getOptions();
    this.columns = pulsarSourceConfig.getColumns();
  }

  @Getter
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class PulsarSourceOptions implements SourceConfig.SourceOptions {
    private String clientUrl;
    private String adminUrl;
    private String token;
    private String topic;
    private String subscription;
    @Builder.Default private String subscriptionType = "Earliest";
    @Builder.Default private int commitSize = 1000;
  }
}
