package io.github.shawn.octopus.fluxus.engine.connector.sink.console;

import static io.github.shawn.octopus.fluxus.engine.common.Constants.SinkConstants.CONSOLE_SINK;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.shawn.octopus.fluxus.api.common.IdGenerator;
import io.github.shawn.octopus.fluxus.api.common.JsonUtils;
import io.github.shawn.octopus.fluxus.api.config.BaseSinkConfig;
import io.github.shawn.octopus.fluxus.api.config.SinkConfig;
import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConsoleSinkConfig extends BaseSinkConfig<ConsoleSinkConfig.ConsoleSinkOptions>
    implements SinkConfig<ConsoleSinkConfig.ConsoleSinkOptions> {

  private String id;
  private String name;
  @Builder.Default private String identifier = CONSOLE_SINK;
  private String input;
  private ConsoleSinkOptions options;

  @Override
  protected void checkOptions() {}

  @Override
  protected void loadSinkConfig(String json) {
    ConsoleSinkConfig consoleSinkConfig =
        JsonUtils.fromJson(json, new TypeReference<ConsoleSinkConfig>() {})
            .orElseThrow(
                () ->
                    new DataWorkflowException(
                        String.format("console sink config parse error. %s", json)));
    this.id =
        StringUtils.isNotBlank(consoleSinkConfig.getId())
            ? consoleSinkConfig.getId()
            : IdGenerator.uuid();
    this.name = consoleSinkConfig.getName();
    this.input = consoleSinkConfig.getInput();
    this.options = consoleSinkConfig.getOptions();
  }

  @Getter
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class ConsoleSinkOptions implements SinkConfig.SinkOptions {

    @Builder.Default private long line = 1000L;
  }
}
