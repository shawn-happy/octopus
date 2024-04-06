package io.github.shawn.octopus.fluxus.api.config;

import static io.github.shawn.octopus.fluxus.api.common.PredicateUtils.verify;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.shawn.octopus.fluxus.api.common.JsonUtils;
import io.github.shawn.octopus.fluxus.api.common.PredicateUtils;
import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import org.apache.commons.lang3.StringUtils;

public abstract class BaseSinkConfig<P extends SinkConfig.SinkOptions> implements SinkConfig<P> {
  @Override
  public SinkConfig<P> toSinkConfig(String json) {
    JsonNode jsonNode =
        JsonUtils.toJsonNode(json)
            .orElseThrow(
                () ->
                    new DataWorkflowException(String.format("sink config parse error. %s", json)));

    JsonNode typeNode = jsonNode.findPath("type");
    verify(JsonUtils.isNotNull(typeNode), "sink type cannot be null");
    verify(
        typeNode.textValue().equalsIgnoreCase(getIdentifier()),
        "transform type [%s] is not match identifier [%s]",
        typeNode.textValue(),
        getIdentifier());
    loadSinkConfig(json);
    checkConfig();
    checkOptions();
    return this;
  }

  protected abstract void checkOptions();

  private void checkConfig() {
    PredicateUtils.verify(StringUtils.isNotBlank(getName()), "sink name cannot be null");
    PredicateUtils.verify(StringUtils.isNotBlank(getInput()), "sink input cannot be null");
  }

  protected abstract void loadSinkConfig(String json);
}
