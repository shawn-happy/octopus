package io.github.shawn.octopus.fluxus.api.config;

import static io.github.shawn.octopus.fluxus.api.common.PredicateUtils.verify;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.shawn.octopus.fluxus.api.common.JsonUtils;
import io.github.shawn.octopus.fluxus.api.common.PredicateUtils;
import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

public abstract class BaseSourceConfig<P extends SourceConfig.SourceOptions>
    implements SourceConfig<P> {

  @Override
  public BaseSourceConfig<P> toSourceConfig(String json) {
    JsonNode jsonNode =
        JsonUtils.toJsonNode(json)
            .orElseThrow(
                () ->
                    new DataWorkflowException(
                        String.format("source config parse error. %s", json)));

    JsonNode typeNode = jsonNode.findPath("type");
    PredicateUtils.verify(JsonUtils.isNotNull(typeNode), "source type cannot be null");
    verify(
        typeNode.textValue().equalsIgnoreCase(getIdentifier()),
        "source type [%s] is not match identifier [%s]",
        typeNode.textValue(),
        getIdentifier());
    loadSourceConfig(json);
    checkConfig();
    checkOptions();
    return this;
  }

  protected abstract void checkOptions();

  protected abstract void loadSourceConfig(String json);

  private void checkConfig() {
    verify(StringUtils.isNotBlank(getName()), "source name cannot be null");
    verify(StringUtils.isNotBlank(getOutput()), "source output cannot be null");
    verify(CollectionUtils.isNotEmpty(getColumns()), "source columns cannot be null");
  }
}
