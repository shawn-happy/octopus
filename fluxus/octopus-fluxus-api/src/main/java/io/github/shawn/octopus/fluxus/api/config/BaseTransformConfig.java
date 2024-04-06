package io.github.shawn.octopus.fluxus.api.config;

import static io.github.shawn.octopus.fluxus.api.common.PredicateUtils.verify;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.shawn.octopus.fluxus.api.common.JsonUtils;
import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

public abstract class BaseTransformConfig<P extends TransformConfig.TransformOptions>
    implements TransformConfig<P> {

  @Override
  public TransformConfig<P> toTransformConfig(String json) {
    JsonNode jsonNode =
        JsonUtils.toJsonNode(json)
            .orElseThrow(
                () ->
                    new DataWorkflowException(
                        String.format("transform config parse error. %s", json)));

    JsonNode typeNode = jsonNode.findPath("type");
    verify(JsonUtils.isNotNull(typeNode), "transform type cannot be null");
    verify(
        typeNode.textValue().equalsIgnoreCase(getIdentifier()),
        "transform type [%s] is not match identifier [%s]",
        typeNode.textValue(),
        getIdentifier());
    loadTransformConfig(json);
    checkConfig();
    checkOptions();
    return this;
  }

  protected abstract void checkOptions();

  private void checkConfig() {
    verify(StringUtils.isNotBlank(getName()), "transform name cannot be null");
    verify(StringUtils.isNotBlank(getOutput()), "transform output cannot be null");
    verify(CollectionUtils.isNotEmpty(getInputs()), "transform inputs cannot be null");
  }

  protected abstract void loadTransformConfig(String json);
}
