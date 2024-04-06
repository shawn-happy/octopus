package io.github.shawn.octopus.fluxus.engine.connector.transform.jsonParse;

import io.github.shawn.octopus.fluxus.api.config.TransformConfig;
import io.github.shawn.octopus.fluxus.api.provider.TransformProvider;
import io.github.shawn.octopus.fluxus.engine.common.Constants;

public class JsonParseTransformProvider implements TransformProvider<JsonParseTransformConfig> {

  @Override
  public String getIdentifier() {
    return Constants.TransformConstants.JSON_PARSE_TRANSFORM;
  }

  @Override
  public JsonParseTransformConfig getTransformConfig() {
    return new JsonParseTransformConfig();
  }

  @Override
  public JsonParseTransform getTransform(TransformConfig<?> transformConfig) {
    return new JsonParseTransform((JsonParseTransformConfig) transformConfig);
  }
}
