package io.github.shawn.octopus.fluxus.engine.connector.transform.xmlParse;

import io.github.shawn.octopus.fluxus.api.config.TransformConfig;
import io.github.shawn.octopus.fluxus.api.provider.TransformProvider;
import io.github.shawn.octopus.fluxus.engine.common.Constants;

public class XmlParseTransformProvider implements TransformProvider<XmlParseTransformConfig> {

  @Override
  public String getIdentifier() {
    return Constants.TransformConstants.XML_PARSE_TRANSFORM;
  }

  @Override
  public XmlParseTransformConfig getTransformConfig() {
    return new XmlParseTransformConfig();
  }

  @Override
  public XmlParseTransform getTransform(TransformConfig<?> transformConfig) {
    return new XmlParseTransform((XmlParseTransformConfig) transformConfig);
  }
}
