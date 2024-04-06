package io.github.shawn.octopus.fluxus.executor.bo.stepOptions.transform;

import static io.github.shawn.octopus.fluxus.engine.common.Constants.TransformConstants.XML_PARSE_TRANSFORM;

import io.github.shawn.octopus.fluxus.api.common.JsonUtils;
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

@Getter
@Builder
@JsonTypeName("transform_xmlParse")
@NoArgsConstructor
@AllArgsConstructor
public class XmlParseStepOptions implements StepOptions {
  @Builder.Default private final PluginType pluginType = PluginType.TRANSFORM;
  @Builder.Default private final String identify = XML_PARSE_TRANSFORM;

  private String valueField;
  private DataFormatField[] xmlParseFields;

  @Override
  public List<StepAttribute> getStepAttributes() {
    return Arrays.asList(
        StepAttribute.builder().code("valueField").value(getValueField()).build(),
        StepAttribute.builder()
            .code("jsonParseFields")
            .value(JsonUtils.toJsonString(xmlParseFields))
            .build());
  }
}
