package io.github.shawn.octopus.fluxus.engine.connector.transform.xmlParse;

import static io.github.shawn.octopus.fluxus.api.common.PredicateUtils.verify;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.shawn.octopus.fluxus.api.common.IdGenerator;
import io.github.shawn.octopus.fluxus.api.common.JsonUtils;
import io.github.shawn.octopus.fluxus.api.config.BaseTransformConfig;
import io.github.shawn.octopus.fluxus.api.config.TransformConfig;
import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import io.github.shawn.octopus.fluxus.engine.common.Constants;
import io.github.shawn.octopus.fluxus.engine.model.type.DataWorkflowFieldTypeParse;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.ArrayUtils;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class XmlParseTransformConfig
    extends BaseTransformConfig<XmlParseTransformConfig.XmlParseTransformOptions>
    implements TransformConfig<XmlParseTransformConfig.XmlParseTransformOptions> {

  @Builder.Default private String identifier = Constants.TransformConstants.XML_PARSE_TRANSFORM;

  private String id;
  private String name;
  private List<String> inputs;
  private String output;
  private XmlParseTransformConfig.XmlParseTransformOptions options;

  @Override
  protected void checkOptions() {
    verify(isNotBlank(options.getValueField()), "valueField in xml parser options cannot be null");
    verify(
        ArrayUtils.isNotEmpty(options.getXmlParseFields()),
        "xmlParseFields in xml parser options cannot be null");
  }

  @Override
  protected void loadTransformConfig(String json) {
    XmlParseTransformConfig xmlParseTransformConfig =
        JsonUtils.fromJson(json, new TypeReference<XmlParseTransformConfig>() {})
            .orElseThrow(
                () -> new DataWorkflowException("xml parse transform config deserialize error"));
    this.id =
        isNotBlank(xmlParseTransformConfig.getId())
            ? xmlParseTransformConfig.getId()
            : IdGenerator.uuid();
    this.name = xmlParseTransformConfig.getName();
    this.inputs = xmlParseTransformConfig.getInputs();
    this.output = xmlParseTransformConfig.getOutput();
    this.options = xmlParseTransformConfig.getOptions();
  }

  @Builder
  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class XmlParseTransformOptions implements TransformConfig.TransformOptions {

    private String valueField;
    private XmlParseField[] xmlParseFields;

    public String[] getDestinations() {
      String[] destinations = new String[xmlParseFields.length];
      for (int i = 0; i < xmlParseFields.length; i++) {
        destinations[i] = xmlParseFields[i].getDestination();
      }
      return destinations;
    }

    public DataWorkflowFieldType[] getDestinationTypes() {
      DataWorkflowFieldType[] destinationTypes = new DataWorkflowFieldType[xmlParseFields.length];
      for (int i = 0; i < xmlParseFields.length; i++) {
        destinationTypes[i] = DataWorkflowFieldTypeParse.parseDataType(xmlParseFields[i].getType());
      }
      return destinationTypes;
    }
  }

  @Getter
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class XmlParseField {
    private String destination;
    private String sourcePath;
    private String type;
  }
}
