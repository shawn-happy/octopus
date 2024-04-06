package io.github.shawn.octopus.fluxus.engine.connector.transform.jsonParse;

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
public class JsonParseTransformConfig
    extends BaseTransformConfig<JsonParseTransformConfig.JsonParseTransformOptions>
    implements TransformConfig<JsonParseTransformConfig.JsonParseTransformOptions> {

  @Builder.Default private String identifier = Constants.TransformConstants.JSON_PARSE_TRANSFORM;
  private String id;
  private String name;
  private List<String> inputs;
  private String output;
  private JsonParseTransformOptions options;

  @Override
  protected void checkOptions() {
    verify(isNotBlank(options.getValueField()), "valueField in json parser options cannot be null");
    verify(
        ArrayUtils.isNotEmpty(options.getJsonParseFields()),
        "jsonParseFields in json parser options cannot be null");
  }

  @Override
  protected void loadTransformConfig(String json) {
    JsonParseTransformConfig jsonParseTransformConfig =
        JsonUtils.fromJson(json, new TypeReference<JsonParseTransformConfig>() {})
            .orElseThrow(
                () -> new DataWorkflowException("json parse transform config deserialize error"));
    this.id =
        isNotBlank(jsonParseTransformConfig.getId())
            ? jsonParseTransformConfig.getId()
            : IdGenerator.uuid();
    this.name = jsonParseTransformConfig.getName();
    this.inputs = jsonParseTransformConfig.getInputs();
    this.output = jsonParseTransformConfig.getOutput();
    this.options = jsonParseTransformConfig.getOptions();
  }

  @Builder
  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class JsonParseTransformOptions implements TransformConfig.TransformOptions {

    private String valueField;
    private JsonParseField[] jsonParseFields;

    public String[] getDestinations() {
      String[] destinations = new String[jsonParseFields.length];
      for (int i = 0; i < jsonParseFields.length; i++) {
        destinations[i] = jsonParseFields[i].getDestination();
      }
      return destinations;
    }

    public DataWorkflowFieldType[] getDestinationTypes() {
      DataWorkflowFieldType[] destinationTypes = new DataWorkflowFieldType[jsonParseFields.length];
      for (int i = 0; i < jsonParseFields.length; i++) {
        destinationTypes[i] =
            DataWorkflowFieldTypeParse.parseDataType(jsonParseFields[i].getType());
      }
      return destinationTypes;
    }
  }

  @Getter
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class JsonParseField {
    private String destination;
    private String sourcePath;
    private String type;
  }
}
