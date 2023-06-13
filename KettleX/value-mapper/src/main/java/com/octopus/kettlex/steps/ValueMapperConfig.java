package com.octopus.kettlex.steps;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.octopus.kettlex.core.exception.KettleXException;
import com.octopus.kettlex.core.exception.KettleXStepConfigException;
import com.octopus.kettlex.core.row.column.FieldType;
import com.octopus.kettlex.core.steps.config.TransformerConfig;
import com.octopus.kettlex.core.utils.YamlUtil;
import com.octopus.kettlex.steps.ValueMapperConfig.ValueMapperOptions;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ValueMapperConfig implements TransformerConfig<ValueMapperOptions> {

  private static final String STEP_TYPE = "value-mapper";

  private String id;
  private String name;
  @Default private String type = STEP_TYPE;
  private String input;
  private String output;
  private ValueMapperOptions options;

  @Override
  public void loadYaml(JsonNode jsonNode) {
    if (jsonNode == null || jsonNode.isNull() || jsonNode.isEmpty()) {
      return;
    }
    ValueMapperConfig valueMapperConfig =
        YamlUtil.fromYaml(jsonNode.toString(), new TypeReference<ValueMapperConfig>() {})
            .orElse(null);
    if (valueMapperConfig != null) {
      String type = valueMapperConfig.getType();
      if (!STEP_TYPE.equals(type)) {
        throw new KettleXException(
            String.format("the step type [%s] is not [%s]", type, STEP_TYPE));
      }
      this.id = valueMapperConfig.id;
      this.name = valueMapperConfig.name;
      this.type = STEP_TYPE;
      this.input = valueMapperConfig.input;
      this.output = valueMapperConfig.output;
      this.options = valueMapperConfig.options;
    }
  }

  @Getter
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class ValueMapperOptions implements TransformerOptions {
    private String sourceField;
    private String targetField;
    private FieldType targetFieldType;
    private Map<Object, Object> fieldValueMap;

    @Override
    public void verify() {
      if (StringUtils.isBlank(sourceField) || StringUtils.isBlank(targetField)) {
        throw new KettleXStepConfigException("sourceField and targetField cannot be null");
      }
    }

    @Override
    public void loadYaml(JsonNode jsonNode) {
      if (jsonNode == null || jsonNode.isNull()) {
        return;
      }
      ValueMapperOptions valueMapperOptions =
          YamlUtil.fromYaml(jsonNode.asText(), new TypeReference<ValueMapperOptions>() {})
              .orElse(null);
      if (valueMapperOptions != null) {
        this.sourceField = valueMapperOptions.sourceField;
        this.targetField = valueMapperOptions.targetField;
        this.targetFieldType = valueMapperOptions.targetFieldType;
        this.fieldValueMap = valueMapperOptions.fieldValueMap;
      }
    }
  }
}
