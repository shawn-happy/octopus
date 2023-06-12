package com.octopus.kettlex.reader.rowgenerator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.octopus.kettlex.core.exception.KettleXException;
import com.octopus.kettlex.core.exception.KettleXStepConfigException;
import com.octopus.kettlex.core.steps.config.ReaderConfig;
import com.octopus.kettlex.core.utils.YamlUtil;
import com.octopus.kettlex.reader.rowgenerator.RowGeneratorConfig.RowGeneratorOptions;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.ArrayUtils;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RowGeneratorConfig implements ReaderConfig<RowGeneratorOptions> {

  private static final String STEP_TYPE = "row-generator";

  private String id;
  private String name;
  @Default private String type = STEP_TYPE;
  private RowGeneratorOptions options;
  private String output;

  @Override
  public void loadYaml(JsonNode jsonNode) {
    if (jsonNode == null || jsonNode.isNull()) {
      return;
    }
    RowGeneratorConfig rowGeneratorConfig =
        YamlUtil.fromYaml(jsonNode.asText(), new TypeReference<RowGeneratorConfig>() {})
            .orElse(null);
    if (rowGeneratorConfig != null) {
      String type = rowGeneratorConfig.type;
      if (!STEP_TYPE.equals(type)) {
        throw new KettleXException(
            String.format("the step type [%s] is not [%s]", type, STEP_TYPE));
      }
      this.id = rowGeneratorConfig.id;
      this.name = rowGeneratorConfig.name;
      this.type = STEP_TYPE;
      this.options = rowGeneratorConfig.options;
      this.output = rowGeneratorConfig.output;
    }
  }

  @Getter
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class RowGeneratorOptions implements ReaderOptions {
    private Integer rowLimit;
    private Field[] fields;

    @Override
    public void verify() {
      if (rowLimit != null && rowLimit < 0) {
        throw new KettleXStepConfigException("row limit cannot be less than 0");
      }
      if (ArrayUtils.isEmpty(fields)) {
        throw new KettleXStepConfigException("fields cannot be empty");
      }
    }

    @Override
    public void loadYaml(JsonNode jsonNode) {
      if (jsonNode == null || jsonNode.isNull()) {
        return;
      }
      RowGeneratorOptions options =
          YamlUtil.fromYaml(jsonNode.asText(), new TypeReference<RowGeneratorOptions>() {})
              .orElse(null);
      if (options != null) {
        this.rowLimit = Optional.ofNullable(options.getRowLimit()).orElse(1);
        this.fields = options.fields;
      }
    }
  }
}
