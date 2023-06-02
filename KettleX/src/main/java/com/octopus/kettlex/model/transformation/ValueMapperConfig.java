package com.octopus.kettlex.model.transformation;

import com.octopus.kettlex.core.exception.KettleXStepConfigException;
import com.octopus.kettlex.core.row.column.FieldType;
import com.octopus.kettlex.core.steps.StepType;
import com.octopus.kettlex.model.TransformationConfig;
import com.octopus.kettlex.model.TransformationOptions;
import com.octopus.kettlex.model.transformation.ValueMapperConfig.ValueMapperOptions;
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
public class ValueMapperConfig implements TransformationConfig<ValueMapperOptions> {

  private String name;
  @Default private final StepType type = StepType.VALUE_MAPPER;
  private String input;
  private String output;
  private ValueMapperOptions options;

  @Getter
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class ValueMapperOptions implements TransformationOptions {
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
  }
}
