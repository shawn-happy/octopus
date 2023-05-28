package com.octopus.kettlex.core.steps.transform.valueMapper;

import com.octopus.kettlex.core.row.column.FieldType;
import com.octopus.kettlex.core.steps.StepConfig;
import com.octopus.kettlex.core.steps.StepType;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ValueMapperConfig implements StepConfig {

  private String id;
  private String name;
  @Default private final StepType stepType = StepType.VALUE_MAPPER;

  private String sourceField;
  private String targetField;
  private FieldType targetFieldType;
  private Map<Object, Object> fieldValueMap;
}
