package com.octopus.kettlex.core.steps.reader.rowgenerator;

import com.octopus.kettlex.core.steps.StepMeta;
import com.octopus.kettlex.core.steps.StepType;
import com.octopus.kettlex.core.steps.common.Field;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class RowGeneratorMeta implements StepMeta {

  private String id;
  private String name;
  private final StepType stepType = StepType.ROW_GENERATOR;
  private Field[] fields;
}
