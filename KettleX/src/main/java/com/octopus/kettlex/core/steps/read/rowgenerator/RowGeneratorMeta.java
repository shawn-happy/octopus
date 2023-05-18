package com.octopus.kettlex.core.steps.read.rowgenerator;

import com.octopus.kettlex.core.exception.KettleXException;
import com.octopus.kettlex.core.steps.BaseStepMeta;
import com.octopus.kettlex.core.steps.StepType;
import com.octopus.kettlex.core.steps.common.Field;
import com.octopus.kettlex.core.utils.JsonUtil;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RowGeneratorMeta extends BaseStepMeta {

  @Default private StepType stepType = StepType.ROW_GENERATOR_INPUT;
  private Field[] fields;

  @Override
  public String getJSON() throws KettleXException {
    return JsonUtil.toJson(this).orElse(null);
  }
}
