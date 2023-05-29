package com.octopus.kettlex.core.steps.common;

import com.octopus.kettlex.core.exception.KettleXException;
import com.octopus.kettlex.core.steps.Step;
import com.octopus.kettlex.core.steps.StepMeta;
import com.octopus.kettlex.core.steps.StepType;
import com.octopus.kettlex.core.steps.reader.rowgenerator.RowGenerator;
import com.octopus.kettlex.core.steps.reader.rowgenerator.RowGeneratorMeta;
import com.octopus.kettlex.core.steps.transform.valueMapper.ValueMapper;
import com.octopus.kettlex.core.steps.transform.valueMapper.ValueMapperMeta;
import com.octopus.kettlex.core.steps.writer.log.LogMessage;
import com.octopus.kettlex.core.steps.writer.log.LogMessageMeta;

public class StepFactory {

  public static Step<?> createStep(StepMeta stepMeta) {
    StepType stepType = stepMeta.getStepType();
    Step<?> step = null;
    switch (stepType) {
      case ROW_GENERATOR:
        step = new RowGenerator((RowGeneratorMeta) stepMeta);
        break;
      case VALUE_MAPPER:
        step = new ValueMapper((ValueMapperMeta) stepMeta);
        break;
      case LOG_MESSAGE:
        step = new LogMessage((LogMessageMeta) stepMeta);
        break;
      default:
        throw new KettleXException("unsupported step type: " + stepType);
    }
    return step;
  }
}
