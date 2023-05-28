package com.octopus.kettlex.core.steps.common;

import com.octopus.kettlex.core.exception.KettleXException;
import com.octopus.kettlex.core.steps.Step;
import com.octopus.kettlex.core.steps.StepConfig;
import com.octopus.kettlex.core.steps.StepContext;
import com.octopus.kettlex.core.steps.StepType;
import com.octopus.kettlex.core.steps.reader.rowgenerator.RowGenerator;
import com.octopus.kettlex.core.steps.reader.rowgenerator.RowGeneratorConfig;
import com.octopus.kettlex.core.steps.reader.rowgenerator.RowGeneratorContext;
import com.octopus.kettlex.core.steps.transform.valueMapper.ValueMapper;
import com.octopus.kettlex.core.steps.transform.valueMapper.ValueMapperConfig;
import com.octopus.kettlex.core.steps.transform.valueMapper.ValueMapperContext;
import com.octopus.kettlex.core.steps.writer.log.LogMessage;
import com.octopus.kettlex.core.steps.writer.log.LogMessageConfig;
import com.octopus.kettlex.core.steps.writer.log.LogMessageContext;

public class StepFactory {

  public static Step<?, ? extends StepContext> createStep(StepConfig stepConfig) {
    StepType stepType = stepConfig.getStepType();
    Step<?, ?> step = null;
    switch (stepType) {
      case ROW_GENERATOR:
        step = new RowGenerator((RowGeneratorConfig) stepConfig, new RowGeneratorContext());
        break;
      case VALUE_MAPPER:
        step = new ValueMapper((ValueMapperConfig) stepConfig, new ValueMapperContext());
        break;
      case LOG_MESSAGE:
        step = new LogMessage((LogMessageConfig) stepConfig, new LogMessageContext());
        break;
      default:
        throw new KettleXException("unsupported step type: " + stepType);
    }
    return step;
  }
}
