package com.octopus.kettlex.core.steps;

import com.octopus.kettlex.core.exception.KettleXException;
import com.octopus.kettlex.core.steps.StepType.PrimaryCategory;
import com.octopus.kettlex.model.StepConfig;
import com.octopus.kettlex.model.reader.RowGeneratorConfig;
import com.octopus.kettlex.model.transformation.ValueMapperConfig;
import com.octopus.kettlex.model.writer.LogMessageConfig;
import com.octopus.kettlex.runtime.reader.RowGenerator;
import com.octopus.kettlex.runtime.transformation.ValueMapper;
import com.octopus.kettlex.runtime.writer.LogMessage;

public class StepFactory {

  public static Step<?> createStep(StepConfigChannelCombination combination) {
    StepConfig<?> stepConfig = combination.getStepConfig();
    StepType stepType = stepConfig.getType();
    Step<?> step = null;
    switch (stepType) {
      case ROW_GENERATOR:
        step = new RowGenerator((RowGeneratorConfig) stepConfig);
        break;
      case VALUE_MAPPER:
        step = new ValueMapper((ValueMapperConfig) stepConfig);
        break;
      case LOG_MESSAGE:
        step = new LogMessage((LogMessageConfig) stepConfig);
        break;
      default:
        throw new KettleXException("unsupported step type: " + stepType);
    }
    PrimaryCategory primaryCategory = stepType.getPrimaryCategory();
    BaseStep<?> baseStep = (BaseStep<?>) step;
    switch (primaryCategory) {
      case SOURCE:
        baseStep.setOutputChannels(combination.getOutputChannels());
        break;
      case TRANSFORMATION:
        baseStep.setInputChannel(combination.getInputChannel());
        baseStep.setOutputChannels(combination.getOutputChannels());
        break;
      case SINK:
        baseStep.setInputChannel(combination.getInputChannel());
        break;
    }
    return baseStep;
  }
}
