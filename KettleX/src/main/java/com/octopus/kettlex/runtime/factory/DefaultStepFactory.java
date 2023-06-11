package com.octopus.kettlex.runtime.factory;

import com.octopus.kettlex.core.exception.KettleXException;
import com.octopus.kettlex.core.steps.Step;
import com.octopus.kettlex.core.steps.StepFactory;
import com.octopus.kettlex.core.steps.StepType;
import com.octopus.kettlex.core.steps.reader.RowGenerator;
import com.octopus.kettlex.core.steps.transformation.ValueMapper;
import com.octopus.kettlex.core.steps.writer.LogMessage;
import com.octopus.kettlex.model.StepConfig;
import com.octopus.kettlex.runtime.StepConfigChannelCombination;

public class DefaultStepFactory implements StepFactory {

  private static final StepFactory stepFactory = new DefaultStepFactory();

  private DefaultStepFactory() {}

  public static StepFactory getStepFactory() {
    return stepFactory;
  }

  public Step<?> createStep(StepConfigChannelCombination combination) {
    StepConfig<?> stepConfig = combination.getStepConfig();
    StepType stepType = stepConfig.getType();
    Step<?> step = null;
    switch (stepType) {
      case ROW_GENERATOR:
        step = new RowGenerator(combination);
        break;
      case VALUE_MAPPER:
        step = new ValueMapper(combination);
        break;
      case LOG_MESSAGE:
        step = new LogMessage(combination);
        break;
      default:
        throw new KettleXException("unsupported step type: " + stepType);
    }
    return step;
  }
}
