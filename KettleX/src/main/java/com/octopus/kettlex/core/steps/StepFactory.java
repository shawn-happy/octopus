package com.octopus.kettlex.core.steps;

import com.octopus.kettlex.core.exception.KettleXException;
import com.octopus.kettlex.model.StepConfig;
import com.octopus.kettlex.model.reader.RowGeneratorConfig;
import com.octopus.kettlex.model.transformation.ValueMapperConfig;
import com.octopus.kettlex.model.writer.LogMessageConfig;
import com.octopus.kettlex.runtime.TaskCombination;
import com.octopus.kettlex.runtime.reader.RowGenerator;
import com.octopus.kettlex.runtime.transformation.ValueMapper;
import com.octopus.kettlex.runtime.writer.LogMessage;

public class StepFactory {

  public static Step<?> createStep(StepConfig<?> stepConfig, TaskCombination taskCombination) {
    StepType stepType = stepConfig.getType();
    Step<?> step = null;
    switch (stepType) {
      case ROW_GENERATOR:
        step = new RowGenerator((RowGeneratorConfig) stepConfig, taskCombination);
        break;
      case VALUE_MAPPER:
        step = new ValueMapper((ValueMapperConfig) stepConfig, taskCombination);
        break;
      case LOG_MESSAGE:
        step = new LogMessage((LogMessageConfig) stepConfig, taskCombination);
        break;
      default:
        throw new KettleXException("unsupported step type: " + stepType);
    }
    return step;
  }
}
