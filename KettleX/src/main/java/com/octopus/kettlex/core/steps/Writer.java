package com.octopus.kettlex.core.steps;

import com.octopus.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.kettlex.core.row.RecordExchanger;

public interface Writer<CONFIG extends StepConfig, CONTEXT extends StepContext>
    extends Step<CONFIG, CONTEXT> {

  void writer(RecordExchanger recordExchanger) throws KettleXStepExecuteException;
}
