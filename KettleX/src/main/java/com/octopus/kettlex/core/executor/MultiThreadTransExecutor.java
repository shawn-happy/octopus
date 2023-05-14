package com.octopus.kettlex.core.executor;

import com.octopus.kettlex.core.statistics.ExecutionState;
import com.octopus.kettlex.core.step.StepRunThread;
import com.octopus.kettlex.core.steps.Step;
import com.octopus.kettlex.core.trans.TransMeta;

public class MultiThreadTransExecutor extends AbstractTransExecutor {

  public MultiThreadTransExecutor(TransMeta transMeta) {
    super(transMeta);
  }

  @Override
  protected void startExecute() {
    markStatus(ExecutionState.RUNNING);
    for (Step step : steps) {
      StepRunThread runThread = new StepRunThread(step);
      new Thread(runThread).start();
      step.markStatus(ExecutionState.RUNNING);
    }
  }
}
