package com.octopus.operators.kettlex.core.listener;

import com.octopus.operators.kettlex.core.context.StepContext;
import com.octopus.operators.kettlex.core.management.Communication;
import com.octopus.operators.kettlex.core.management.ExecutionStatus;

public class DefaultStepListener implements StepListener {

  @Override
  public int getOrder() {
    return 0;
  }

  @Override
  public void onPrepare(StepContext stepContext) {
    stepContext.getCommunication().markStatus(ExecutionStatus.WAITING);
  }

  @Override
  public void onRunnable(StepContext stepContext) {
    stepContext.getCommunication().markStatus(ExecutionStatus.RUNNING);
  }

  @Override
  public void onSuccess(StepContext stepContext) {
    stepContext.getCommunication().markStatus(ExecutionStatus.SUCCEEDED);
  }

  @Override
  public void onError(StepContext stepContext, Throwable exception) {
    Communication communication = stepContext.getCommunication();
    communication.markStatus(ExecutionStatus.FAILED);
    communication.setException(exception);
  }

  @Override
  public int compareTo(StepListener o) {
    if (o == null) {
      return -1;
    }
    return getOrder() - o.getOrder();
  }
}
