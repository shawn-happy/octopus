package com.octopus.operators.kettlex.core.listener;

import com.octopus.operators.kettlex.core.context.StepContext;

public interface StepListener extends Comparable<StepListener> {

  int getOrder();

  void onPrepare(StepContext stepContext);

  void onRunnable(StepContext stepContext);

  void onSuccess(StepContext stepContext);

  void onError(StepContext stepContext, Throwable exception);
}
