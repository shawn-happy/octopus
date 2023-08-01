package com.octopus.operators.kettlex.core.steps;

import com.octopus.operators.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.operators.kettlex.core.row.record.TerminateRecord;
import com.octopus.operators.kettlex.core.steps.config.ReaderConfig;

public abstract class BaseReader<C extends ReaderConfig<?>> extends BaseStep<C>
    implements Reader<C> {

  protected BaseReader() {}

  @Override
  public void read() throws KettleXStepExecuteException {
    if (isShutdown()) {
      throw new KettleXStepExecuteException("step is shutdown");
    }
    try {
      stepListeners.forEach(stepListener -> stepListener.onRunnable(stepContext));
      doReader();
      putRow(TerminateRecord.get());
      stepListeners.forEach(stepListener -> stepListener.onSuccess(stepContext));
    } catch (Exception e) {
      setError(e);
      throw new KettleXStepExecuteException(
          String.format("%s read error", stepContext.getStepName()), e);
    }
  }

  protected abstract void doReader() throws KettleXStepExecuteException;
}
