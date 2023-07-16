package com.octopus.operators.kettlex.core.steps;

import com.octopus.operators.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.operators.kettlex.core.row.Record;
import com.octopus.operators.kettlex.core.row.record.TerminateRecord;
import com.octopus.operators.kettlex.core.steps.config.WriterConfig;
import java.util.function.Consumer;

public abstract class BaseWriter<C extends WriterConfig<?>> extends BaseStep<C>
    implements Writer<C> {

  protected BaseWriter() {}

  @Override
  public void writer() throws KettleXStepExecuteException {
    if (isShutdown()) {
      throw new KettleXStepExecuteException("step is shutdown");
    }
    Record record;
    try {
      stepListeners.forEach(stepListener -> stepListener.onRunnable(stepContext));
      while ((record = getRow()) != null) {
        if (record instanceof TerminateRecord) {
          stepListeners.forEach(stepListener -> stepListener.onSuccess(stepContext));
          break;
        }
        doWriter().accept(record);
      }
    } catch (Exception e) {
      setError(e);
    }
  }

  protected abstract Consumer<Record> doWriter() throws KettleXStepExecuteException;
}
