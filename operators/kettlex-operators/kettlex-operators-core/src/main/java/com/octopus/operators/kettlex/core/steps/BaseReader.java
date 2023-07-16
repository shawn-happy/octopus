package com.octopus.operators.kettlex.core.steps;

import com.octopus.operators.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.operators.kettlex.core.row.Record;
import com.octopus.operators.kettlex.core.row.record.TerminateRecord;
import com.octopus.operators.kettlex.core.steps.config.ReaderConfig;
import java.util.List;
import java.util.function.Supplier;
import org.apache.commons.collections4.CollectionUtils;

public abstract class BaseReader<C extends ReaderConfig<?>> extends BaseStep<C>
    implements Reader<C> {

  protected BaseReader() {}

  @Override
  public void read() throws KettleXStepExecuteException {
    if (isShutdown()) {
      throw new KettleXStepExecuteException("step is shutdown");
    }
    try {
      List<Record> records = doReader().get();
      stepListeners.forEach(stepListener -> stepListener.onRunnable(stepContext));
      if (CollectionUtils.isEmpty(records)) {
        return;
      }
      for (Record record : records) {
        putRow(record);
      }
      putRow(TerminateRecord.get());
      stepListeners.forEach(stepListener -> stepListener.onSuccess(stepContext));
    } catch (Exception e) {
      setError(e);
    }
  }

  protected abstract Supplier<List<Record>> doReader() throws KettleXStepExecuteException;
}
